// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::{self, Display, Formatter};
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use engine_traits::{CfName, IterOptions, Iterable, Iterator, KvEngine, CF_WRITE, LARGE_CFS};
use kvproto::metapb::Region;
use kvproto::metapb::RegionEpoch;
use kvproto::pdpb::CheckPolicy;

use crate::coprocessor::Config;
use crate::coprocessor::CoprocessorHost;
use crate::coprocessor::SplitCheckerHost;
use crate::coprocessor::{get_region_approximate_keys, get_region_approximate_size};
use crate::store::{Callback, CasualMessage, CasualRouter};
use crate::Result;
use configuration::{ConfigChange, Configuration};
use file_system::{IOType, WithIOType};
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::worker::Runnable;

use super::metrics::*;

#[derive(PartialEq, Eq)]
pub struct KeyEntry {
    key: Vec<u8>,
    pos: usize,
    value_size: usize,
    cf: CfName,
}

impl KeyEntry {
    pub fn new(key: Vec<u8>, pos: usize, value_size: usize, cf: CfName) -> KeyEntry {
        KeyEntry {
            key,
            pos,
            value_size,
            cf,
        }
    }

    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_commit_version(&self) -> bool {
        self.cf == CF_WRITE
    }

    pub fn entry_size(&self) -> usize {
        self.value_size + self.key.len()
    }
}

impl PartialOrd for KeyEntry {
    fn partial_cmp(&self, rhs: &KeyEntry) -> Option<Ordering> {
        // BinaryHeap is max heap, so we have to reverse order to get a min heap.
        Some(self.key.cmp(&rhs.key).reverse())
    }
}

impl Ord for KeyEntry {
    fn cmp(&self, rhs: &KeyEntry) -> Ordering {
        self.partial_cmp(rhs).unwrap()
    }
}

struct MergedIterator<I> {
    iters: Vec<(CfName, I)>,
    heap: BinaryHeap<KeyEntry>,
}

impl<I> MergedIterator<I>
where
    I: Iterator,
{
    fn new<EK: KvEngine>(
        db: &EK,
        cfs: &[CfName],
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<EK::Iterator>> {
        let mut iters = Vec::with_capacity(cfs.len());
        let mut heap = BinaryHeap::with_capacity(cfs.len());
        for (pos, cf) in cfs.iter().enumerate() {
            let iter_opt = IterOptions::new(
                Some(KeyBuilder::from_slice(start_key, 0, 0)),
                Some(KeyBuilder::from_slice(end_key, 0, 0)),
                fill_cache,
            );
            let mut iter = db.iterator_cf_opt(cf, iter_opt)?;
            let found: Result<bool> = iter.seek(start_key.into()).map_err(|e| box_err!(e));
            if found? {
                heap.push(KeyEntry::new(
                    iter.key().to_vec(),
                    pos,
                    iter.value().len(),
                    *cf,
                ));
            }
            iters.push((*cf, iter));
        }
        Ok(MergedIterator { iters, heap })
    }

    fn next(&mut self) -> Option<KeyEntry> {
        let pos = match self.heap.peek() {
            None => return None,
            Some(e) => e.pos,
        };
        let (cf, iter) = &mut self.iters[pos];
        if iter.next().unwrap() {
            // TODO: avoid copy key.
            let mut e = KeyEntry::new(iter.key().to_vec(), pos, iter.value().len(), cf);
            let mut front = self.heap.peek_mut().unwrap();
            mem::swap(&mut e, &mut front);
            Some(e)
        } else {
            self.heap.pop()
        }
    }
}

pub enum Task<EK> {
    SplitCheckTask {
        tablet: EK,
        region: Region,
        auto_split: bool,
        policy: CheckPolicy,
    },
    ChangeConfig(ConfigChange),
    #[cfg(any(test, feature = "testexport"))]
    Validate(Box<dyn FnOnce(&Config) + Send>),
    GetRegionApproximateSizeAndKeys {
        tablet: EK,
        region: Region,
        pending_tasks: Arc<AtomicU64>,
        cb: Box<dyn FnOnce(u64, u64) + Send>,
    },
}

impl<EK> Task<EK> {
    pub fn split_check(
        tablet: EK,
        region: Region,
        auto_split: bool,
        policy: CheckPolicy,
    ) -> Task<EK> {
        Task::SplitCheckTask {
            tablet,
            region,
            auto_split,
            policy,
        }
    }
}

impl<EK> Display for Task<EK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::SplitCheckTask {
                region, auto_split, ..
            } => write!(
                f,
                "[split check worker] Split Check Task for {}, auto_split: {:?}",
                region.get_id(),
                auto_split
            ),
            Task::ChangeConfig(_) => write!(f, "[split check worker] Change Config Task"),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(_) => write!(f, "[split check worker] Validate config"),
            Task::GetRegionApproximateSizeAndKeys { region, .. } => write!(
                f,
                "[split check worker] Get region approximate size and keys for region {}",
                region.get_id()
            ),
        }
    }
}

pub struct Runner<EK, S>
where
    EK: KvEngine,
{
    router: S,
    coprocessor: CoprocessorHost<EK>,
    cfg: Config,
}

impl<EK, S> Runner<EK, S>
where
    EK: KvEngine,
    S: CasualRouter<EK>,
{
    pub fn new(
        _engine: EK,
        router: S,
        coprocessor: CoprocessorHost<EK>,
        cfg: Config,
    ) -> Runner<EK, S> {
        Runner {
            router,
            coprocessor,
            cfg,
        }
    }

    /// Checks a Region with split checkers to produce split keys and generates split admin command.
    fn check_split(&mut self, tablet: EK, region: &Region, auto_split: bool, policy: CheckPolicy) {
        let region_id = region.get_id();
        let start_key = keys::enc_start_key(region);
        let end_key = keys::enc_end_key(region);
        debug!(
            "executing task";
            "region_id" => region_id,
            "start_key" => log_wrappers::Value::key(&start_key),
            "end_key" => log_wrappers::Value::key(&end_key),
        );
        CHECK_SPILT_COUNTER.all.inc();

        let mut host = self
            .coprocessor
            .new_split_checker_host(&self.cfg, region, &tablet, auto_split, policy);
        if host.skip() {
            debug!("skip split check"; "region_id" => region.get_id());
            return;
        }

        let split_keys = match host.policy() {
            CheckPolicy::Scan => {
                match self.scan_split_keys(&mut host, &tablet, region, &start_key, &end_key) {
                    Ok(keys) => keys,
                    Err(e) => {
                        error!(%e; "failed to scan split key"; "region_id" => region_id,);
                        return;
                    }
                }
            }
            CheckPolicy::Approximate => match host.approximate_split_keys(region, &tablet) {
                Ok(keys) => keys
                    .into_iter()
                    .map(|k| keys::origin_key(&k).to_vec())
                    .collect(),
                Err(e) => {
                    error!(%e;
                        "failed to get approximate split key, try scan way";
                        "region_id" => region_id,
                    );
                    match self.scan_split_keys(&mut host, &tablet, region, &start_key, &end_key) {
                        Ok(keys) => keys,
                        Err(e) => {
                            error!(%e; "failed to scan split key"; "region_id" => region_id,);
                            return;
                        }
                    }
                }
            },
            CheckPolicy::Usekey => vec![], // Handled by pd worker directly.
        };

        if !split_keys.is_empty() {
            let region_epoch = region.get_region_epoch().clone();
            let msg = new_split_region(region_epoch, split_keys, "split checker");
            let res = self.router.send(region_id, msg);
            if let Err(e) = res {
                warn!("failed to send check result"; "region_id" => region_id, "err" => %e);
            }

            CHECK_SPILT_COUNTER.success.inc();
        } else {
            debug!(
                "no need to send, split key not found";
                "region_id" => region_id,
            );

            CHECK_SPILT_COUNTER.ignore.inc();
        }
    }

    /// Gets the split keys by scanning the range.
    fn scan_split_keys(
        &self,
        host: &mut SplitCheckerHost<'_, EK>,
        tablet: &EK,
        region: &Region,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        MergedIterator::<<EK as Iterable>::Iterator>::new(
            tablet, LARGE_CFS, start_key, end_key, false,
        )
        .map(|mut iter| {
            let mut size = 0;
            let mut keys = 0;
            while let Some(e) = iter.next() {
                if host.on_kv(region, &e) {
                    return;
                }
                size += e.entry_size() as u64;
                keys += 1;
            }

            // if we scan the whole range, we can update approximate size and keys with accurate value.
            info!(
                "update approximate size and keys with accurate value";
                "region_id" => region.get_id(),
                "size" => size,
                "keys" => keys,
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateSize { size },
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateKeys { keys },
            );
        })?;
        timer.observe_duration();

        Ok(host.split_keys())
    }

    fn change_cfg(&mut self, change: ConfigChange) {
        info!(
            "split check config updated";
            "change" => ?change
        );
        self.cfg.update(change);
    }
}

impl<EK, S> Runnable for Runner<EK, S>
where
    EK: KvEngine,
    S: CasualRouter<EK>,
{
    type Task = Task<EK>;
    fn run(&mut self, task: Task<EK>) {
        let _io_type_guard = WithIOType::new(IOType::LoadBalance);
        match task {
            Task::SplitCheckTask {
                tablet,
                region,
                auto_split,
                policy,
            } => self.check_split(tablet, &region, auto_split, policy),
            Task::ChangeConfig(c) => self.change_cfg(c),
            #[cfg(any(test, feature = "testexport"))]
            Task::Validate(f) => f(&self.cfg),
            Task::GetRegionApproximateSizeAndKeys {
                tablet,
                region,
                pending_tasks,
                cb,
            } => {
                if pending_tasks.fetch_sub(1, AtomicOrdering::SeqCst) > 1 {
                    return;
                }
                let size = get_region_approximate_size(&tablet, &region, 0).unwrap_or_default();
                let keys = get_region_approximate_keys(&tablet, &region, 0).unwrap_or_default();
                let _ = self.router.send(
                    region.get_id(),
                    CasualMessage::RegionApproximateSize { size },
                );
                let _ = self.router.send(
                    region.get_id(),
                    CasualMessage::RegionApproximateKeys { keys },
                );
                cb(size, keys);
            }
        }
    }
}

fn new_split_region<EK>(
    region_epoch: RegionEpoch,
    split_keys: Vec<Vec<u8>>,
    source: &'static str,
) -> CasualMessage<EK>
where
    EK: KvEngine,
{
    CasualMessage::SplitRegion {
        region_epoch,
        split_keys,
        callback: Callback::None,
        source: source.into(),
    }
}

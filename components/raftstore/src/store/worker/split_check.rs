// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::{self, Display, Formatter};
use std::mem;

use engine_traits::{CfName, IterOptions, Iterable, Iterator, KvEngine, CF_WRITE, LARGE_CFS};
use kvproto::metapb::{Region, RegionBucket, RegionEpoch};
use kvproto::pdpb::CheckPolicy;

#[cfg(any(test, feature = "testexport"))]
use crate::coprocessor::Config;
use crate::coprocessor::CoprocessorHost;
use crate::coprocessor::SplitCheckerHost;
use crate::store::{Callback, CasualMessage, CasualRouter};
use crate::Result;
use configuration::{ConfigChange, Configuration};
use file_system::{IOType, WithIOType};
use tikv_util::keybuilder::KeyBuilder;
use tikv_util::worker::Runnable;
use tikv_util::{box_err, debug, error, info, warn};

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
    fn new<E: KvEngine>(
        db: &E,
        cfs: &[CfName],
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
    ) -> Result<MergedIterator<E::Iterator>> {
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
        }
    }
}

pub struct Runner<E, S>
where
    E: KvEngine,
{
    router: S,
    coprocessor: CoprocessorHost<E>,
}

impl<E, S> Runner<E, S>
where
    E: KvEngine,
    S: CasualRouter<E>,
{
    pub fn new(_engine: E, router: S, coprocessor: CoprocessorHost<E>) -> Runner<E, S> {
        Runner {
            router,
            coprocessor,
        }
    }

    /// Checks a Region with split checkers to produce split keys and generates split admin command.
    fn check_split(&mut self, tablet: E, region: &Region, auto_split: bool, policy: CheckPolicy) {
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
            .new_split_checker_host(region, &tablet, auto_split, policy);
        /*if host.skip() {
            debug!("skip split check"; "region_id" => region.get_id());
            return;
        }*/

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
            CheckPolicy::Approximate => {
                if host.enable_region_bucket() {
                    let mut bucket_keys = match host.approximate_bucket_keys(region, &tablet) {
                        Ok(keys) => keys.into_iter().map(|k| k.to_vec()).collect(),
                        Err(e) => {
                            error!(%e;
                                "failed to get approximate bucket key";
                                "region_id" => region_id,
                            );
                            vec![]
                        }
                    };
                    info!("starting approximate_bucket_keys {}", bucket_keys.len());
                    if bucket_keys.len() > 0 {
                        bucket_keys.insert(0, start_key.clone()); //
                        bucket_keys.push(end_key.clone());
                        let mut i = 0;
                        let mut region_buckets = vec![];
                        while i < bucket_keys.len() - 1 {
                            let mut region_bucket = RegionBucket::default();
                            region_bucket.start_key = bucket_keys[i].clone();
                            region_bucket.end_key = bucket_keys[i + 1].clone();
                            region_buckets.push(region_bucket);
                            i += 1;
                        }
                        let _ = self.router.send(
                            region.get_id(),
                            CasualMessage::RefreshRegionBuckets { region_buckets },
                        );
                    }
                }
                match host.approximate_split_keys(region, &tablet) {
                    Ok(keys) => keys
                        .into_iter()
                        .map(|k| keys::origin_key(&k).to_vec())
                        .collect(),
                    Err(e) => {
                        error!(%e;
                            "failed to get approximate split key, try scan way";
                            "region_id" => region_id,
                        );
                        match self.scan_split_keys(&mut host, &tablet, region, &start_key, &end_key)
                        {
                            Ok(keys) => keys,
                            Err(e) => {
                                error!(%e; "failed to scan split key"; "region_id" => region_id,);
                                return;
                            }
                        }
                    }
                }
            }
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
        host: &mut SplitCheckerHost<'_, E>,
        tablet: &E,
        region: &Region,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<Vec<Vec<u8>>> {
        let timer = CHECK_SPILT_HISTOGRAM.start_coarse_timer();
        let mut region_buckets = vec![];
        MergedIterator::<<E as Iterable>::Iterator>::new(
            tablet, LARGE_CFS, start_key, end_key, false,
        )
        .map(|mut iter| {
            let mut size = 0;
            let mut keys = 0;
            let mut bucket_size: u64 = 0;
            let mut bucket_start_key = start_key.to_vec();
            while let Some(e) = iter.next() {
                if host.on_kv(region, &e) {
                    return;
                }
                size += e.entry_size() as u64;
                keys += 1;
                if host.enable_region_bucket() {
                    bucket_size += e.entry_size() as u64;
                    if bucket_size >= host.region_bucket_size() {
                        let mut region_bucket = RegionBucket::default();
                        region_bucket.start_key = bucket_start_key;
                        region_bucket.end_key = e.key().to_vec();
                        region_buckets.push(region_bucket);
                        bucket_size = 0;
                        bucket_start_key = e.key().to_vec();
                    }
                }
            }
            let buckets_len = region_buckets.len();
            if buckets_len > 1 {
                region_buckets[buckets_len - 1].end_key = end_key.to_vec();
            }

            // if we scan the whole range, we can update approximate size and keys with accurate value.
            info!(
                "update approximate size and keys with accurate value";
                "region_id" => region.get_id(),
                "size" => size,
                "keys" => keys,
                "bucket_count" => region_buckets.len(),
                "bucket_size" => bucket_size,
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateSize { size },
            );
            let _ = self.router.send(
                region.get_id(),
                CasualMessage::RegionApproximateKeys { keys },
            );
            if host.enable_region_bucket() {
                let _ = self.router.send(
                    region.get_id(),
                    CasualMessage::RefreshRegionBuckets { region_buckets },
                );
            }
        })?;
        timer.observe_duration();

        Ok(host.split_keys())
    }

    fn change_cfg(&mut self, change: ConfigChange) {
        info!(
            "split check config updated";
            "change" => ?change
        );
        self.coprocessor.cfg.update(change);
    }
}

impl<E, S> Runnable for Runner<E, S>
where
    E: KvEngine,
    S: CasualRouter<E>,
{
    type Task = Task<E>;
    fn run(&mut self, task: Task<E>) {
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
            Task::Validate(f) => f(&self.coprocessor.cfg),
        }
    }
}

fn new_split_region<E>(
    region_epoch: RegionEpoch,
    split_keys: Vec<Vec<u8>>,
    source: &'static str,
) -> CasualMessage<E>
where
    E: KvEngine,
{
    CasualMessage::SplitRegion {
        region_epoch,
        split_keys,
        callback: Callback::None,
        source: source.into(),
    }
}

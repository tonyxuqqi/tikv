// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;
use std::u64;

use collections::HashMap;
use engine_traits::{DeleteStrategy, Range, CF_LOCK, CF_RAFT, DATA_CFS};
use engine_traits::{
    Engines, ImportExt, IngestExternalFileOptions, KvEngine, Mutable, RaftEngine,
    WriteBatch,
};
use fail::fail_point;
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
use raft::eraftpb::Snapshot as RaftSnapshot;
use tikv_util::time::Instant;
use tikv_util::{box_err, box_try, defer, error, info, thd_name, warn};

use crate::coprocessor::CoprocessorHost;
use crate::store::peer_storage::{
    JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING, JOB_STATUS_FAILED, JOB_STATUS_FINISHED,
    JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
};
use crate::store::snap::{plain_file_used, Error, Result, SNAPSHOT_CFS};
use crate::store::transport::CasualRouter;
use crate::store::{
    self, check_abort, ApplyOptions, CasualMessage, SnapEntry, SnapKey, SnapManager,
};
use yatp::pool::{Builder, ThreadPool};
use yatp::task::future::TaskCell;

use file_system::{IOType, WithIOType};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use crate::store::fsm::apply::PrepareMergeState;
use super::metrics::*;

const GENERATE_POOL_SIZE: usize = 5;

// used to periodically check whether we should delete a stale peer's range in region runner

#[cfg(test)]
pub const STALE_PEER_CHECK_TICK: usize = 1; // 1000 milliseconds

#[cfg(not(test))]
pub const STALE_PEER_CHECK_TICK: usize = 10; // 10000 milliseconds

// used to periodically check whether schedule pending applies in region runner
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 1_000; // 1000 milliseconds

/// Region related task
//#[derive(Debug)]
pub enum Task {
    Gen {
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    },
    Apply {
        region_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        tablet_suffix: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    }, 
    TargetRegionIngestSST {
        src_region_id: u64,
        src_tablet_suffix: u64,
        dst_region_id: u64,
        dst_tablet_suffix: u64,
        cb: Box<dyn FnOnce(u64) + Send>,
    },
    PrepareMerge {
        src_region_id: u64,
        src_tablet_suffix: u64,
        src_start_key: Vec<u8>,
        src_end_key: Vec<u8>,
        target_region_id: u64,
        target_tablet_suffix: u64,
        target_start_key: Vec<u8>,
        target_end_key: Vec<u8>,
        prepare_merge: PrepareMergeState,
        cb: Box<dyn FnOnce(PrepareMergeState) + Send>, 
    },
}

impl Task {
    pub fn destroy(
        region_id: u64,
        tablet_suffix: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) -> Task {
        Task::Destroy {
            region_id,
            tablet_suffix,
            start_key,
            end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy {
                region_id,
                tablet_suffix,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {} {} [{}, {})",
                region_id,
                tablet_suffix,
                log_wrappers::Value::key(&start_key),
                log_wrappers::Value::key(&end_key)
            ),
            Task::PrepareMerge { src_region_id, .. } => write!(
                f,
                "PrepareMerge for source region:{}",
                src_region_id,
            ),
            Task::TargetRegionIngestSST {
                src_region_id,
                src_tablet_suffix: _,
                dst_region_id,
                ..
            } => write!(
                f,
                "TargetRegionIngestSST for {} {}",
                src_region_id, dst_region_id
            ),
        }
    }
}

#[derive(Clone)]
struct StalePeerInfo {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    // Once the oldest snapshot sequence exceeds this, it ensures that no one is
    // reading on this peer anymore. So we can safely call `delete_files_in_range`
    // , which may break the consistency of snapshot, of this peer range.
    pub stale_sequence: u64,
}

/// (region_id, suffix)
pub type Key = (u64, u64);

/// A structure records all ranges to be deleted with some delay.
/// The delay is because there may be some coprocessor requests related to these ranges.
type PendingDeleteRanges = HashMap<Key, Vec<StalePeerInfo>>;

#[derive(Clone)]
struct SnapContext<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    engines: Engines<EK, ER>,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_range: bool,
    pending_delete_ranges: PendingDeleteRanges,
    coprocessor_host: CoprocessorHost<EK>,
    router: R,
}

impl<EK, ER, R> SnapContext<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
{
    /// Generates the snapshot of the Region.
    fn generate_snap(
        &self,
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) -> Result<()> {
        // do we need to check leader here?
        let snap = box_try!(store::do_snapshot(
            self.mgr.clone(),
            &self.engines,
            region_id,
            tablet_suffix,
            for_balance,
            canceled,
        ));
        // Only enable the fail point when the region id is equal to 1, which is
        // the id of bootstrapped region in tests.
        fail_point!("region_gen_snap", region_id == 1, |_| Ok(()));
        if let Err(e) = notifier.try_send(snap) {
            info!(
                "failed to notify snap result, leadership may have changed, ignore error";
                "region_id" => region_id,
                "err" => %e,
            );
        }
        // The error can be ignored as snapshot will be sent in next heartbeat in the end.
        let _ = self
            .router
            .send(region_id, CasualMessage::SnapshotGenerated);
        Ok(())
    }

    /// Handles the task of generating snapshot of the Region. It calls `generate_snap` to do the actual work.
    fn handle_gen(
        &self,
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) {
        fail_point!("before_region_gen_snap", |_| ());
        SNAP_COUNTER.generate.all.inc();
        if canceled.load(Ordering::Relaxed) {
            info!("generate snap is canceled"; "region_id" => region_id);
            return;
        }

        let start = Instant::now();
        let _io_type_guard = WithIOType::new(if for_balance {
            IOType::LoadBalance
        } else {
            IOType::Replication
        });

        if let Err(e) =
            self.generate_snap(region_id, tablet_suffix, canceled, notifier, for_balance)
        {
            error!(%e; "failed to generate snap!!!"; "region_id" => region_id,);
            return;
        }

        SNAP_COUNTER.generate.success.inc();
        SNAP_HISTOGRAM
            .generate
            .observe(start.saturating_elapsed_secs());
    }

    /// Applies snapshot data of the Region.
    fn apply_snap(&mut self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<()> {
        info!("begin apply snap data"; "region_id" => region_id);
        fail_point!("region_apply_snap", |_| { Ok(()) });
        check_abort(&abort)?;
        let region_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState =
            match box_try!(self.engines.kv.get_msg_cf(CF_RAFT, &region_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get region_state from {}",
                        log_wrappers::Value::key(&region_key)
                    ));
                }
            };

        // clear up origin data.
        let region = region_state.get_region().clone();
        check_abort(&abort)?;
        check_abort(&abort)?;
        fail_point!("apply_snap_cleanup_range");

        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState =
            match box_try!(self.engines.kv.get_msg_cf(CF_RAFT, &state_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get raftstate from {}",
                        log_wrappers::Value::key(&state_key)
                    ));
                }
            };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(region_id, term, idx);
        self.mgr.register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.deregister(&snap_key, &SnapEntry::Applying);
        });
        let mut s = box_try!(self.mgr.get_snapshot_for_applying(&snap_key));
        if !s.exists() {
            return Err(box_err!("missing snapshot file {}", s.path()));
        }
        check_abort(&abort)?;
        let timer = Instant::now();
        let options = ApplyOptions {
            db: self.engines.kv.clone(),
            region,
            abort: Arc::clone(&abort),
            write_batch_size: self.batch_size,
            coprocessor_host: self.coprocessor_host.clone(),
        };
        s.apply(options)?;

        let mut wb = self.engines.kv.write_batch();
        region_state.set_state(PeerState::Normal);
        box_try!(wb.put_msg_cf(CF_RAFT, &region_key, &region_state));
        box_try!(wb.delete_cf(CF_RAFT, &keys::snapshot_raft_state_key(region_id)));
        wb.write().unwrap_or_else(|e| {
            panic!("{} failed to save apply_snap result: {:?}", region_id, e);
        });
        info!(
            "apply new data";
            "region_id" => region_id,
            "time_takes" => ?timer.saturating_elapsed(),
        );
        Ok(())
    }

    /// Tries to apply the snapshot of the specified Region. It calls `apply_snap` to do the actual work.
    fn handle_apply(&mut self, region_id: u64, status: Arc<AtomicUsize>) {
        let _ = status.compare_exchange(
            JOB_STATUS_PENDING,
            JOB_STATUS_RUNNING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        SNAP_COUNTER.apply.all.inc();
        // let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        // let timer = apply_histogram.start_coarse_timer();
        let start = Instant::now();

        match self.apply_snap(region_id, Arc::clone(&status)) {
            Ok(()) => {
                status.swap(JOB_STATUS_FINISHED, Ordering::SeqCst);
                SNAP_COUNTER.apply.success.inc();
            }
            Err(Error::Abort) => {
                warn!("applying snapshot is aborted"; "region_id" => region_id);
                assert_eq!(
                    status.swap(JOB_STATUS_CANCELLED, Ordering::SeqCst),
                    JOB_STATUS_CANCELLING
                );
                SNAP_COUNTER.apply.abort.inc();
            }
            Err(e) => {
                error!(%e; "failed to apply snap!!!");
                status.swap(JOB_STATUS_FAILED, Ordering::SeqCst);
                SNAP_COUNTER.apply.fail.inc();
            }
        }

        SNAP_HISTOGRAM
            .apply
            .observe(start.saturating_elapsed_secs());
    }

    /// Cleans up the data within the range.
    fn cleanup_range(
        tablet: &EK,
        ranges: &[Range],
        mgr: &SnapManager,
        use_delete_range: bool,
    ) -> Result<()> {
        tablet
            .delete_all_in_range(DeleteStrategy::DeleteFiles, &ranges)
            .unwrap_or_else(|e| {
                error!("failed to delete files in range"; "err" => %e);
            });
        Self::delete_all_in_range(tablet, ranges, mgr, use_delete_range)?;
        tablet
            .delete_all_in_range(DeleteStrategy::DeleteBlobs, &ranges)
            .unwrap_or_else(|e| {
                error!("failed to delete files in range"; "err" => %e);
            });
        Ok(())
    }

    /// Inserts a new pending range, and it will be cleaned up with some delay.
    fn insert_pending_delete_range(
        &mut self,
        tablet: &EK,
        region_id: u64,
        tablet_suffix: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) {
        info!("register deleting data in range";
            "region_id" => region_id,
            "start_key" => log_wrappers::Value::key(start_key),
            "end_key" => log_wrappers::Value::key(end_key),
        );
        let seq = tablet.get_latest_sequence_number();
        self.pending_delete_ranges
            .entry((region_id, tablet_suffix))
            .or_insert_with(Default::default)
            .push(StalePeerInfo {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                stale_sequence: seq,
            });
    }

    /// Cleans up stale ranges.
    fn clean_stale_ranges(&mut self) {
        STALE_PEER_PENDING_DELETE_RANGE_GAUGE.set(self.pending_delete_ranges.len() as f64);

        let mut to_clean = vec![];
        for ((region_id, tablet_suffix), ranges) in &mut self.pending_delete_ranges {
            let tablet = match self
                .engines
                .tablets
                .open_tablet_cache(*region_id, *tablet_suffix)
            {
                Some(t) => t,
                None => {
                    to_clean.push((*region_id, *tablet_suffix));
                    continue;
                }
            };
            let oldest_sequence = tablet
                .get_oldest_snapshot_sequence_number()
                .unwrap_or(u64::MAX);
            let (mgr, use_delete_range) = (&self.mgr, self.use_delete_range);
            ranges.retain(|r| {
                if r.stale_sequence < oldest_sequence {
                    info!("delete data in range because of stale"; "region_id" => region_id,
                  "start_key" => log_wrappers::Value::key(&r.start_key),
                  "end_key" => log_wrappers::Value::key(&r.end_key));
                    if let Err(e) = Self::cleanup_range(
                        &tablet,
                        &[Range::new(&r.start_key, &r.end_key)],
                        mgr,
                        use_delete_range,
                    ) {
                        error!("failed to cleanup stale range"; "err" => %e);
                        return true;
                    }
                    false
                } else {
                    true
                }
            });
        }
        for key in &to_clean {
            info!("remove stale ranges as tablet is stale"; "region_id" => key.0, "tablet_suffix" => key.1);
            self.pending_delete_ranges.remove(key);
        }
    }

    fn clean_stale_tablets(&self) {
        let root = self.engines.tablets.tablets_path();
        let dir = match std::fs::read_dir(&root) {
            Ok(dir) => dir,
            Err(e) => {
                info!("skip cleaning stale tablets: {:?}", e);
                return;
            }
        };
        for path in dir.flatten() {
            let file_name = path.file_name().into_string().unwrap();
            let mut parts = file_name.split('_');
            let (region_id, suffix) = match (
                parts.next().map(|p| p.parse()),
                parts.next().map(|p| p.parse()),
            ) {
                (Some(Ok(r)), Some(Ok(s))) => (r, s),
                _ => continue,
            };
            if self
                .engines
                .tablets
                .open_tablet_cache(region_id, suffix)
                .is_some()
            {
                continue;
            }
            if self.engines.tablets.is_tombstoned(region_id, suffix) {
                if let Err(e) = self.engines.tablets.destroy_tablet(region_id, suffix) {
                    info!("failed to destroy tablet {} {}: {:?}", region_id, suffix, e);
                }
            }
        }
    }

    /// Checks the number of files at level 0 to avoid write stall after ingesting sst.
    /// Returns true if the ingestion causes write stall.
    fn ingest_maybe_stall(&self, tablet: &EK) -> bool {
        for cf in SNAPSHOT_CFS {
            // no need to check lock cf
            if plain_file_used(cf) {
                continue;
            }
            if tablet.ingest_maybe_slowdown_writes(cf).expect("cf") {
                return true;
            }
        }
        false
    }

    fn delete_all_in_range(
        tablet: &EK,
        ranges: &[Range],
        mgr: &SnapManager,
        use_delete_range: bool,
    ) -> Result<()> {
        for cf in tablet.cf_names() {
            let strategy = if cf == CF_LOCK {
                DeleteStrategy::DeleteByKey
            } else if use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByWriter {
                    sst_path: mgr.get_temp_path_for_ingest(),
                }
            };
            box_try!(tablet.delete_ranges_cf(cf, strategy, ranges));
        }

        Ok(())
    }
}

pub struct Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pool: ThreadPool<TaskCell>,
    ctx: SnapContext<EK, ER, R>,
    // we may delay some apply tasks if level 0 files to write stall threshold,
    // pending_applies records all delayed apply task, and will check again later
    pending_applies: VecDeque<Task>,
    clean_stale_tick: usize,
    clean_stale_check_interval: Duration,
}

impl<EK, ER, R> Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
{
    pub fn new(
        engines: Engines<EK, ER>,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_range: bool,
        coprocessor_host: CoprocessorHost<EK>,
        router: R,
    ) -> Runner<EK, ER, R> {
        Runner {
            pool: Builder::new(thd_name!("snap-generator"))
                .max_thread_count(GENERATE_POOL_SIZE)
                .build_future_pool(),
            ctx: SnapContext {
                engines,
                mgr,
                batch_size,
                use_delete_range,
                pending_delete_ranges: PendingDeleteRanges::default(),
                coprocessor_host,
                router,
            },
            pending_applies: VecDeque::new(),
            clean_stale_tick: 0,
            clean_stale_check_interval: Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL),
        }
    }

    /// Tries to apply pending tasks if there is some.
    fn handle_pending_applies(&mut self) {
        fail_point!("apply_pending_snapshot", |_| {});
        while !self.pending_applies.is_empty() {
            if let Some(Task::Apply { region_id, status }) = self.pending_applies.pop_front() {
                self.ctx.handle_apply(region_id, status);
            }
        }
    }
}

impl<EK, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Gen {
                region_id,
                tablet_suffix,
                canceled,
                notifier,
                for_balance,
            } => {
                // It is safe for now to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let ctx = self.ctx.clone();

                self.pool.spawn(async move {
                    tikv_alloc::add_thread_memory_accessor();
                    ctx.handle_gen(region_id, tablet_suffix, canceled, notifier, for_balance);
                    tikv_alloc::remove_thread_memory_accessor();
                });
            }
            task @ Task::Apply { .. } => {
                fail_point!("on_region_worker_apply", true, |_| {});
                // to makes sure applying snapshots in order.
                self.pending_applies.push_back(task);
                self.handle_pending_applies();
                if !self.pending_applies.is_empty() {
                    // delay the apply and retry later
                    SNAP_COUNTER.apply.delay.inc()
                }
            }
            Task::Destroy {
                region_id,
                tablet_suffix,
                start_key,
                end_key,
            } => {
                fail_point!("on_region_worker_destroy", true, |_| {});
                // try to delay the range deletion because
                // there might be a coprocessor request related to this range
                let tablet = match self
                    .ctx
                    .engines
                    .tablets
                    .open_tablet_cache(region_id, tablet_suffix)
                {
                    Some(t) => t,
                    None => return,
                };
                self.ctx.insert_pending_delete_range(
                    &tablet,
                    region_id,
                    tablet_suffix,
                    &start_key,
                    &end_key,
                );

                // try to delete stale ranges if there are any
                if !self.ctx.ingest_maybe_stall(&tablet) {
                    self.ctx.clean_stale_ranges();
                }
            }
            Task::PrepareMerge {
                src_region_id,
                src_tablet_suffix,
                src_start_key,
                src_end_key,
                target_region_id,
                target_tablet_suffix,
                target_start_key,
                target_end_key,
                prepare_merge,
                cb, 
            } => {
                let clean_data = |region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>, tablet_suffix: u64| {
                    let tablet = match self
                    .ctx
                    .engines
                    .tablets
                    .open_tablet_cache(region_id, tablet_suffix)
                    {
                        Some(t) => t,
                        None => return,
                    };
                    info!("RegionPrepareMerge region {} start {} end {} tablet_suffix {}", region_id, hex::encode(&start_key), hex::encode(&end_key), tablet_suffix);

                    let end = if end_key.len() != 0 {
                        keys::data_key(&end_key)
                    } else {
                        keys::DATA_MAX_KEY.to_vec()
                    };
                    let start = keys::data_key(&start_key);
                    /*
                    let result =
                        tablet.filter_sst("", &keys::data_key(&start_key), &end); 
                    */

                    for cf in DATA_CFS {
                        if start_key.len() != 0 {
                            let range = Range::new(keys::DATA_MIN_KEY, start.as_slice());
                            info!("delete ranges from min key to {} for cf {}, region_id {}", hex::encode(&start_key), cf, region_id);
                            tablet.delete_ranges_cf(cf, DeleteStrategy::DeleteByKey, &[range]).unwrap();
                        }
                        if end_key.len() != 0 {
                            let range = Range::new(&end, keys::DATA_MAX_KEY);
                            info!("delete ranges from {} to max key for cf {}, region_id {}", hex::encode(&end_key), cf, region_id);
                            tablet.delete_ranges_cf(cf, DeleteStrategy::DeleteByKey, &[range]).unwrap();
                        }
                    }
                    tablet.flush(true).unwrap(); // flush mem table data;
                    for cf in DATA_CFS {
                        if start_key.len() != 0 {
                            tablet
                            .compact_range(
                                cf,
                                Some(keys::DATA_MIN_KEY),
                                Some(start.as_slice()),
                                false,
                                1, /* threads */
                            )
                            .unwrap();
                        }
                        if end_key.len() != 0 {
                            tablet
                            .compact_range(
                                cf,
                                Some(end.as_slice()),
                                Some(keys::DATA_MAX_KEY),
                                false,
                                1, /* threads */
                            )
                            .unwrap();
                        }
                    }
                };
                let timer = MERGE_SOURCE_REGION_HISTOGRAM.start_coarse_timer();
                clean_data(src_region_id, src_start_key, src_end_key, src_tablet_suffix);
                timer.observe_duration();
                info!(
                    "SourceRegionPrepareMerge finished";
                    "region_id" => src_region_id
                );
                let timer = MERGE_TARGET_REGION_HISTOGRAM.start_coarse_timer();
                clean_data(target_region_id, target_start_key, target_end_key, target_tablet_suffix);
                info!(
                    "TargetRegionPrepareMerge finished";
                    "region_id" => target_region_id
                );
                timer.observe_duration();
                cb(prepare_merge);
            }

            Task::TargetRegionIngestSST {
                src_region_id,
                src_tablet_suffix,
                dst_region_id,
                dst_tablet_suffix,
                cb,
            } => {
                let timer = MERGE_SST_INGEST_HISTOGRAM.start_coarse_timer();
                let src_tablet = self
                    .ctx
                    .engines
                    .tablets
                    .open_tablet_cache(src_region_id, src_tablet_suffix).unwrap();
                /*
                src_tablet.flush(true).unwrap(); // flush mem table data;
                for cf in DATA_CFS {
                    let sst_files = src_tablet.get_cf_files(cf, 0).unwrap().iter().map(|sst_file| sst_file.get_file_name().to_string()).collect::<Vec<_>>();
                    if sst_files.len() != 0 {
                        src_tablet.compact_files_cf(cf, sst_files, Some(1), 0, false).unwrap();
                    }
                }*/

                let dst_tablet = self
                    .ctx
                    .engines
                    .tablets
                    .open_tablet_cache(dst_region_id, dst_tablet_suffix).unwrap();

                let mut success = true; 
                for cf in DATA_CFS {
                    if !success {
                        break;
                    }

                    let num_of_level = src_tablet.get_cf_num_of_level(cf);
                    for i in 0..num_of_level {
                        if !success {
                            break;
                        }
                        let level = num_of_level - i - 1;
                        let mut sst_files = src_tablet.get_cf_files(cf, level).unwrap();
                        if level == 0 {
                            sst_files.sort_by(|sst1, sst2| sst1.get_file_name().cmp(sst2.get_file_name()));
                        }
                        let mut valid_sst_files: Vec<String> = vec![];
                        let mut smallest_seqnos: Vec<u64> = vec![];
                        let mut largest_seqnos: Vec<u64> = vec![];
                        //let mut additional_sst_files: Vec<SSTFile> = vec![];
                        for sst_file in &sst_files {
                            if !success {
                                break;
                            }
                            let file_name = sst_file.get_file_name();
                            let smallest_seqno = sst_file.get_smallest_seqno();
                            let largest_seqno = sst_file.get_largest_seqno();
                            let mut file_to_injest = "";
                            let full_file_name = src_tablet.path().to_string() + file_name;
                            file_to_injest = &full_file_name; 
                            if file_to_injest.len() != 0 {
                                if level != 0 {
                                    valid_sst_files.push(file_to_injest.to_string()); 
                                    smallest_seqnos.push(smallest_seqno);
                                    largest_seqnos.push(largest_seqno);
                                } else {
                                    info!("ingest level 0 file {} region_id:{} smallest_seq {}, largest_seq {}", file_to_injest, dst_region_id, smallest_seqno, largest_seqno);
                                    let mut ingest_opt =
                                        <EK as ImportExt>::IngestExternalFileOptions::new();
                                    ingest_opt.move_files(true);
                                    let result = dst_tablet
                                        .ingest_external_file_cf_with_seqno(cf, &ingest_opt, &[&file_to_injest], &[smallest_seqno], &[largest_seqno]);
                                    match result {
                                        Ok(_) => {},
                                        Err(e) =>  {
                                            error!(
                                                "failed ingest external file";
                                                "region_id" => dst_region_id,
                                                "err" => %e,
                                            );
                                            success = false;
                                        }
                                    }
                                }
                            }
                        }
                        if valid_sst_files.len() != 0 {
                            let mut ingest_opt =
                                <EK as ImportExt>::IngestExternalFileOptions::new();
                            ingest_opt.move_files(true);
                            let v: Vec<&str> = valid_sst_files.iter().map(|x| x.as_ref()).collect();
                            let result = dst_tablet
                                        .ingest_external_file_cf_with_seqno(cf, &ingest_opt, &v, &smallest_seqnos, &largest_seqnos);
                            info!("ingested external files {:?}, {:?}, {:?}", v, smallest_seqnos, largest_seqnos);
                            match result {
                                Ok(_) => {},
                                Err(e) =>  {
                                    error!(
                                        "failed ingest external file";
                                        "region_id" => dst_region_id,
                                        "err" => %e,
                                    );
                                    success = false;
                                }
                            }
                        } else {
                            info!("no sst files at level {}", level);
                        }
                    }
                }

                cb(src_region_id);
                info!(
                    "TargetRegionIngestSST finished";
                    "src_region_id" => src_region_id,
                    "dst_region_id" => dst_region_id
                );
                timer.observe_duration(); 
            }
        }
    }

    fn shutdown(&mut self) {
        self.pool.shutdown();
    }
}

impl<EK, ER, R> RunnableWithTimer for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    fn on_timeout(&mut self) {
        self.handle_pending_applies();
        self.clean_stale_tick += 1;
        if self.clean_stale_tick >= STALE_PEER_CHECK_TICK {
            self.ctx.clean_stale_ranges();
            self.ctx.clean_stale_tablets();
            self.clean_stale_tick = 0;
        }
    }

    fn get_interval(&self) -> Duration {
        self.clean_stale_check_interval
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{mpsc, Arc};
    use std::thread;
    use std::time::Duration;

    use crate::coprocessor::CoprocessorHost;
    use crate::store::peer_storage::JOB_STATUS_PENDING;
    use crate::store::snap::tests::{get_test_db_for_regions, get_test_tablets_for_regions};
    use crate::store::worker::RegionRunner;
    use crate::store::{CasualMessage, SnapKey, SnapManager};
    use engine_test::ctor::CFOptions;
    use engine_test::ctor::ColumnFamilyOptions;
    use engine_test::kv::KvTestEngine;
    use engine_traits::{
        CFNamesExt, CompactExt, Iterable, MiscExt, Mutable, Peekable, SyncMutable, WriteBatch,
        WriteBatchExt,
    };
    use engine_traits::{CF_DEFAULT, CF_RAFT};
    use kvproto::raft_serverpb::{PeerState, RegionLocalState};
    use tempfile::Builder;
    use tikv_util::worker::Worker;

    use super::*;

    #[test]
    fn test_pending_applies() {
        let temp_dir = Builder::new()
            .prefix("test_pending_applies")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_slowdown_writes_trigger(5);
        cf_opts.set_disable_auto_compactions(true);
        let kv_cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("write", cf_opts.clone()),
            CFOptions::new("lock", cf_opts.clone()),
            CFOptions::new("raft", cf_opts.clone()),
        ];
        let raft_cfs_opt = CFOptions::new(CF_DEFAULT, cf_opts);
        let engine = get_test_db_for_regions(
            &temp_dir,
            None,
            Some(raft_cfs_opt),
            None,
            Some(kv_cfs_opts),
            &[1, 2, 3, 4, 5, 6],
        )
        .unwrap();

        for cf_name in engine.kv.cf_names() {
            for i in 0..6 {
                engine.kv.put_cf(cf_name, &[i], &[i]).unwrap();
                engine.kv.put_cf(cf_name, &[i + 1], &[i + 1]).unwrap();
                engine.kv.flush_cf(cf_name, true).unwrap();
                // check level 0 files
                assert_eq!(
                    engine
                        .kv
                        .get_cf_num_files_at_level(cf_name, 0)
                        .unwrap()
                        .unwrap(),
                    u64::from(i) + 1
                );
            }
        }

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("snap-manager");
        let mut worker = bg_worker.lazy_build("snapshot-worker");
        let sched = worker.scheduler();
        let (router, receiver) = mpsc::sync_channel(1);
        let runner = RegionRunner::new(
            engine.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);

        let gen_and_apply_snap = |id: u64| {
            // construct snapshot
            let (tx, rx) = mpsc::sync_channel(1);
            sched
                .schedule(Task::Gen {
                    region_id: id,
                    tablet_suffix: 0,
                    canceled: Arc::new(AtomicBool::new(false)),
                    notifier: tx,
                    for_balance: false,
                })
                .unwrap();
            let s1 = rx.recv().unwrap();
            match receiver.recv() {
                Ok((region_id, CasualMessage::SnapshotGenerated)) => {
                    assert_eq!(region_id, id);
                }
                msg => panic!("expected SnapshotGenerated, but got {:?}", msg),
            }
            let data = s1.get_data();
            let key = SnapKey::from_snap(&s1).unwrap();
            let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
            let mut s2 = mgr.get_snapshot_for_sending(&key).unwrap();
            let mut s3 = mgr.get_snapshot_for_receiving(&key, data).unwrap();
            io::copy(&mut s2, &mut s3).unwrap();
            s3.save().unwrap();

            // set applying state
            let mut wb = engine.kv.write_batch();
            let region_key = keys::region_state_key(id);
            let mut region_state = engine
                .kv
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_key)
                .unwrap()
                .unwrap();
            region_state.set_state(PeerState::Applying);
            wb.put_msg_cf(CF_RAFT, &region_key, &region_state).unwrap();
            wb.write().unwrap();

            // apply snapshot
            let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
            sched
                .schedule(Task::Apply {
                    region_id: id,
                    status,
                })
                .unwrap();
        };
        let wait_apply_finish = |id: u64| {
            let region_key = keys::region_state_key(id);
            loop {
                thread::sleep(Duration::from_millis(100));
                if engine
                    .kv
                    .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_key)
                    .unwrap()
                    .unwrap()
                    .get_state()
                    == PeerState::Normal
                {
                    break;
                }
            }
        };

        // snapshot will not ingest cause already write stall
        gen_and_apply_snap(1);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            6
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );

        wait_apply_finish(1);

        // the pending apply task should be finished and snapshots are ingested.
        // note that when ingest sst, it may flush memtable if overlap,
        // so here will two level 0 files.
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            2
        );

        // no write stall, ingest without delay
        gen_and_apply_snap(2);
        wait_apply_finish(2);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // snapshot will not ingest cause it may cause write stall
        gen_and_apply_snap(3);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );
        gen_and_apply_snap(4);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );
        gen_and_apply_snap(5);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );

        // make sure have checked pending applies
        wait_apply_finish(4);

        // before two pending apply tasks should be finished and snapshots are ingested
        // and one still in pending.
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // make sure have checked pending applies
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );
        wait_apply_finish(5);

        // the last one pending task finished
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            2
        );
    }

    #[test]
    fn test_source_region_prepare_merge_task() {
        let temp_dir = Builder::new()
            .prefix("test_source_region_prepare_merge")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_slowdown_writes_trigger(5);
        cf_opts.set_disable_auto_compactions(true);
        let kv_cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("write", cf_opts.clone()),
            CFOptions::new("lock", cf_opts.clone()),
            CFOptions::new("raft", cf_opts.clone()),
        ];
        let raft_cfs_opt = CFOptions::new(CF_DEFAULT, cf_opts);
        let engine = get_test_tablets_for_regions(
            &temp_dir,
            None,
            Some(raft_cfs_opt),
            None,
            Some(kv_cfs_opts),
            &[1],
        )
        .unwrap();

        let tablet = engine.tablets.open_tablet(1, 0);
        for cf_name in tablet.cf_names() {
            for i in 0..6 {
                tablet.put_cf(cf_name, &keys::data_key(&[i]), &[i]).unwrap();
                tablet
                    .put_cf(cf_name, &keys::data_key(&[i + 1]), &[i + 1])
                    .unwrap();
                tablet.flush_cf(cf_name, true).unwrap();
                // check level 0 files
                assert_eq!(
                    tablet
                        .get_cf_num_files_at_level(cf_name, 0)
                        .unwrap()
                        .unwrap(),
                    u64::from(i) + 1
                );
            }
        }
        tablet.set_compaction_filter_key_range(1, [0].to_vec(), [1].to_vec());

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("source_region_prepare_merge");
        let mut worker = bg_worker.lazy_build("source_region_prepare_merge");
        let sched = worker.scheduler();
        let (router, _receiver) = mpsc::sync_channel(1);
        let runner = RegionRunner::new(
            engine.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);

        let run_and_wait_prepare_merge_task = |id: u64, start_key: Vec<u8>, end_key: Vec<u8>| {
            // construct snapshot
            sched
                .schedule(Task::SourceRegionPrepareMerge {
                    region_id: id,
                    tablet_suffix: 0,
                    start_key,
                    end_key,
                    notifier: tx,
                    cb: Box::new(move |region_id| {
                        assert_eq!(region_id, id);
                    }),
                })
                .unwrap();
        };

        run_and_wait_prepare_merge_task(1, vec![0], vec![1]);
        let tablet = engine.tablets.open_tablet_cache(1, 0).unwrap();
        for cf_name in DATA_CFS {
            tablet
                .scan_cf(
                    cf_name,
                    keys::MIN_KEY,
                    keys::MAX_KEY,
                    false,
                    |key, value| {
                        //assert!(key[0] == 0);
                        Ok(true)
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn test_target_region_prepare_merge_task() {
        let temp_dir = Builder::new()
            .prefix("test_target_region_prepare_merge")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_slowdown_writes_trigger(5);
        cf_opts.set_disable_auto_compactions(true);
        let kv_cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("write", cf_opts.clone()),
            CFOptions::new("lock", cf_opts.clone()),
            CFOptions::new("raft", cf_opts.clone()),
        ];
        let raft_cfs_opt = CFOptions::new(CF_DEFAULT, cf_opts);
        let engine = get_test_tablets_for_regions(
            &temp_dir,
            None,
            Some(raft_cfs_opt),
            None,
            Some(kv_cfs_opts),
            &[1],
        )
        .unwrap();
        let tablet = engine.tablets.open_tablet(1, 0);
        for cf_name in tablet.cf_names() {
            for i in 0..6 {
                tablet.put_cf(cf_name, &keys::data_key(&[i]), &[i]).unwrap();
                tablet
                    .put_cf(cf_name, &keys::data_key(&[i + 1]), &[i + 1])
                    .unwrap();
                tablet.flush_cf(cf_name, true).unwrap();
                // check level 0 files
                assert_eq!(
                    tablet
                        .get_cf_num_files_at_level(cf_name, 0)
                        .unwrap()
                        .unwrap(),
                    u64::from(i) + 1
                );
            }
        }
        tablet.set_compaction_filter_key_range(1, [0].to_vec(), [1].to_vec());

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("target_region_prepare_merge");
        let mut worker = bg_worker.lazy_build("target_region_prepare_merge");
        let sched = worker.scheduler();
        let (router, _receiver) = mpsc::sync_channel(1);
        let runner = RegionRunner::new(
            engine.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);

        let run_and_wait_prepare_merge_task = |id: u64, start_key: Vec<u8>, end_key: Vec<u8>| {
            // construct snapshot
            sched
                .schedule(Task::TargetRegionPrepareMerge {
                    region_id: id,
                    tablet_suffix: 0,
                    start_key,
                    end_key,
                    cb: Box::new(move |region_id| {
                        assert_eq!(region_id, id);
                    }),
                })
                .unwrap();
        };

        run_and_wait_prepare_merge_task(1, vec![0], vec![1]);
        let tablet = engine.tablets.open_tablet_cache(1, 0).unwrap();
        for cf_name in DATA_CFS {
            tablet
                .scan_cf(
                    cf_name,
                    keys::MIN_KEY,
                    keys::MAX_KEY,
                    false,
                    |key, value| {
                        //assert!(key[0] == 0);
                        println!("{:?} {:?}", key, value);
                        Ok(true)
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn test_target_region_ingest_task() {
        let temp_dir = Builder::new()
            .prefix("test_target_region_prepare_merge")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_slowdown_writes_trigger(5);
        cf_opts.set_disable_auto_compactions(true);
        let kv_cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("write", cf_opts.clone()),
            CFOptions::new("lock", cf_opts.clone()),
            CFOptions::new("raft", cf_opts.clone()),
        ];
        let raft_cfs_opt = CFOptions::new(CF_DEFAULT, cf_opts);
        let engine = get_test_tablets_for_regions(
            &temp_dir,
            None,
            Some(raft_cfs_opt),
            None,
            Some(kv_cfs_opts),
            &[1, 2],
        )
        .unwrap();
        for region_id in 1..3 {
            let tablet = engine.tablets.open_tablet(region_id, 0);
            for cf_name in tablet.cf_names() {
                for i in 0..6 {
                    tablet.put_cf(cf_name, &keys::data_key(&[i]), &[i]).unwrap();
                    tablet
                        .put_cf(cf_name, &keys::data_key(&[i + 1]), &[i + 1])
                        .unwrap();
                    tablet.flush_cf(cf_name, true).unwrap();
                    // check level 0 files
                    assert_eq!(
                        tablet
                            .get_cf_num_files_at_level(cf_name, 0)
                            .unwrap()
                            .unwrap(),
                        u64::from(i) + 1
                    );
                }
            }
            tablet.set_compaction_filter_key_range(
                region_id,
                [(region_id - 1) as u8].to_vec(),
                [region_id as u8].to_vec(),
            );
        }

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("target_region_prepare_merge");
        let mut worker = bg_worker.lazy_build("target_region_prepare_merge");
        let sched = worker.scheduler();
        let (router, _receiver) = mpsc::sync_channel(1);
        let runner = RegionRunner::new(
            engine.clone(),
            mgr,
            0,
            true,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);
        let mut sst_file_maps_recv: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut run_and_wait_prepare_source_region_merge_task =
            |id: u64, start_key: Vec<u8>, end_key: Vec<u8>| {
                // construct snapshot
                sched
                    .schedule(Task::SourceRegionPrepareMerge {
                        region_id: id,
                        tablet_suffix: 0,
                        start_key,
                        end_key,
                        notifier: tx,
                        cb: Box::new(move |region_id| {
                            assert_eq!(region_id, id);
                        }),
                    })
                    .unwrap();
            };

        run_and_wait_prepare_source_region_merge_task(1, vec![0], vec![1]); // filter region 1 from key 0 to 1
        //assert!(sst_file_maps_recv.len() != 0);

        let run_and_wait_prepare_merge_task = |id: u64, start_key: Vec<u8>, end_key: Vec<u8>, source_region_id: u64| {
            // construct snapshot
            let (tx, rx) = mpsc::sync_channel(1);
            sched
                .schedule(Task::TargetRegionPrepareMerge {
                    region_id: id,
                    tablet_suffix: 0,
                    source_region_id,
                    start_key,
                    end_key,
                    notifier: tx,
                    cb: Box::new(move |region_id| {
                        assert_eq!(region_id, id);
                    }),
                })
                .unwrap();
            rx.recv().unwrap();
        };

        run_and_wait_prepare_merge_task(2, vec![1], vec![2], 1); // filter region 2 from key 1 to 2

        let tablet = engine.tablets.open_tablet_cache(2, 0).unwrap();
        for cf_name in DATA_CFS {
            let mut sum = 0;
            tablet
                .scan_cf(
                    cf_name,
                    keys::MIN_KEY,
                    keys::MAX_KEY,
                    false,
                    |key, value| {
                        sum += key[1] + 1;
                        println!("check before ingest {:?} {:?}", key, value);
                        Ok(true)
                    },
                )
                .unwrap();
        }

        let run_and_wait_sst_ingest_task =
            |src_id: u64, dst_id: u64, sst_file_maps: std::collections::HashMap<String, String>| {
                // construct snapshot
                sched
                    .schedule(Task::TargetRegionIngestSST {
                        src_region_id: src_id,
                        src_tablet_suffix: 0,
                        dst_region_id: dst_id,
                        dst_tablet_suffix: 0,
                        cb: Box::new(move |region_id| {
                            assert_eq!(region_id, dst_id);
                        }),
                    })
                    .unwrap();
            };

        run_and_wait_sst_ingest_task(1, 2, sst_file_maps_recv);
        let tablet = engine.tablets.open_tablet_cache(2, 0).unwrap();
        for cf_name in DATA_CFS {
            let mut sum = 0;
            tablet
                .scan_cf(
                    cf_name,
                    keys::MIN_KEY,
                    keys::MAX_KEY,
                    false,
                    |key, value| {
                        sum += key[1] + 1;
                        println!("{:?} {:?}", key, value);
                        Ok(true)
                    },
                )
                .unwrap();
            assert_eq!(sum, 3);
        }
    }
}

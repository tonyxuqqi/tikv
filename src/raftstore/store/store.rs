// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use protobuf::{self, Message, RepeatedField};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver as StdReceiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{cmp, mem, thread, u64};
use time::{self, Timespec};

use futures::{Stream, Future, Async, Poll};
use futures_cpupool::CpuPool;
use rocksdb::rocksdb_options::WriteOptions;
use rocksdb::{CompactionJobInfo, WriteBatch, DB};

use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb;
use kvproto::pdpb::{CheckPolicy, StoreStats};
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse, StatusCmdType, StatusResponse,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftMessage, RaftSnapshotData, RaftTruncatedState, RegionLocalState,
};
use raft::eraftpb::{ConfChangeType, MessageType};
use raft::{self, SnapshotStatus, INVALID_INDEX, NO_LIMIT};
use tokio_timer::timer::Handle;

use pd::{PdClient, PdRunner, PdTask};
use raftstore::coprocessor::split_observer::SplitObserver;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::{Error, Result};
use storage::{CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use util::collections::{HashMap, HashSet};
use util::mpsc::{loose_bounded, LooseBoundedSender, Receiver};
use util::rocksdb::{CompactedEvent, CompactionListener};
use util::sys as util_sys;
use util::time::{duration_to_sec, SlowTimer};
use util::timer::{Timer, GLOBAL_TIMER_HANDLE};
use util::transport::SendCh;
use util::worker::{FutureScheduler, FutureWorker, Scheduler, Stopped, Worker};
use util::future::CountDownLatch;
use util::{escape, rocksdb};

use super::cmd_resp::{bind_term, new_error};
use super::config::Config;
use super::engine::{Iterable, Mutable, Peekable, Snapshot as EngineSnapshot};
use super::keys::{
    self, data_end_key, data_key, enc_end_key, enc_start_key, origin_key, DATA_MAX_KEY,
};
use super::local_metrics::RaftMetrics;
use super::metrics::*;
use super::msg::{Callback, ReadResponse, StoreMsg, StoreTick};
use super::peer::{ConsistencyState, ReadyContext, StaleState};
use super::peer_agent::{ConfigProvider, StoreMeta, PeerAgent};
use super::peer_storage::{self, ApplySnapResult, CacheQueryStats};
use super::transport::Transport;
use super::worker::apply::{ApplyMetrics, ApplyRes, ChangePeer, ExecResult};
use super::worker::{
    ApplyRunner, ApplyTask, ApplyTaskRes, CleanupSSTRunner, CleanupSSTTask, CompactRunner,
    CompactTask, ConsistencyCheckRunner, ConsistencyCheckTask, LocalReader, RaftlogGcRunner,
    RaftlogGcTask, ReadTask, RegionRunner, RegionTask, SplitCheckRunner, SplitCheckTask,
    STALE_PEER_CHECK_INTERVAL,
};
use super::{
    util, Engines, SnapKey, SnapManager, SnapshotDeleter,
};
use import::SSTImporter;

const PENDING_VOTES_CAP: usize = 20;

pub struct StoreStat {
    pub lock_cf_bytes_written: u64,

    pub engine_total_bytes_written: u64,
    pub engine_total_keys_written: u64,

    pub engine_last_total_bytes_written: u64,
    pub engine_last_total_keys_written: u64,
}

impl Default for StoreStat {
    fn default() -> StoreStat {
        StoreStat {
            lock_cf_bytes_written: 0,
            engine_total_bytes_written: 0,
            engine_total_keys_written: 0,

            engine_last_total_bytes_written: 0,
            engine_last_total_keys_written: 0,
        }
    }
}

pub struct DestroyPeerJob {
    pub initialized: bool,
    pub async_remove: bool,
    pub region_id: u64,
    pub peer: metapb::Peer,
}

pub struct StoreInfo {
    pub engine: Arc<DB>,
    pub capacity: u64,
}

pub struct Store<T: 'static> {
    cfg: Arc<Config>,
    engines: Engines,
    store: metapb::Store,
    sender: LooseBoundedSender<StoreMsg>,
    receiver: Receiver<StoreMsg>,
    store_meta: Arc<Mutex<StoreMeta>>,

    split_check_worker: Worker<SplitCheckTask>,
    raftlog_gc_worker: Worker<RaftlogGcTask>,
    region_worker: Worker<RegionTask>,
    compact_worker: Worker<CompactTask>,
    pd_worker: FutureWorker<PdTask>,
    consistency_check_worker: Worker<ConsistencyCheckTask>,
    cleanup_sst_worker: Worker<CleanupSSTTask>,
    pub apply_worker: Worker<ApplyTask>,
    local_reader: Worker<ReadTask>,

    last_compact_checked_key: Vec<u8>,

    trans: T,

    pub coprocessor_host: Arc<CoprocessorHost>,

    pub importer: Arc<SSTImporter>,

    snap_mgr: SnapManager,

    raft_metrics: RaftMetrics,

    tag: String,

    start_time: Timespec,
    is_busy: bool,
    stopped: bool,

    store_stat: StoreStat,
    peer_agents: Vec<PeerAgent<T>>,
    count_down_latch: CountDownLatch,
    timer: Handle,
    poller: CpuPool,
}

impl<T: Transport> ConfigProvider<T> for Store<T> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        Arc::clone(&self.cfg)
    }

    #[inline]
    fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

    #[inline]
    fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.apply_worker.scheduler()
    }

    #[inline]
    fn read_scheduler(&self) -> Scheduler<ReadTask> {
        self.local_reader.scheduler()
    }

    #[inline]
    fn engines(&self) -> Engines {
        self.engines.clone()
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        Arc::clone(&self.coprocessor_host)
    }

    #[inline]
    fn pd_scheduler(&self) -> FutureScheduler<PdTask> {
        self.pd_worker.scheduler()
    }

    #[inline]
    fn raft_log_gc_scheduler(&self) -> Scheduler<RaftlogGcTask> {
        self.raftlog_gc_worker.scheduler()
    }
    
    #[inline]
    fn store_meta(&self) -> Arc<Mutex<StoreMeta>> {
        Arc::clone(&self.store_meta)
    }

    #[inline]
    fn consistency_check_scheduler(&self) -> Scheduler<ConsistencyCheckTask> {
        self.consistency_check_worker.scheduler()
    }

    #[inline]
    fn snap_manager(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    #[inline]
    fn split_check_scheduler(&self) -> Scheduler<SplitCheckTask> {
        self.split_check_worker.scheduler()
    }

    #[inline]
    fn cleanup_sst_scheduler(&self) -> Scheduler<CleanupSSTTask> {
        self.cleanup_sst_worker.scheduler()
    }

    #[inline]
    fn transport(&self) -> T {
        self.trans.clone()
    }

    #[inline]
    fn poller(&self) -> CpuPool {
        self.poller.clone()
    }

    #[inline]
    fn count_down_latch(&self) -> CountDownLatch {
        self.count_down_latch.clone()
    }
}

impl<T: Transport> Store<T> {
    #[cfg_attr(feature = "cargo-clippy", allow(too_many_arguments))]
    pub fn new(
        meta: metapb::Store,
        mut cfg: Config,
        engines: Engines,
        trans: T,
        mgr: SnapManager,
        pd_worker: FutureWorker<PdTask>,
        local_reader: Worker<ReadTask>,
        mut coprocessor_host: CoprocessorHost,
        importer: Arc<SSTImporter>,
        latch: CountDownLatch,
        poller: CpuPool,
    ) -> Result<Store<T>> {
        // TODO: we can get cluster meta regularly too later.
        cfg.validate()?;

        let tag = format!("[store {}]", meta.get_id());

        // TODO load coprocessors from configuration
        coprocessor_host
            .registry
            .register_admin_observer(100, box SplitObserver);
        
        let (tx, rx) = loose_bounded(cfg.notify_capacity);

        let store_meta = StoreMeta::new(PENDING_VOTES_CAP);

        let mut s = Store {
            cfg: Arc::new(cfg),
            store: meta,
            store_meta: Arc::new(Mutex::new(store_meta)),
            engines,
            sender: tx,
            receiver: rx,
            split_check_worker: Worker::new("split-check"),
            region_worker: Worker::new("snapshot-worker"),
            raftlog_gc_worker: Worker::new("raft-gc-worker"),
            compact_worker: Worker::new("compact-worker"),
            pd_worker,
            consistency_check_worker: Worker::new("consistency-check"),
            cleanup_sst_worker: Worker::new("cleanup-sst"),
            apply_worker: Worker::new("apply-worker"),
            local_reader,
            last_compact_checked_key: keys::DATA_MIN_KEY.to_vec(),
            trans,
            coprocessor_host: Arc::new(coprocessor_host),
            importer,
            snap_mgr: mgr,
            raft_metrics: RaftMetrics::default(),
            tag,
            start_time: time::get_time(),
            is_busy: false,
            stopped: false,
            store_stat: StoreStat::default(),
            peer_agents: vec![],
            count_down_latch: latch,
            timer: GLOBAL_TIMER_HANDLE.clone(),
            poller,
        };
        s.init()?;
        Ok(s)
    }

    /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if necessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let kv_engine = Arc::clone(&self.engines.kv);
        let mut total_cnt = 0;
        let mut tombstone_cnt = 0;

        let t = Instant::now();
        let mut kv_wb = WriteBatch::new();
        let mut raft_wb = WriteBatch::new();
        let mut local_states = vec![];
        kv_engine.scan_cf(CF_RAFT, start_key, end_key, false, |key, value| {
            let (region_id, suffix) = keys::decode_region_meta_key(key)?;
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_cnt += 1;

            let local_state = protobuf::parse_from_bytes::<RegionLocalState>(value)?;
            if local_state.get_state() == PeerState::Tombstone {
                tombstone_cnt += 1;
                debug!(
                    "region {:?} is tombstone in store {}",
                    local_state.get_region(),
                    self.store_id()
                );
                self.clear_stale_meta(&mut kv_wb, &mut raft_wb, &local_state);
                return Ok(true);
            }
            if local_state.get_state() == PeerState::Applying {
                // in case of restart happen when we just write region state to Applying,
                // but not write raft_local_state to raft rocksdb in time.
                peer_storage::recover_from_applying_state(&self.engines, &raft_wb, region_id)?;
            }

            local_states.push(local_state);
            Ok(true)
        })?;

        if !kv_wb.is_empty() {
            self.engines.kv.write(kv_wb).unwrap();
            self.engines.kv.sync_wal().unwrap();
        }
        if !raft_wb.is_empty() {
            self.engines.raft.write(raft_wb).unwrap();
            self.engines.raft.sync_wal().unwrap();
        }

        let mut applying_cnt = 0;
        let merging_cnt = 0;
        self.peer_agents = Vec::with_capacity(local_states.len());
        let mut meta = self.store_meta.lock().unwrap();
        for local_state in local_states {
            let region = local_state.get_region();
            let mut peer = PeerAgent::create(self, region)?;
            meta.region_ranges.insert(enc_end_key(region), region.get_id());
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            meta.regions.insert(region.get_id(), region.clone());
            match local_state.get_state() {
                PeerState::Applying => {
                    peer.resume_applying_snapshot();
                    applying_cnt += 1;
                },
                PeerState::Merging => {
                    peer.resume_merging(local_state.get_merge_state().to_owned());
                    merging_cnt += 1;
                },
                PeerState::Normal => {}
            }
            self.peer_agents.push(peer);
        }

        info!(
            "{} starts with {} regions, including {} tombstones, {} applying \
             regions and {} merging regions, takes {:?}",
            self.tag,
            total_cnt,
            tombstone_cnt,
            applying_cnt,
            merging_cnt,
            t.elapsed()
        );

        self.clear_stale_data(&mut meta)?;

        Ok(())
    }
}

impl<T> Store<T> {
    fn clear_stale_meta(
        &mut self,
        kv_wb: &mut WriteBatch,
        raft_wb: &mut WriteBatch,
        origin_state: &RegionLocalState,
    ) {
        let region = origin_state.get_region();
        let raft_key = keys::raft_state_key(region.get_id());
        let raft_state = match self.engines.raft.get_msg(&raft_key).unwrap() {
            // it has been cleaned up.
            None => return,
            Some(value) => value,
        };

        peer_storage::clear_meta(&self.engines, kv_wb, raft_wb, region.get_id(), &raft_state)
            .unwrap();
        let key = keys::region_state_key(region.get_id());
        let handle = rocksdb::get_cf_handle(&self.engines.kv, CF_RAFT).unwrap();
        kv_wb.put_msg_cf(handle, &key, origin_state).unwrap();
    }

    /// `clear_stale_data` clean up all possible garbage data.
    fn clear_stale_data(&mut self, meta: &mut StoreMeta) -> Result<()> {
        let t = Instant::now();

        let mut ranges = Vec::new();
        let mut last_start_key = keys::data_key(b"");
        for region_id in meta.region_ranges.values() {
            let region = &meta.regions[region_id];
            let start_key = keys::enc_start_key(region);
            ranges.push((last_start_key, start_key));
            last_start_key = keys::enc_end_key(region);
        }
        ranges.push((last_start_key, keys::DATA_MAX_KEY.to_vec()));

        rocksdb::roughly_cleanup_ranges(&self.engines.kv, &ranges)?;

        info!(
            "{} cleans up {} ranges garbage data, takes {:?}",
            self.tag,
            ranges.len(),
            t.elapsed()
        );

        Ok(())
    }

    pub fn kv_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.kv)
    }

    pub fn raft_engine(&self) -> Arc<DB> {
        Arc::clone(&self.engines.raft)
    }
}

impl<T: Transport> Store<T> {
    pub fn run(self, pd_client: Arc<impl PdClient>) -> Result<()> {
        self.snap_mgr.init()?;

        self.schedule_compact_check_tick();
        self.schedule_pd_store_heartbeat_tick();
        self.schedule_snap_mgr_gc_tick();
        self.schedule_compact_lock_cf_tick();
        self.schedule_cleanup_import_sst_tick();

        let split_check_runner = SplitCheckRunner::new(
            Arc::clone(&self.engines.kv),
            self.sendch.clone(),
            Arc::clone(&self.coprocessor_host),
        );
        box_try!(self.split_check_worker.start(split_check_runner));

        let region_runner = RegionRunner::new(
            self.engines.clone(),
            self.snap_mgr.clone(),
            self.cfg.snap_apply_batch_size.0 as usize,
            self.cfg.use_delete_range,
            self.cfg.clean_stale_peer_delay.0,
        );
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(STALE_PEER_CHECK_INTERVAL), ());
        box_try!(self.region_worker.start_with_timer(region_runner, timer));

        let raftlog_gc_runner = RaftlogGcRunner::new(None);
        box_try!(self.raftlog_gc_worker.start(raftlog_gc_runner));

        let compact_runner = CompactRunner::new(Arc::clone(&self.engines.kv));
        box_try!(self.compact_worker.start(compact_runner));

        let pd_runner = PdRunner::new(
            self.store_id(),
            Arc::clone(&pd_client),
            self.sendch.clone(),
            Arc::clone(&self.engines.kv),
        );
        box_try!(self.pd_worker.start(pd_runner));

        let consistency_check_runner = ConsistencyCheckRunner::new(self.sendch.clone());
        box_try!(
            self.consistency_check_worker
                .start(consistency_check_runner)
        );

        let cleanup_sst_runner = CleanupSSTRunner::new(
            self.store_id(),
            self.sendch.clone(),
            Arc::clone(&self.importer),
            pd_client,
        );
        box_try!(self.cleanup_sst_worker.start(cleanup_sst_runner));

        let apply_runner = ApplyRunner::new(&self, self.sendch.clone(), self.cfg.sync_log, self.cfg.use_delete_range);
        box_try!(self.apply_worker.start(apply_runner));

        let reader = LocalReader::new(&self);
        let timer = LocalReader::new_timer();
        box_try!(self.local_reader.start_with_timer(reader, timer));

        if let Err(e) = util_sys::thread::set_priority(util_sys::HIGH_PRI) {
            warn!("set thread priority for raftstore failed, error: {:?}", e);
        }

        let peer_agents = mem::replace(&mut self.peer_agents, Vec::default());
        for agent in peer_agents {
            self.poller.spawn(agent).forget();
        }
        self.poller().spawn(self).forget();

        Ok(())
    }

    fn stop(&mut self) {
        info!("{} start to stop raftstore.", self.tag);

        // Applying snapshot may take an unexpected long time.
        for peer in self.region_peers.values_mut() {
            peer.stop();
        }

        // Wait all workers finish.
        let mut handles: Vec<Option<thread::JoinHandle<()>>> = vec![];
        handles.push(self.split_check_worker.stop());
        handles.push(self.region_worker.stop());
        handles.push(self.raftlog_gc_worker.stop());
        handles.push(self.compact_worker.stop());
        handles.push(self.pd_worker.stop());
        handles.push(self.consistency_check_worker.stop());
        handles.push(self.cleanup_sst_worker.stop());
        handles.push(self.apply_worker.stop());
        handles.push(self.local_reader.stop());

        for h in handles {
            if let Some(h) = h {
                h.join().unwrap();
            }
        }

        self.coprocessor_host.shutdown();

        info!("{} stop raftstore finished.", self.tag);
    }

    /// If target peer doesn't exist, create it.
    ///
    /// return false to indicate that target peer is in invalid state or
    /// doesn't exist and can't be created.
    fn maybe_create_peer(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let target = msg.get_to_peer();
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut has_peer = false;
        let meta = self.store_meta.lock().unwrap();
        if meta.regions.contains_key(&region_id) {
            return Ok(true);
        }

        let message = msg.get_message();
        let msg_type = message.get_msg_type();
        if msg_type != MessageType::MsgRequestVote
            && msg_type != MessageType::MsgRequestPreVote
            && (msg_type != MessageType::MsgHeartbeat || message.get_commit() != INVALID_INDEX)
        {
            debug!(
                "target peer {:?} doesn't exist, stale message {:?}.",
                target, msg_type
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return Ok(false);
        }

        let start_key = data_key(msg.get_start_key());
        if let Some((_, &exist_region_id)) = meta
            .region_ranges
            .range((Excluded(start_key), Unbounded::<Vec<u8>>))
            .next()
        {
            let exist_region = &meta.regions[&exist_region_id];
            if enc_start_key(exist_region) < data_end_key(msg.get_end_key()) {
                debug!("msg {:?} is overlapped with region {:?}", msg, exist_region);
                if util::is_first_vote_msg(msg) {
                    meta.pending_votes.push(msg.to_owned());
                }
                self.raft_metrics.message_dropped.region_overlap += 1;
                meta.pending_cross_snap
                    .insert(region_id, msg.get_region_epoch().to_owned());
                return Ok(false);
            }
        }

        // New created peers should know it's learner or not.
        let peer = PeerAgent::replicate(self, region_id, target.clone())?;
        // following snapshot may overlap, should insert into region_ranges after
        // snapshot is applied.
        // TODO: make sure a wrong region won't cause problem.
        meta.regions.insert(region_id, peer.region().to_owned());
        self.poller.spawn(peer).forget();

        Ok(true)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        // TODO: Redirect first.

        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            return Ok(());
        }

        if msg.has_merge_target() {
            return Ok(());
        }

        if self.check_msg(&msg)? {
            return Ok(());
        }

        if !self.maybe_create_peer(&msg)? {
            return Ok(());
        }

        // TODO: Redirect again.
        Ok(())
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!(
            "[region {}] handle raft message {:?}, from {} to {}",
            region_id,
            msg.get_message().get_msg_type(),
            from.get_id(),
            to.get_id()
        );

        if to.get_store_id() != self.store_id() {
            warn!(
                "[region {}] store not match, to store id {}, mine {}, ignore it",
                region_id,
                to.get_store_id(),
                self.store_id()
            );
            self.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!(
                "[region {}] missing epoch in raft message, ignore it",
                region_id
            );
            self.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    fn check_msg(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let msg_type = msg.get_message().get_msg_type();
        let is_vote_msg =
            msg_type == MessageType::MsgRequestVote || msg_type == MessageType::MsgRequestPreVote;
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.

        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = self
            .engines
            .kv
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            if local_state.get_state() != PeerState::Tombstone {
                // Maybe split, but not registered yet.
                self.raft_metrics.message_dropped.region_nonexistent += 1;
                if util::is_first_vote_msg(msg) {
                    info!(
                        "[region {}] doesn't exist yet, wait for it to be split",
                        region_id
                    );
                    let mut meta = self.store_meta.lock().unwrap();
                    return if meta.regions.contains_key(&region_id) {
                        // Retry.
                        Ok(false)
                    } else {
                        meta.pending_votes.push(msg.to_owned());
                        Ok(true)
                    };
                }
                return Err(box_err!(
                    "[region {}] region not exist but not tombstone: {:?}",
                    region_id,
                    local_state
                ));
            }
            debug!("[region {}] tombstone state: {:?}", region_id, local_state);
            let region = local_state.get_region();
            let region_epoch = region.get_region_epoch();
            if local_state.has_merge_state() {
                info!(
                    "[region {}] merged peer [epoch: {:?}] receive a stale message {:?}",
                    region_id, region_epoch, msg_type
                );

                let merge_target = if let Some(peer) = util::find_peer(region, from_store_id) {
                    assert_eq!(peer, msg.get_from_peer());
                    // Let stale peer decides whether it should wait for merging or just remove
                    // itself.
                    Some(local_state.get_merge_state().get_target().to_owned())
                } else {
                    // If a peer is isolated before prepare_merge and conf remove, it should just
                    // remove itself.
                    None
                };
                self.handle_stale_msg(msg, region_epoch, true, merge_target);
                return Ok(true);
            }
            // The region in this peer is already destroyed
            if util::is_epoch_stale(from_epoch, region_epoch) {
                info!(
                    "[region {}] tombstone peer [epoch: {:?}] \
                     receive a stale message {:?}",
                    region_id, region_epoch, msg_type,
                );

                let not_exist = util::find_peer(region, from_store_id).is_none();
                self.handle_stale_msg(
                    msg,
                    region_epoch,
                    is_vote_msg && not_exist,
                    None,
                );

                return Ok(true);
            }

            if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                self.raft_metrics.message_dropped.region_tombstone_peer += 1;
                return Err(box_err!(
                    "tombstone peer [epoch: {:?}] receive an invalid \
                     message {:?}, ignore it",
                    region_epoch,
                    msg_type
                ));
            }
        }

        Ok(false)
    }

    fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        cur_epoch: &metapb::RegionEpoch,
        need_gc: bool,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();

        if !need_gc {
            info!(
                "[region {}] raft message {:?} is stale, current {:?}, ignore it",
                region_id, msg_type, cur_epoch
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "[region {}] raft message {:?} is stale, current {:?}, tell to gc",
            region_id, msg_type, cur_epoch
        );

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        if let Some(r) = target_region {
            gc_msg.set_merge_target(r);
        } else {
            gc_msg.set_is_tombstone(true);
        }
        if let Err(e) = self.trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    fn on_compaction_finished(&mut self, event: CompactedEvent) {
        // If size declining is trivial, skip.
        let total_bytes_declined = if event.total_input_bytes > event.total_output_bytes {
            event.total_input_bytes - event.total_output_bytes
        } else {
            0
        };
        if total_bytes_declined < self.cfg.region_split_check_diff.0
            || total_bytes_declined * 10 < event.total_input_bytes
        {
            return;
        }

        let output_level_str = event.output_level.to_string();
        COMPACTION_DECLINED_BYTES
            .with_label_values(&[&output_level_str])
            .observe(total_bytes_declined as f64);

        // self.cfg.region_split_check_diff.0 / 16 is an experienced value.
        let mut region_declined_bytes = calc_region_declined_bytes(
            event,
            &self.region_ranges,
            self.cfg.region_split_check_diff.0 / 16,
        );

        COMPACTION_RELATED_REGION_COUNT
            .with_label_values(&[&output_level_str])
            .observe(region_declined_bytes.len() as f64);

        for (region_id, declined_bytes) in region_declined_bytes.drain(..) {
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                peer.compaction_declined_bytes += declined_bytes;
                if peer.compaction_declined_bytes >= self.cfg.region_split_check_diff.0 {
                    UPDATE_REGION_SIZE_BY_COMPACTION_COUNTER.inc();
                }
            }
        }
    }

    fn schedule_compact_check_tick(&self) {
        self.schedule_tick(
            self.cfg.region_compact_check_interval.0,
            StoreTick::CompactCheck,
        )
    }

    fn on_compact_check_tick(&mut self) {
        if self.compact_worker.is_busy() {
            debug!("compact worker is busy, check space redundancy next time");
        } else if self.region_ranges.is_empty() {
            debug!("there is no range need to check");
        } else if rocksdb::auto_compactions_is_disabled(&self.engines.kv) {
            debug!("skip compact check when disabled auto compactions.");
        } else {
            // Start from last checked key.
            let mut ranges_need_check =
                Vec::with_capacity(self.cfg.region_compact_check_step as usize + 1);
            ranges_need_check.push(self.last_compact_checked_key.clone());

            // Collect continuous ranges.
            let left_ranges = self.region_ranges.range((
                Excluded(self.last_compact_checked_key.clone()),
                Unbounded::<Key>,
            ));
            ranges_need_check.extend(
                left_ranges
                    .take(self.cfg.region_compact_check_step as usize)
                    .map(|(k, _)| k.to_owned()),
            );

            // Update last_compact_checked_key.
            let largest_key = self.region_ranges.keys().last().unwrap().to_vec();
            let last_key = ranges_need_check.last().unwrap().clone();
            if last_key == largest_key {
                // Range [largest key, DATA_MAX_KEY) also need to check.
                if last_key != keys::DATA_MAX_KEY.to_vec() {
                    ranges_need_check.push(keys::DATA_MAX_KEY.to_vec());
                }
                // Next task will start from the very beginning.
                self.last_compact_checked_key = keys::DATA_MIN_KEY.to_vec();
            } else {
                self.last_compact_checked_key = last_key;
            }

            // Schedule the task.
            let cf_names = vec![CF_DEFAULT.to_owned(), CF_WRITE.to_owned()];
            if let Err(e) = self.compact_worker.schedule(CompactTask::CheckAndCompact {
                cf_names,
                ranges: ranges_need_check,
                tombstones_num_threshold: self.cfg.region_compact_min_tombstones,
                tombstones_percent_threshold: self.cfg.region_compact_tombstones_percent,
            }) {
                error!("{} failed to schedule space check task: {}", self.tag, e);
            }
        }

        self.schedule_compact_check_tick();
    }

    fn store_heartbeat_pd(&mut self) {
        let mut stats = StoreStats::new();

        let used_size = self.snap_mgr.get_total_snap_size();
        stats.set_used_size(used_size);
        stats.set_store_id(self.store_id());
        stats.set_region_count(self.region_peers.len() as u32);

        let snap_stats = self.snap_mgr.stats();
        stats.set_sending_snap_count(snap_stats.sending_count as u32);
        stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["sending"])
            .set(snap_stats.sending_count as i64);
        STORE_SNAPSHOT_TRAFFIC_GAUGE_VEC
            .with_label_values(&["receiving"])
            .set(snap_stats.receiving_count as i64);

        // TODO: count applying region count.

        stats.set_start_time(self.start_time.sec as u32);

        // report store write flow to pd
        stats.set_bytes_written(
            self.store_stat.engine_total_bytes_written
                - self.store_stat.engine_last_total_bytes_written,
        );
        stats.set_keys_written(
            self.store_stat.engine_total_keys_written
                - self.store_stat.engine_last_total_keys_written,
        );
        self.store_stat.engine_last_total_bytes_written =
            self.store_stat.engine_total_bytes_written;
        self.store_stat.engine_last_total_keys_written = self.store_stat.engine_total_keys_written;

        stats.set_is_busy(self.is_busy);
        self.is_busy = false;

        let store_info = StoreInfo {
            engine: Arc::clone(&self.engines.kv),
            capacity: self.cfg.capacity.0,
        };

        let task = PdTask::StoreHeartbeat { stats, store_info };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", self.tag, e);
        }
    }

    fn on_pd_store_heartbeat_tick(&mut self) {
        self.store_heartbeat_pd();
        self.schedule_pd_store_heartbeat_tick();
    }

    fn handle_snap_mgr_gc(&mut self) -> Result<()> {
        // TODO: send a validate message to peer instead.
        unimplemented!()
    }

    fn on_snap_mgr_gc(&mut self) {
        if let Err(e) = self.handle_snap_mgr_gc() {
            error!("{} failed to gc snap manager: {:?}", self.tag, e);
        }
        self.schedule_snap_mgr_gc_tick();
    }

    fn on_compact_lock_cf(&mut self) {
        unimplemented!()
    }

    #[inline]
    fn schedule_pd_store_heartbeat_tick(&self) {
        self.schedule_tick(
            self.cfg.pd_store_heartbeat_tick_interval.0,
            StoreTick::PdStoreHeartbeat,
        );
    }

    #[inline]
    fn schedule_snap_mgr_gc_tick(&self) {
        self.schedule_tick(
            self.cfg.snap_mgr_gc_tick_interval.0, StoreTick::SnapGc,
        )
    }

    #[inline]
    fn schedule_compact_lock_cf_tick(&self) {
        self.schedule_tick(self.cfg.lock_cf_compact_interval.0, StoreTick::CompactLockCf)
    }
}

impl<T: Transport> Store<T> {
    fn on_validate_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        // A stale peer can still ingest a stale SST before it is
        // destroyed. We need to make sure that no stale peer exists.
        let mut delete_ssts = Vec::new();
        {
            let meta = self.store_meta.lock().unwrap();
            for sst in ssts {
                if !meta.regions.contains_key(&sst.get_region_id()) {
                    delete_ssts.push(sst);
                }
            }
        }
        if delete_ssts.is_empty() {
            return;
        }

        let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
        if let Err(e) = self.cleanup_sst_worker.schedule(task) {
            error!("schedule to delete ssts: {:?}", e);
        }
    }

    fn on_cleanup_import_sst(&mut self) -> Result<()> {
        let mut delete_ssts = Vec::new();
        let mut validate_ssts = Vec::new();

        let ssts = box_try!(self.importer.list_ssts());
        if ssts.is_empty() {
            return Ok(());
        }
        {
            let meta = self.store_meta.lock().unwrap();
            for sst in ssts {
                if let Some(r) = meta.regions.get(&sst.get_region_id()) {
                    let region_epoch = r.get_region_epoch();
                    if util::is_epoch_stale(sst.get_region_epoch(), region_epoch) {
                        // If the SST epoch is stale, it will not be ingested anymore.
                        delete_ssts.push(sst);
                    }
                } else {
                    // If the peer doesn't exist, we need to validate the SST through PD.
                    validate_ssts.push(sst);
                }
            }
        }

        if !delete_ssts.is_empty() {
            let task = CleanupSSTTask::DeleteSST { ssts: delete_ssts };
            if let Err(e) = self.cleanup_sst_worker.schedule(task) {
                error!("schedule to delete ssts: {:?}", e);
            }
        }

        if !validate_ssts.is_empty() {
            let task = CleanupSSTTask::ValidateSST {
                ssts: validate_ssts,
            };
            if let Err(e) = self.cleanup_sst_worker.schedule(task) {
                error!("schedule to validate ssts: {:?}", e);
            }
        }

        Ok(())
    }

    fn on_cleanup_import_sst_tick(&mut self) {
        if let Err(e) = self.on_cleanup_import_sst() {
            error!("{} failed to cleanup import sst: {:?}", self.tag, e);
        }
        self.schedule_cleanup_import_sst_tick();
    }

    #[inline]
    fn schedule_cleanup_import_sst_tick(&self) {
        self.schedule_tick(
            self.cfg.cleanup_import_sst_interval.0,
            StoreTick::CleanupImportSST,
        )
    }

    #[inline]
    fn schedule_tick(&self, dur: Duration, tick: StoreTick) {
        if dur != Duration::new(0, 0) {
            let mut tx = self.sender.clone();
            let f = self.timer.delay(Instant::now() + dur).map(move |_| {
                let _ = tx.force_send(StoreMsg::Tick(tick));
            });
            self.poller.spawn(f).forget()
        }
    }

    fn on_store_msg(&mut self, msg: StoreMsg) {
        match msg {
            StoreMsg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.tag, e);
            },
            StoreMsg::Tick(tick) => self.on_store_tick(tick),
            StoreMsg::SnapshotStats => self.store_heartbeat_pd(),
            StoreMsg::CompactedEvent(event) => self.on_compaction_finished(event),
            StoreMsg::ValidateSSTResult { invalid_ssts } => self.on_validate_sst_result(invalid_ssts),
            StoreMsg::Quit => {
                info!("{} receive quit message", self.tag);
                self.stop();
            }
            StoreMsg::SeekRegion {
                from_key,
                filter,
                limit,
                callback,
            } => unimplemented!(),
        }
    }

    fn on_store_tick(&mut self, tick: StoreTick) {
        let t = SlowTimer::new();
        match tick {
            StoreTick::CompactCheck => self.on_compact_check_tick(),
            StoreTick::PdStoreHeartbeat => self.on_pd_store_heartbeat_tick(),
            StoreTick::SnapGc => self.on_snap_mgr_gc(),
            StoreTick::CompactLockCf => self.on_compact_lock_cf(),
            StoreTick::CleanupImportSST => self.on_cleanup_import_sst_tick(),
        }
        slow_log!(t, "{} handle tick {:?}", self.tag, tick);
    }
}

fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

fn new_verify_hash_request(
    region_id: u64,
    peer: metapb::Peer,
    state: &ConsistencyState,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::VerifyHash);
    admin.mut_verify_hash().set_index(state.index);
    admin.mut_verify_hash().set_hash(state.hash.clone());
    request.set_admin_request(admin);
    request
}

fn new_compute_hash_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::ComputeHash);
    request.set_admin_request(admin);
    request
}

fn new_compact_log_request(
    region_id: u64,
    peer: metapb::Peer,
    compact_index: u64,
    compact_term: u64,
) -> RaftCmdRequest {
    let mut request = new_admin_request(region_id, peer);

    let mut admin = AdminRequest::new();
    admin.set_cmd_type(AdminCmdType::CompactLog);
    admin.mut_compact_log().set_compact_index(compact_index);
    admin.mut_compact_log().set_compact_term(compact_term);
    request.set_admin_request(admin);
    request
}

impl<T: Transport> Future for Store<T> {
    type Item = ();
    type Error = ();
    
    fn poll(&mut self) -> Poll<(), ()> {
        let mut msgs;
        match self.receiver.poll() {
            Ok(Async::Ready(Some(m))) => {
                msgs = Vec::with_capacity(self.cfg.messages_per_tick);
                msgs.push(m);
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            _ => unreachable!()
        }
        loop {
            while msgs.len() < self.cfg.messages_per_tick {
                match self.receiver.poll() {
                    Ok(Async::Ready(Some(m))) => msgs.push(m),
                    Ok(Async::NotReady) => break,
                    _ => unreachable!(),
                }
            }
            let keep_going = msgs.len() == self.cfg.messages_per_tick;
            for m in msgs.drain(..) {
                self.on_store_msg(m);
            }
            if !self.stopped {
                if keep_going {
                    continue;
                }
                return Ok(Async::NotReady);
            }
            self.stop();
            return Ok(Async::Ready(()));
        }
    }
}

fn size_change_filter(info: &CompactionJobInfo) -> bool {
    // When calculating region size, we only consider write and default
    // column families.
    let cf = info.cf_name();
    if cf != CF_WRITE && cf != CF_DEFAULT {
        return false;
    }
    // Compactions in level 0 and level 1 are very frequently.
    if info.output_level() < 2 {
        return false;
    }

    true
}

pub fn new_compaction_listener(ch: SendCh<Msg>) -> CompactionListener {
    let compacted_handler = box move |compacted_event: CompactedEvent| {
        if let Err(e) = ch.try_send(Msg::CompactedEvent(compacted_event)) {
            error!(
                "Send compaction finished event to raftstore failed: {:?}",
                e
            );
        }
    };
    CompactionListener::new(compacted_handler, Some(size_change_filter))
}

fn calc_region_declined_bytes(
    event: CompactedEvent,
    region_ranges: &BTreeMap<Key, u64>,
    bytes_threshold: u64,
) -> Vec<(u64, u64)> {
    // Calculate influenced regions.
    let mut influenced_regions = vec![];
    for (end_key, region_id) in
        region_ranges.range((Excluded(event.start_key), Included(event.end_key.clone())))
    {
        influenced_regions.push((region_id, end_key.clone()));
    }
    if let Some((end_key, region_id)) = region_ranges
        .range((Included(event.end_key), Unbounded))
        .next()
    {
        influenced_regions.push((region_id, end_key.clone()));
    }

    // Calculate declined bytes for each region.
    // `end_key` in influenced_regions are in incremental order.
    let mut region_declined_bytes = vec![];
    let mut last_end_key: Vec<u8> = vec![];
    for (region_id, end_key) in influenced_regions {
        let mut old_size = 0;
        for prop in &event.input_props {
            old_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        let mut new_size = 0;
        for prop in &event.output_props {
            new_size += prop.get_approximate_size_in_range(&last_end_key, &end_key);
        }
        last_end_key = end_key;

        // Filter some trivial declines for better performance.
        if old_size > new_size && old_size - new_size > bytes_threshold {
            region_declined_bytes.push((*region_id, old_size - new_size));
        }
    }

    region_declined_bytes
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use util::rocksdb::properties::{IndexHandle, IndexHandles, SizeProperties};
    use util::rocksdb::CompactedEvent;

    use super::*;

    #[test]
    fn test_calc_region_declined_bytes() {
        let index_handle1 = IndexHandle {
            size: 4 * 1024,
            offset: 4 * 1024,
        };
        let index_handle2 = IndexHandle {
            size: 4 * 1024,
            offset: 8 * 1024,
        };
        let index_handle3 = IndexHandle {
            size: 4 * 1024,
            offset: 12 * 1024,
        };
        let mut index_handles = IndexHandles::new();
        index_handles.add(b"a".to_vec(), index_handle1);
        index_handles.add(b"b".to_vec(), index_handle2);
        index_handles.add(b"c".to_vec(), index_handle3);
        let size_prop = SizeProperties {
            total_size: 12 * 1024,
            index_handles,
        };
        let event = CompactedEvent {
            cf: "default".to_owned(),
            output_level: 3,
            total_input_bytes: 12 * 1024,
            total_output_bytes: 0,
            start_key: size_prop.smallest_key().unwrap(),
            end_key: size_prop.largest_key().unwrap(),
            input_props: vec![size_prop.into()],
            output_props: vec![],
        };

        let mut region_ranges = BTreeMap::new();
        region_ranges.insert(b"a".to_vec(), 1);
        region_ranges.insert(b"b".to_vec(), 2);
        region_ranges.insert(b"c".to_vec(), 3);

        let declined_bytes = calc_region_declined_bytes(event, &region_ranges, 1024);
        let expected_declined_bytes = vec![(2, 8192), (3, 4096)];
        assert_eq!(declined_bytes, expected_declined_bytes);
    }
}

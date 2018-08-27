// Copyright 2018 PingCAP, Inc.
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

use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuPool;
use kvproto::import_sstpb::SSTMeta;
use kvproto::metapb::{self, Region, RegionEpoch};
use kvproto::pdpb::CheckPolicy;
use kvproto::raft_cmdpb::{
    AdminCmdType, AdminRequest, RaftCmdRequest, RaftCmdResponse, StatusCmdType, StatusResponse,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftMessage, RaftSnapshotData, RaftTruncatedState, RegionLocalState,
};
use pd::PdTask;
use protobuf::{Message, RepeatedField};
use raft::eraftpb::{ConfChangeType, MessageType};
use raft::{self, SnapshotStatus, INVALID_INDEX, NO_LIMIT};
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::cmd_resp::{bind_term, new_error};
use raftstore::store::engine::{Peekable, Snapshot as EngineSnapshot};
use raftstore::store::keys::{self, enc_end_key, enc_start_key};
use raftstore::store::local_metrics::RaftMetrics;
use raftstore::store::metrics::*;
use raftstore::store::msg::{Callback, PeerMsg, PeerTick, SignificantMsg};
use raftstore::store::peer_storage::ApplySnapResult;
use raftstore::store::store::DestroyPeerJob;
use raftstore::store::transport::Transport;
use raftstore::store::worker::apply::{ApplyRes, ChangePeer, ExecResult};
use raftstore::store::worker::{
    ApplyTask, CleanupSSTTask, ConsistencyCheckTask, RaftlogGcTask, ReadTask, RegionTask,
    SplitCheckTask,
};
use raftstore::store::{
    peer::{ConsistencyState, StaleState, ReadyContext},
    snap::SnapshotDeleter,
    util as raftstore_util, Config, Engines, Peer, SnapKey, SnapManager,
};
use raftstore::{Error, Result};
use rocksdb::{DB, WriteOptions};
use std::cmp;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use storage::CF_RAFT;
use tokio_timer::timer::Handle;
use util::collections::HashMap;
use util::mpsc::{loose_bounded, LooseBoundedSender, Receiver};
use util::timer::{GLOBAL_TIMER_HANDLE};
use util::time::{SlowTimer, duration_to_sec};
use util::worker::{FutureScheduler, Scheduler, Stopped};
use util::{escape, RingQueue};
use util::future::CountDownLatch;

pub struct StoreMeta {
    // region end key -> region id
    pub region_ranges: BTreeMap<Vec<u8>, u64>,
    // region_id -> region
    pub regions: HashMap<u64, Region>,
    // A marker used to indicate if the peer of a region is going to apply a snapshot
    // with different range.
    // It assumes that when a peer is going to accept snapshot, it can never
    // captch up by normal log replication.
    pub pending_cross_snap: HashMap<u64, RegionEpoch>,
    pub pending_votes: RingQueue<RaftMessage>,
    // the regions with pending snapshots.
    pub pending_snapshot_regions: Vec<metapb::Region>,
}

impl StoreMeta {
    pub fn new(vote_capacity: usize) -> StoreMeta {
        StoreMeta {
            region_ranges: BTreeMap::default(),
            regions: HashMap::default(),
            pending_cross_snap: HashMap::default(),
            pending_votes: RingQueue::with_capacity(vote_capacity),
            pending_snapshot_regions: Vec::default(),
        }
    }

    #[inline]
    pub fn set_region(&mut self, region: Region, peer: &mut Peer) {
        let prev = self.regions.insert(region.get_id(), region.clone());
        if prev.map_or(true, |r| r.get_id() != region.get_id()) {
            // TODO: may not be a good idea to panic when holding a lock.
            panic!("{} region corrupted", peer.tag);
        }
        peer.set_region(region);
    }
}

pub struct PeerAgent<T: 'static> {
    peer: Peer,
    pd_scheduler: FutureScheduler<PdTask>,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    consistency_check_scheduler: Scheduler<ConsistencyCheckTask>,
    split_check_scheduler: Scheduler<SplitCheckTask>,
    cleanup_sst_scheduler: Scheduler<CleanupSSTTask>,
    store_meta: Arc<Mutex<StoreMeta>>,
    poller: CpuPool,
    raft_metrics: RaftMetrics,
    snap_mgr: SnapManager,
    timer: Handle,
    sender: LooseBoundedSender<PeerMsg>,
    receiver: Receiver<PeerMsg>,
    trans: T,
    count_down_latch: CountDownLatch,
    queued_snapshot: bool,
    has_ready: bool,
    stopped: bool,
}

pub trait ConfigProvider<Transport> {
    fn store_id(&self) -> u64;
    fn config(&self) -> Arc<Config>;
    fn snap_scheduler(&self) -> Scheduler<RegionTask>;
    fn engines(&self) -> Engines;
    fn coprocessor_host(&self) -> Arc<CoprocessorHost>;
    fn apply_scheduler(&self) -> Scheduler<ApplyTask>;
    fn read_scheduler(&self) -> Scheduler<ReadTask>;
    fn pd_scheduler(&self) -> FutureScheduler<PdTask>;
    fn raft_log_gc_scheduler(&self) -> Scheduler<RaftlogGcTask>;
    fn store_meta(&self) -> Arc<Mutex<StoreMeta>>;
    fn consistency_check_scheduler(&self) -> Scheduler<ConsistencyCheckTask>;
    fn snap_manager(&self) -> SnapManager;
    fn split_check_scheduler(&self) -> Scheduler<SplitCheckTask>;
    fn cleanup_sst_scheduler(&self) -> Scheduler<CleanupSSTTask>;
    fn transport(&self) -> Transport;
    fn poller(&self) -> CpuPool;
    fn count_down_latch(&self) -> CountDownLatch;
}

impl<T: Transport> ConfigProvider<T> for PeerAgent<T> {
    #[inline]
    fn store_id(&self) -> u64 {
        self.peer.peer.get_store_id()
    }

    #[inline]
    fn config(&self) -> Arc<Config> {
        Arc::clone(&self.peer.cfg)
    }

    #[inline]
    fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.peer.get_store().region_sched()
    }

    #[inline]
    fn apply_scheduler(&self) -> Scheduler<ApplyTask> {
        self.peer.apply_scheduler.clone()
    }

    #[inline]
    fn read_scheduler(&self) -> Scheduler<ReadTask> {
        self.peer.read_scheduler.clone()
    }

    #[inline]
    fn engines(&self) -> Engines {
        self.peer.engines.clone()
    }

    #[inline]
    fn coprocessor_host(&self) -> Arc<CoprocessorHost> {
        Arc::clone(&self.peer.coprocessor_host)
    }

    #[inline]
    fn pd_scheduler(&self) -> FutureScheduler<PdTask> {
        self.pd_scheduler.clone()
    }

    #[inline]
    fn raft_log_gc_scheduler(&self) -> Scheduler<RaftlogGcTask> {
        self.raftlog_gc_scheduler.clone()
    }

    #[inline]
    fn consistency_check_scheduler(&self) -> Scheduler<ConsistencyCheckTask> {
        self.consistency_check_scheduler.clone()
    }

    #[inline]
    fn store_meta(&self) -> Arc<Mutex<StoreMeta>> {
        self.store_meta.clone()
    }

    #[inline]
    fn snap_manager(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    #[inline]
    fn split_check_scheduler(&self) -> Scheduler<SplitCheckTask> {
        self.split_check_scheduler.clone()
    }

    #[inline]
    fn cleanup_sst_scheduler(&self) -> Scheduler<CleanupSSTTask> {
        self.cleanup_sst_scheduler.clone()
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

impl<T: Transport> PeerAgent<T> {
    pub fn create<P: ConfigProvider<T>>(p: &P, region: &Region) -> Result<PeerAgent<T>> {
        let peer = Peer::create(p, region)?;
        Ok(PeerAgent::new(p, peer))
    }

    pub fn replicate<P: ConfigProvider<T>>(
        p: &P,
        region_id: u64,
        peer: metapb::Peer,
    ) -> Result<PeerAgent<T>> {
        let peer = Peer::replicate(p, region_id, peer)?;
        Ok(PeerAgent::new(p, peer))
    }

    fn new<P: ConfigProvider<T>>(p: &P, peer: Peer) -> PeerAgent<T> {
        let (tx, rx) = loose_bounded(peer.cfg.notify_capacity);
        PeerAgent {
            peer,
            pd_scheduler: p.pd_scheduler(),
            raftlog_gc_scheduler: p.raft_log_gc_scheduler(),
            consistency_check_scheduler: p.consistency_check_scheduler(),
            split_check_scheduler: p.split_check_scheduler(),
            cleanup_sst_scheduler: p.cleanup_sst_scheduler(),
            store_meta: p.store_meta(),
            raft_metrics: RaftMetrics::default(),
            poller: p.poller(),
            snap_mgr: p.snap_manager(),
            timer: GLOBAL_TIMER_HANDLE.clone(),
            sender: tx,
            receiver: rx,
            trans: p.transport(),
            count_down_latch: p.count_down_latch(),
            queued_snapshot: false,
            has_ready: false,
            stopped: false,
        }
    }

    #[inline]
    pub fn region(&self) -> Region {
        self.peer.region()
    }

    pub fn resume_applying_snapshot(&mut self) {
        info!(
            "{} resume applying snapshot [region {:?}]",
            self.peer.tag,
            self.peer.region()
        );
        self.peer.mut_store().schedule_applying_snapshot();
    }

    pub fn resume_merging(&mut self, state: MergeState) {
        info!("{} resume merging [region {:?}, state: {:?}]", self.peer.tag, self.peer.region(), state);
        self.peer.pending_merge_state = Some(state);
        self.on_check_merge();
    }

    pub fn stop(&mut self) {
        self.stopped = true;
        self.peer.stop();
    }

    #[inline]
    fn kv_engine(&self) -> &Arc<DB> {
        &self.peer.engines.kv
    }

    #[inline]
    fn region_id(&self) -> u64 {
        self.peer.region().get_id()
    }

    pub fn destroy_peer(&mut self, keep_data: bool) {
        // Can we destroy it in another thread later?
        info!("{} starts destroy", self.peer.tag);
        self.stopped = true;
        let region_id = self.region_id();
        let store_id = self.store_id();
        // We can't destroy a peer which is applying snapshot.
        assert!(!self.peer.is_applying_snapshot());
        let mut meta = self.store_meta.lock().unwrap();
        if meta.regions.remove(&region_id).is_none() {
            panic!("{} meta occrupted: {:?}", self.peer.tag, meta.region_ranges);
        }
        meta.pending_cross_snap.remove(&region_id);
        let task = PdTask::DestroyPeer { region_id };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd: {}", self.peer.tag, e);
        }
        let is_initialized = self.peer.is_initialized();
        if let Err(e) = self.peer.destroy(keep_data) {
            // If not panic here, the peer will be recreated in the next restart,
            // then it will be gc again. But if some overlap region is created
            // before restarting, the gc action will delete the overlap region's
            // data too.
            panic!(
                "{} destroy peer {:?} in store {} err {:?}",
                self.peer.tag, self.peer.peer, store_id, e
            );
        }

        if is_initialized
            && meta
                .region_ranges
                .remove(&enc_end_key(self.peer.region()))
                .is_none()
        {
            panic!(
                "{} remove peer {:?} in store {} fail",
                self.peer.tag, self.peer.peer, store_id
            );
        }
    }

    fn on_ready_change_peer(&mut self, cp: ChangePeer) {
        let my_peer_id;
        let change_type = cp.conf_change.get_change_type();
        self.peer.raft_group.apply_conf_change(&cp.conf_change);
        if cp.conf_change.get_node_id() == raft::INVALID_ID {
            // Apply failed, skip.
            return;
        }
        {
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(cp.region, &mut self.peer);
        }
        if self.peer.is_leader() {
            // Notify pd immediately.
            info!(
                "{} notify pd with change peer region {:?}",
                self.peer.tag,
                self.peer.region()
            );
            self.peer.heartbeat_pd2(&self.pd_scheduler);
        }

        let peer_id = cp.peer.get_id();
        match change_type {
            ConfChangeType::AddNode | ConfChangeType::AddLearnerNode => {
                let peer = cp.peer.clone();
                if self.peer.peer_id() == peer_id && self.peer.peer.get_is_learner() {
                    self.peer.peer = peer.clone();
                }

                // Add this peer to cache and heartbeats.
                let now = Instant::now();
                self.peer.peer_heartbeats.insert(peer.get_id(), now);
                if self.peer.is_leader() {
                    self.peer
                        .peers_start_pending_time
                        .push((peer.get_id(), now));
                }
            }
            ConfChangeType::RemoveNode => {
                // Remove this peer from cache.
                self.peer.peer_heartbeats.remove(&peer_id);
                if self.peer.is_leader() {
                    self.peer
                        .peers_start_pending_time
                        .retain(|&(p, _)| p != peer_id);
                }
                self.peer.remove_peer_from_cache(peer_id);
            }
        }
        my_peer_id = self.peer.peer_id();

        let peer = cp.peer;

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if my_peer_id == peer.get_id() {
                self.destroy_peer(false)
            } else {
                panic!("{} trying to remove unknown peer {:?}", self.peer.tag, peer);
            }
        }
    }

    fn on_ready_compact_log(&mut self, first_index: u64, state: RaftTruncatedState) {
        let total_cnt = self.peer.last_applying_idx - first_index;
        // the size of current CompactLog command can be ignored.
        let remain_cnt = self.peer.last_applying_idx - state.get_index() - 1;
        self.peer.raft_log_size_hint = self.peer.raft_log_size_hint * remain_cnt / total_cnt;
        let task = RaftlogGcTask {
            raft_engine: Arc::clone(&self.peer.get_store().get_raft_engine()),
            region_id: self.region_id(),
            start_idx: self.peer.last_compacted_idx,
            end_idx: state.get_index() + 1,
        };
        self.peer.last_compacted_idx = task.end_idx;
        self.peer.mut_store().compact_to(task.end_idx);
        if let Err(e) = self.raftlog_gc_scheduler.schedule(task) {
            error!("{} failed to schedule compact task: {}", self.peer.tag, e);
        }
    }

    fn on_ready_split_region(&mut self, derived: Region, regions: Vec<Region>) {
        let mut meta = self.store_meta.lock().unwrap();
        let region_id = derived.get_id();
        meta.set_region(derived, &mut self.peer);
        self.peer.post_split();
        let is_leader = self.peer.is_leader();
        if is_leader {
            self.peer.heartbeat_pd2(&self.pd_scheduler);
            // Notify pd immediately to let it update the region meta.
            info!(
                "{} notify pd with split count {}",
                self.peer.tag,
                regions.len()
            );

            // Now pd only uses ReportBatchSplit for history operation show,
            // so we send it independently here.
            let task = PdTask::ReportBatchSplit {
                regions: regions.clone(),
            };

            if let Err(e) = self.pd_scheduler.schedule(task) {
                error!("{} failed to notify pd: {}", self.peer.tag, e);
            }
        }
        let peer_stat = self.peer.peer_stat.clone();
        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{} meta corrupted.", self.peer.tag);
        }
        let last_region_id = regions.last().unwrap().get_id();
        for new_region in regions {
            let new_region_id = new_region.get_id();
            let not_exist = meta
                .region_ranges
                .insert(enc_end_key(&new_region), new_region_id)
                .is_none();
            assert!(
                not_exist,
                "[region {}] should not exists",
                new_region.get_id()
            );
            if new_region_id == region_id {
                continue;
            }
            // Insert new regions and validation
            info!(
                "[region {}] insert new region {:?}",
                new_region.get_id(),
                new_region
            );

            let mut new_peer = match PeerAgent::create(self, &new_region) {
                Ok(new_peer) => new_peer,
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };
            let peer = new_peer.peer.peer.clone();
            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer.peer_stat = peer_stat.clone();
            let campaigned = new_peer.peer.maybe_campaign2(is_leader);
            new_peer.has_ready = campaigned;

            if is_leader {
                // The new peer is likely to become leader, send a heartbeat immediately to reduce
                // client query miss.
                new_peer.peer.heartbeat_pd2(&self.pd_scheduler);
            }

            new_peer.peer.register_delegates();
            if let Some(r) = meta.regions.insert(new_region.get_id(), new_region) {
                // If the store received a raft msg with the new region raft group
                // before splitting, it will creates a uninitialized peer.
                // We can remove this uninitialized peer directly.
                if !r.get_peers().is_empty() {
                    panic!(
                        "{} duplicated region {:?} for split region {:?}",
                        new_peer.peer.tag,
                        r,
                        new_peer.peer.region()
                    );
                }
            }

            if !campaigned {
                if let Some(msg) = meta
                    .pending_votes
                    .swap_remove_front(|m| m.get_to_peer() == &peer)
                {
                    let _ = new_peer.on_raft_message(msg);
                }
            }
            if new_region_id == last_region_id {
                // To prevent from big region, the right region needs run split
                // check again after split.
                new_peer.peer.size_diff_hint = self.peer.cfg.region_split_check_diff.0;
            }
            self.poller.spawn(new_peer).forget();
        }
    }

    fn on_ready_prepare_merge(&mut self, region: metapb::Region, state: MergeState, merged: bool) {
        {
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(region.clone(), &mut self.peer);
            self.peer.pending_merge_state = Some(state);
        }

        if merged {
            // CommitMerge will try to catch up log for source region. If PrepareMerge is executed
            // in the progress of catching up, there is no need to schedule merge again.
            return;
        }

        self.on_check_merge();
    }

    fn validate_merge_target(&self, tag: &str, target_region: &Region) -> Result<bool> {
        let region_id = target_region.get_id();
        let exist_region = {
            let meta = self.store_meta.lock().unwrap();
            meta.regions.get(&region_id).cloned()
        };

        if let Some(exist_region) = exist_region {
            let exist_epoch = exist_region.get_region_epoch();
            let expect_epoch = target_region.get_region_epoch();
            // exist_epoch > expect_epoch
            if raftstore_util::is_epoch_stale(expect_epoch, exist_epoch) {
                return Err(box_err!(
                    "target region changed {:?} -> {:?}",
                    target_region,
                    exist_region
                ));
            }
            // exist_epoch < expect_epoch
            if raftstore_util::is_epoch_stale(exist_epoch, expect_epoch) {
                info!(
                    "{} target region still not catch up: {:?} vs {:?}, skip.",
                    tag, target_region, exist_region
                );
                return Ok(false);
            }
            return Ok(true);
        }

        let state_key = keys::region_state_key(region_id);
        let state: RegionLocalState = match self.peer.engines.kv.get_msg_cf(CF_RAFT, &state_key) {
            Err(e) => {
                error!(
                    "{} failed to load region state of {}, ignore: {}",
                    tag, region_id, e
                );
                return Ok(false);
            }
            Ok(None) => {
                info!(
                    "{} seems to merge into a new replica of region {}, let's wait.",
                    tag, region_id
                );
                return Ok(false);
            }
            Ok(Some(state)) => state,
        };
        if state.get_state() != PeerState::Tombstone {
            info!("{} wait for region {} split.", tag, region_id);
            return Ok(false);
        }

        let tombstone_region = state.get_region();
        if tombstone_region.get_region_epoch().get_conf_ver()
            < target_region.get_region_epoch().get_conf_ver()
        {
            info!(
                "{} seems to merge into a new replica of region {}, let's wait.",
                tag, region_id
            );
            return Ok(false);
        }

        Err(box_err!("region {} is destroyed", region_id))
    }

    fn schedule_merge(&mut self) -> Result<()> {
        fail_point!("on_schedule_merge", |_| Ok(()));
        let req = {
            let state = self.peer.pending_merge_state.as_ref().unwrap();
            let sibling_region = state.get_target();
            if !self.validate_merge_target(&self.peer.tag, sibling_region)? {
                // Wait till next round.
                return Ok(());
            }

            let min_index = self.peer.get_min_progress() + 1;
            let low = cmp::max(min_index, state.get_min_index());
            // TODO: move this into raft module.
            // > over >= to include the PrepareMerge proposal.
            let entries = if low > state.get_commit() {
                vec![]
            } else {
                self.peer
                    .get_store()
                    .entries(low, state.get_commit() + 1, NO_LIMIT)
                    .unwrap()
            };

            let sibling_peer = raftstore_util::find_peer(&sibling_region, self.store_id())
                .unwrap()
                .clone();
            let mut request = new_admin_request(sibling_region.get_id(), sibling_peer);
            request
                .mut_header()
                .set_region_epoch(sibling_region.get_region_epoch().clone());
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(AdminCmdType::CommitMerge);
            admin
                .mut_commit_merge()
                .set_source(self.peer.region().clone());
            admin.mut_commit_merge().set_commit(state.get_commit());
            admin
                .mut_commit_merge()
                .set_entries(RepeatedField::from_vec(entries));
            request.set_admin_request(admin);
            request
        };
        // Please note that, here assumes that the unit of network isolation is store rather than
        // peer. So a quorum stores of souce region should also be the quorum stores of target
        // region. Otherwise we need to enable proposal forwarding.
        // TODO: propose raft command on other node.
        Ok(())
    }

    fn rollback_merge(&mut self) {
        let req = {
            let state = self.peer.pending_merge_state.as_ref().unwrap();
            let mut request = new_admin_request(self.region_id(), self.peer.peer.clone());
            request
                .mut_header()
                .set_region_epoch(self.peer.region().get_region_epoch().clone());
            let mut admin = AdminRequest::new();
            admin.set_cmd_type(AdminCmdType::RollbackMerge);
            admin.mut_rollback_merge().set_commit(state.get_commit());
            request.set_admin_request(admin);
            request
        };
        self.propose_raft_command(req, Callback::None);
    }

    fn on_ready_commit_merge(&mut self, region: Region, source: Region) {
        let mut meta = self.store_meta.lock().unwrap();
        // TODO: tell source to destroy itself.
        let prev = meta.region_ranges.remove(&enc_end_key(&source));
        if region.get_end_key() == source.get_end_key() {
            assert_eq!(prev, Some(source.get_id()));
            let prev = meta.region_ranges.remove(&enc_start_key(&source));
            assert_eq!(prev, Some(region.get_id()));
        } else {
            assert_eq!(prev, Some(region.get_id()));
            let prev = meta.region_ranges.remove(&enc_end_key(&region));
            assert_eq!(prev, Some(source.get_id()));
        }
        assert!(
            meta.region_ranges
                .insert(enc_end_key(&region), region.get_id())
                .is_none()
        );
        assert!(meta.regions.remove(&source.get_id()).is_some());
        meta.set_region(region, &mut self.peer);
        if self.peer.is_leader() {
            info!(
                "{} notify pd with merge {:?} into {:?}",
                self.peer.tag,
                source,
                self.peer.region()
            );
            self.peer.heartbeat_pd2(&self.pd_scheduler);
        }
    }

    /// Handle rollbacking Merge result.
    ///
    /// If commit is 0, it means that Merge is rollbacked by a snapshot; otherwise
    /// it's rollbacked by a proposal, and its value should be equal to the commit
    /// index of previous PrepareMerge.
    fn on_ready_rollback_merge(&mut self, commit: u64, region: Option<metapb::Region>) {
        let pending_commit = self.peer.pending_merge_state.as_ref().unwrap().get_commit();
        if commit != 0 && pending_commit != commit {
            panic!(
                "{} rollbacks a wrong merge: {} != {}",
                self.peer.tag, pending_commit, commit
            );
        }
        self.peer.pending_merge_state = None;
        if let Some(r) = region {
            let mut meta = self.store_meta.lock().unwrap();
            meta.set_region(r, &mut self.peer);
        }
        if self.peer.is_leader() {
            info!("{} notify pd with rollback merge {}", self.peer.tag, commit);
            self.peer.heartbeat_pd2(&self.pd_scheduler);
        }
    }

    fn on_ready_apply_snapshot(&mut self, apply_result: ApplySnapResult) {
        let prev_region = apply_result.prev_region;
        let region = apply_result.region;

        info!(
            "{} snapshot for region {:?} is applied",
            self.peer.tag, region
        );

        let mut meta = self.store_meta.lock().unwrap();

        let initialized = !prev_region.get_peers().is_empty();
        if initialized {
            info!(
                "{} region changed from {:?} -> {:?} after applying snapshot",
                self.peer.tag, prev_region, region
            );
        }

        let prev = meta
            .region_ranges
            .insert(enc_end_key(&region), region.get_id());
        if prev.map_or(initialized, |id| id != prev_region.get_id() || !initialized) {
            panic!(
                "{} meta corrupted: {:?} {:?} {:?}",
                self.peer.tag, prev, prev_region, initialized
            );
        }

        let prev = meta.regions.insert(region.get_id(), region);
        assert_eq!(prev, Some(prev_region));
    }

    fn on_ready_compute_hash(&mut self, region: Region, index: u64, snap: EngineSnapshot) {
        self.peer.consistency_state.last_check_time = Instant::now();
        let task = ConsistencyCheckTask::compute_hash(region, index, snap);
        info!("{} schedule {}", self.peer.tag, task);
        if let Err(e) = self.consistency_check_scheduler.schedule(task) {
            error!("{} schedule failed: {:?}", self.peer.tag, e);
        }
    }

    fn on_ready_verify_hash(&mut self, expected_index: u64, expected_hash: Vec<u8>) {
        let region_id = self.region_id();
        verify_and_store_hash(
            region_id,
            &mut self.peer.consistency_state,
            expected_index,
            expected_hash,
        );
    }

    // return false means the message is invalid, and can be ignored.
    fn validate_raft_msg(&mut self, msg: &RaftMessage) -> bool {
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!(
            "{} handle raft message {:?}, from {} to {}",
            self.peer.tag,
            msg.get_message().get_msg_type(),
            from.get_id(),
            to.get_id()
        );

        if to.get_store_id() != self.store_id() {
            warn!(
                "{} store not match, to store id {}, mine {}, ignore it",
                self.peer.tag,
                to.get_store_id(),
                self.store_id()
            );
            self.raft_metrics.message_dropped.mismatch_store_id += 1;
            return false;
        }

        if !msg.has_region_epoch() {
            error!("{} missing epoch in raft message, ignore it", self.peer.tag);
            self.raft_metrics.message_dropped.mismatch_region_epoch += 1;
            return false;
        }

        true
    }

    fn handle_destroy_peer(&mut self, job: DestroyPeerJob) -> bool {
        if job.initialized {
            self.peer
                .apply_scheduler
                .schedule(ApplyTask::destroy(job.region_id))
                .unwrap();
            self.peer
                .read_scheduler
                .schedule(ReadTask::destroy(job.region_id))
                .unwrap();
        }
        if job.async_remove {
            info!("{} is destroyed asychroniously", self.peer.tag);
            false
        } else {
            self.destroy_peer(false);
            true
        }
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let from_epoch = msg.get_region_epoch();
        if !raftstore_util::is_epoch_stale(self.peer.region().get_region_epoch(), from_epoch) {
            return;
        }

        if self.peer.peer != *msg.get_to_peer() {
            info!("{} receive stale gc message, ignore.", self.peer.tag);
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        // TODO: ask pd to guarantee we are stale now.
        info!(
            "{} peer {:?} receives gc message, trying to remove",
            self.peer.tag,
            msg.get_to_peer()
        );
        match self.peer.maybe_destroy() {
            Some(job) => {
                self.handle_destroy_peer(job);
            }
            None => self.raft_metrics.message_dropped.applying_snap += 1,
        }
    }

    /// Check if it's necessary to gc the source merge peer.
    ///
    /// If the target merge peer won't be created on this store,
    /// then it's appropriate to destroy it immediately.
    fn need_gc_merge(&mut self, msg: &RaftMessage) -> Result<bool> {
        let merge_target = msg.get_merge_target();
        let target_region_id = merge_target.get_id();

        let meta = self.store_meta.lock().unwrap();

        if let Some(epoch) = meta.pending_cross_snap.get(&target_region_id).or_else(|| {
            meta.regions
                .get(&target_region_id)
                .map(|r| r.get_region_epoch())
        }) {
            info!(
                "{} checking target {} epoch: {:?}",
                self.peer.tag, target_region_id, epoch
            );
            // So the target peer has moved on, we should let it go.
            if epoch.get_version() > merge_target.get_region_epoch().get_version() {
                return Ok(true);
            }
            // Wait till it catching up logs.
            return Ok(false);
        }

        let state_key = keys::region_state_key(target_region_id);
        if let Some(state) = self
            .kv_engine()
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &state_key)?
        {
            debug!(
                "{} check local state {:?} for region {}",
                self.peer.tag, state, target_region_id
            );
            if state.get_state() == PeerState::Tombstone
                && state.get_region().get_region_epoch().get_conf_ver()
                    >= merge_target.get_region_epoch().get_conf_ver()
            {
                // Replica was destroyed.
                return Ok(true);
            }
        }

        info!(
            "{} no replica of region {} exist, check pd.",
            self.peer.tag, target_region_id
        );
        // We can't know whether the peer is destroyed or not for sure locally, ask
        // pd for help.
        let target_peer = merge_target
            .get_peers()
            .iter()
            .find(|p| p.get_store_id() == self.store_id())
            .unwrap();
        let task = PdTask::ValidatePeer {
            peer: target_peer.to_owned(),
            region: merge_target.to_owned(),
            merge_source: Some(self.region_id()),
        };
        if let Err(e) = self.pd_scheduler.schedule(task) {
            error!(
                "{} failed to validate target peer {:?}: {}",
                self.peer.tag, target_peer, e
            );
        }
        Ok(false)
    }

    /// Merge finished on other nodes, and it's impossible for this node
    /// to continue.
    fn on_stale_merge(&mut self) {
        info!("{} merge fail, try gc stale peer.", self.peer.tag);
        if let Some(job) = self.peer.maybe_destroy() {
            self.handle_destroy_peer(job);
        }
    }

    fn handle_stale_msg(
        &mut self,
        msg: &RaftMessage,
        need_gc: bool,
        target_region: Option<metapb::Region>,
    ) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();
        let msg_type = msg.get_message().get_msg_type();
        let cur_epoch = self.peer.region().get_region_epoch();

        if !need_gc {
            info!(
                "{} raft message {:?} is stale, current {:?}, ignore it",
                self.peer.tag, msg_type, cur_epoch
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            return;
        }

        info!(
            "{} raft message {:?} is stale, current {:?}, tell to gc",
            self.peer.tag, msg_type, cur_epoch
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
            error!("{} send gc message failed {:?}", self.peer.tag, e);
        }
    }

    fn check_msg(&mut self, msg: &RaftMessage) -> bool {
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

        if raftstore_util::is_epoch_stale(from_epoch, self.peer.region().get_region_epoch())
            && raftstore_util::find_peer(self.peer.region(), from_store_id).is_none()
        {
            // The message is stale and not in current region.
            self.handle_stale_msg(msg, is_vote_msg, None);
            return true;
        }

        let target = msg.get_from_peer();
        if target.get_id() < self.peer.peer_id() {
            info!(
                "{} target peer id {} is less than {}, msg maybe stale.",
                self.peer.tag,
                target.get_id(),
                self.peer.peer_id()
            );
            self.raft_metrics.message_dropped.stale_msg += 1;
            true
        } else if target.get_id() > self.peer.peer_id() {
            match self.peer.maybe_destroy() {
                Some(job) => {
                    info!("{} is stale as received a larger peer {:?}, destroying.", self.peer.tag, target);
                    self.handle_destroy_peer(job);
                }
                None => self.raft_metrics.message_dropped.applying_snap += 1,
            }
            true
        } else {
            false
        }
    }

    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<Option<SnapKey>> {
        if !msg.get_message().has_snapshot() {
            return Ok(None);
        }

        let region_id = msg.get_region_id();
        let snap = msg.get_message().get_snapshot();
        let key = SnapKey::from_region_snap(region_id, snap);
        let mut snap_data = RaftSnapshotData::new();
        snap_data.merge_from_bytes(snap.get_data())?;
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();

        if snap_region
            .get_peers()
            .iter()
            .all(|p| p.get_id() != peer_id)
        {
            info!(
                "{} {:?} doesn't contain peer {:?}, skip.",
                self.peer.tag,
                snap_region,
                msg.get_to_peer()
            );
            self.raft_metrics.message_dropped.region_no_peer += 1;
            return Ok(Some(key));
        }

        let mut meta = self.store_meta.lock().unwrap();
        let r = meta
            .region_ranges
            .range((Excluded(enc_start_key(&snap_region)), Unbounded::<Vec<u8>>))
            .map(|(_, region_id)| &meta.regions[region_id])
            .take_while(|r| enc_start_key(r) < enc_end_key(&snap_region))
            .skip_while(|r| r.get_id() == region_id)
            .next()
            .map(|r| r.to_owned());
        if let Some(exist_region) = r {
            info!(
                "{} region overlapped {:?}, {:?}",
                self.peer.tag, exist_region, snap_region
            );
            meta.pending_cross_snap
                .insert(region_id, snap_region.get_region_epoch().to_owned());
            self.raft_metrics.message_dropped.region_overlap += 1;
            return Ok(Some(key));
        }
        for region in &meta.pending_snapshot_regions {
            if enc_start_key(region) < enc_end_key(&snap_region) &&
               enc_end_key(region) > enc_start_key(&snap_region) &&
               // Same region can overlap, we will apply the latest version of snapshot.
               region.get_id() != snap_region.get_id()
            {
                info!("pending region overlapped {:?}, {:?}", region, snap_region);
                self.raft_metrics.message_dropped.region_overlap += 1;
                return Ok(Some(key));
            }
        }
        if let Some(r) = meta.pending_cross_snap.get(&region_id) {
            // Check it to avoid epoch moves backward.
            if raftstore_util::is_epoch_stale(snap_region.get_region_epoch(), r) {
                info!(
                    "{} snapshot epoch is stale, drop: {:?} < {:?}",
                    self.peer.tag,
                    snap_region.get_region_epoch(),
                    r
                );
                self.raft_metrics.message_dropped.stale_msg += 1;
                return Ok(Some(key));
            }
        }
        // check if snapshot file exists.
        self.snap_mgr.get_snapshot_for_applying(&key)?;

        meta.pending_snapshot_regions.push(snap_region);
        self.queued_snapshot = true;
        meta.pending_cross_snap.remove(&region_id);

        Ok(None)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        if !self.validate_raft_msg(&msg) {
            return Ok(());
        }

        if msg.get_is_tombstone() {
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }

        if msg.has_merge_target() {
            if self.need_gc_merge(&msg)? {
                self.on_stale_merge();
            }
            return Ok(());
        }

        if self.check_msg(&msg) {
            return Ok(());
        }

        if let Some(key) = self.check_snapshot(&msg)? {
            // If the snapshot file is not used again, then it's OK to
            // delete them here. If the snapshot file will be reused when
            // receiving, then it will fail to pass the check again, so
            // missing snapshot files should not be noticed.
            let s = self.snap_mgr.get_snapshot_for_applying(&key)?;
            self.snap_mgr.delete_snapshot(&key, s.as_ref(), false);
            return Ok(());
        }

        let from_peer_id = msg.get_from_peer().get_id();
        self.peer.insert_peer_cache(msg.take_from_peer());
        self.peer.step(msg.take_message())?;

        if self.peer.any_new_peer_catch_up(from_peer_id) {
            self.peer.heartbeat_pd2(&self.pd_scheduler);
        }

        self.has_ready = true;
        Ok(())
    }

    fn execute_region_leader(&mut self, _: &RaftCmdRequest) -> StatusResponse {
        let mut resp = StatusResponse::new();
        if let Some(leader) = self.peer.get_peer_from_cache(self.peer.leader_id()) {
            resp.mut_region_leader().set_leader(leader);
        }

        resp
    }

    fn execute_region_detail(&mut self, request: &RaftCmdRequest) -> Result<StatusResponse> {
        if !self.peer.get_store().is_initialized() {
            let region_id = request.get_header().get_region_id();
            return Err(Error::RegionNotInitialized(region_id));
        }
        let mut resp = StatusResponse::new();
        resp.mut_region_detail()
            .set_region(self.peer.region().clone());
        if let Some(leader) = self.peer.get_peer_from_cache(self.peer.leader_id()) {
            resp.mut_region_detail().set_leader(leader);
        }

        Ok(resp)
    }

    // Handle status commands here, separate the logic, maybe we can move it
    // to another file later.
    // Unlike other commands (write or admin), status commands only show current
    // store status, so no need to handle it in raft group.
    fn execute_status_command(&mut self, request: &RaftCmdRequest) -> Result<RaftCmdResponse> {
        let cmd_type = request.get_status_request().get_cmd_type();

        let mut response = match cmd_type {
            StatusCmdType::RegionLeader => self.execute_region_leader(request),
            StatusCmdType::RegionDetail => self.execute_region_detail(request)?,
            StatusCmdType::InvalidStatus => return Err(box_err!("invalid status command!")),
        };
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_status_response(response);
        // Bind peer current term here.
        bind_term(&mut resp, self.peer.term());
        Ok(resp)
    }

    pub fn find_sibling_region(&self, region: &Region) -> Option<Region> {
        let start = if self.peer.cfg.right_derive_when_split {
            Included(enc_start_key(region))
        } else {
            Excluded(enc_end_key(region))
        };
        let meta = self.store_meta.lock().unwrap();
        meta.region_ranges
            .range((start, Unbounded::<Vec<u8>>))
            .next()
            .and_then(|(_, region_id)| meta.regions.get(region_id).cloned())
    }

    fn pre_propose_raft_command(
        &mut self,
        msg: &RaftCmdRequest,
    ) -> Result<Option<RaftCmdResponse>> {
        // Check store_id, make sure that the msg is dispatched to the right place.
        raftstore_util::check_store_id(msg, self.store_id())?;
        if msg.has_status_request() {
            // For status commands, we handle it here directly.
            let resp = self.execute_status_command(msg)?;
            return Ok(Some(resp));
        }

        // Check whether the store has the right peer to handle the request.
        let region_id = self.region_id();
        let leader_id = self.peer.leader_id();
        if !self.peer.is_leader() {
            let leader = self.peer.get_peer_from_cache(leader_id);
            return Err(Error::NotLeader(region_id, leader));
        }
        // peer_id must be the same as peer's.
        raftstore_util::check_peer_id(msg, self.peer.peer_id())?;
        // Check whether the term is stale.
        raftstore_util::check_term(msg, self.peer.term())?;

        match raftstore_util::check_region_epoch(msg, self.peer.region(), true) {
            Err(Error::StaleEpoch(msg, mut new_regions)) => {
                // Attach the region which might be split from the current region. But it doesn't
                // matter if the region is not split from the current region. If the region meta
                // received by the TiKV driver is newer than the meta cached in the driver, the meta is
                // updated.
                let sibling_region = self.find_sibling_region(self.peer.region());
                if let Some(sibling_region) = sibling_region {
                    new_regions.push(sibling_region);
                }
                Err(Error::StaleEpoch(msg, new_regions))
            }
            Err(e) => Err(e),
            Ok(()) => Ok(None),
        }
    }

    /// Check if a request is valid if it has valid prepare_merge/commit_merge proposal.
    fn check_merge_proposal(&self, msg: &RaftCmdRequest) -> Result<()> {
        if !msg.get_admin_request().has_prepare_merge()
            && !msg.get_admin_request().has_commit_merge()
        {
            return Ok(());
        }

        let region_id = msg.get_header().get_region_id();
        let region = self.peer.region();

        if msg.get_admin_request().has_prepare_merge() {
            let target_region = msg.get_admin_request().get_prepare_merge().get_target();
            {
                let meta = self.store_meta.lock().unwrap();
                match meta.regions.get(&target_region.get_id()) {
                    Some(r) => if r != target_region {
                        return Err(box_err!(
                            "target region not matched, skip proposing: {:?} != {:?}",
                            r,
                            target_region
                        ));
                    },
                    None => return Err(box_err!("target region doesn't exist.")),
                }
            }
            if !raftstore_util::is_sibling_regions(target_region, region) {
                return Err(box_err!("regions are not sibling, skip proposing."));
            }
            if !raftstore_util::region_on_same_stores(target_region, region) {
                return Err(box_err!(
                    "peers doesn't match {:?} != {:?}, reject merge",
                    region.get_peers(),
                    target_region.get_peers()
                ));
            }
        } else {
            let source_region = msg.get_admin_request().get_commit_merge().get_source();
            if !raftstore_util::is_sibling_regions(source_region, region) {
                return Err(box_err!(
                    "{} {:?} {:?} should be sibling",
                    self.peer.tag,
                    source_region,
                    region
                ));
            }
            if !raftstore_util::region_on_same_stores(source_region, region) {
                return Err(box_err!(
                    "peers not matched: {:?} {:?}",
                    source_region,
                    region
                ));
            }
        };

        Ok(())
    }

    fn propose_raft_command(&mut self, cmd: RaftCmdRequest, cb: Callback) {
        match self.pre_propose_raft_command(&cmd) {
            Ok(Some(resp)) => {
                cb.invoke_with_response(resp);
                return;
            }
            Err(e) => {
                debug!("{} failed to propose {:?}: {:?}", self.peer.tag, cmd, e);
                cb.invoke_with_response(new_error(e));
                return;
            }
            _ => (),
        }

        if let Err(e) = self.check_merge_proposal(&cmd) {
            warn!(
                "{} failed to propose merge: {:?}: {}",
                self.peer.tag, cmd, e
            );
            cb.invoke_with_response(new_error(e));
            return;
        }

        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.

        let mut resp = RaftCmdResponse::new();
        let term = self.peer.term();
        bind_term(&mut resp, term);
        if self
            .peer
            .propose(cb, cmd, resp, &mut self.raft_metrics.propose)
        {
            self.has_ready = true;
        }

        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
    }

    fn on_hash_computed(&mut self, index: u64, hash: Vec<u8>) {
        let region_id = self.region_id();
        if !verify_and_store_hash(region_id, &mut self.peer.consistency_state, index, hash) {
            return;
        }

        let mut request = new_admin_request(region_id, self.peer.peer.clone());

        let mut admin = AdminRequest::new();
        admin.set_cmd_type(AdminCmdType::VerifyHash);
        admin
            .mut_verify_hash()
            .set_index(self.peer.consistency_state.index);
        admin
            .mut_verify_hash()
            .set_hash(self.peer.consistency_state.hash.clone());
        request.set_admin_request(admin);

        self.propose_raft_command(request, Callback::None);
    }

    fn validate_split_region(&mut self, epoch: &RegionEpoch, split_keys: &[Vec<u8>]) -> Result<()> {
        if split_keys.is_empty() {
            return Err(box_err!("no split key is specified."));
        }
        for key in split_keys {
            if key.is_empty() {
                return Err(box_err!("split key should not be empty"));
            }
        }
        if !self.peer.is_leader() {
            return Err(Error::NotLeader(
                self.region_id(),
                self.peer.get_peer_from_cache(self.peer.leader_id()),
            ));
        }

        let region = self.peer.region();
        let latest_epoch = region.get_region_epoch();

        if latest_epoch.get_version() != epoch.get_version() {
            return Err(Error::StaleEpoch(
                format!(
                    "epoch changed {:?} != {:?}, retry later",
                    latest_epoch, epoch
                ),
                vec![region.to_owned()],
            ));
        }
        Ok(())
    }

    fn on_prepare_split_region(
        &mut self,
        region_epoch: metapb::RegionEpoch,
        split_keys: Vec<Vec<u8>>,
        cb: Callback,
    ) {
        if let Err(e) = self.validate_split_region(&region_epoch, &split_keys) {
            warn!("{} invalid split request: {:?}", self.peer.tag, e);
            cb.invoke_with_response(new_error(e));
            return;
        }
        let region = self.peer.region();
        let task = PdTask::AskBatchSplit {
            region: region.clone(),
            split_keys,
            peer: self.peer.peer.clone(),
            right_derive: self.peer.cfg.right_derive_when_split,
            callback: cb,
        };
        if let Err(Stopped(t)) = self.pd_scheduler.schedule(task) {
            error!("{} failed to notify pd to split: Stopped", self.peer.tag);
            match t {
                PdTask::AskBatchSplit { callback, .. } => {
                    callback.invoke_with_response(new_error(box_err!("failed to split: Stopped")));
                }
                _ => unreachable!(),
            }
        }
    }

    fn on_schedule_half_split_region(&mut self, region_epoch: &RegionEpoch, policy: CheckPolicy) {
        if !self.peer.is_leader() {
            // region on this store is no longer leader, skipped.
            warn!("{} is not leader, skip.", self.peer.tag,);
            return;
        }

        let region = self.peer.region();
        if raftstore_util::is_epoch_stale(region_epoch, region.get_region_epoch()) {
            warn!("{} receive a stale halfsplit message", self.peer.tag);
            return;
        }

        let task = SplitCheckTask::new(region.clone(), false, policy);
        if let Err(e) = self.split_check_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
    }

    fn on_raft_base_tick(&mut self) {
        if !self.peer.pending_remove {
            if !self.peer.is_applying_snapshot() && !self.peer.has_pending_snapshot() {
                if self.peer.raft_group.tick() {
                    self.has_ready = true;
                }
            } else {
                // need to check if snapshot is applied.
                self.has_ready = true;
            }
            self.schedule_raft_base_tick();
        }

        self.raft_metrics.flush();
    }

    #[inline]
    fn schedule_tick(&self, dur: Duration, tick: PeerTick) {
        if dur != Duration::new(0, 0) {
            let mut tx = self.sender.clone();
            let f = self.timer.delay(Instant::now() + dur).map(move |_| {
                let _ = tx.force_send(PeerMsg::Tick(tick));
            });
            self.poller.spawn(f).forget()
        }
    }

    #[inline]
    fn schedule_raft_base_tick(&self) {
        self.schedule_tick(self.peer.cfg.raft_base_tick_interval.0, PeerTick::Raft)
    }

    #[inline]
    fn schedule_raft_gc_log_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.raft_log_gc_tick_interval.0,
            PeerTick::RaftLogGc,
        )
    }

    fn on_raft_gc_log_tick(&mut self) {
        self.schedule_raft_gc_log_tick();

        let applied_idx = self.peer.get_store().applied_index();
        if !self.peer.is_leader() {
            self.peer.mut_store().compact_to(applied_idx + 1);
            return;
        }

        // Leader will replicate the compact log command to followers,
        // If we use current replicated_index (like 10) as the compact index,
        // when we replicate this log, the newest replicated_index will be 11,
        // but we only compact the log to 10, not 11, at that time,
        // the first index is 10, and replicated_index is 11, with an extra log,
        // and we will do compact again with compact index 11, in cycles...
        // So we introduce a threshold, if replicated index - first index > threshold,
        // we will try to compact log.
        // raft log entries[..............................................]
        //                  ^                                       ^
        //                  |-----------------threshold------------ |
        //              first_index                         replicated_index
        // `healthy_replicated_index` is the smallest `replicated_index` of healthy nodes.
        let truncated_idx = self.peer.get_store().truncated_index();
        let last_idx = self.peer.get_store().last_index();
        let (mut replicated_idx, mut healthy_replicated_idx) = (last_idx, last_idx);
        for (_, p) in self.peer.raft_group.raft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if healthy_replicated_idx > p.matched && p.matched >= truncated_idx {
                healthy_replicated_idx = p.matched;
            }
        }
        // When an election happened or a new peer is added, replicated_idx can be 0.
        if replicated_idx > 0 {
            assert!(
                last_idx >= replicated_idx,
                "expect last index {} >= replicated index {}",
                last_idx,
                replicated_idx
            );
            REGION_MAX_LOG_LAG.observe((last_idx - replicated_idx) as f64);
        }
        self.peer
            .mut_store()
            .maybe_gc_cache(healthy_replicated_idx, applied_idx);
        let first_idx = self.peer.get_store().first_index();
        let mut compact_idx;
        if (applied_idx > first_idx
            && applied_idx - first_idx >= self.peer.cfg.raft_log_gc_count_limit)
            || self.peer.raft_log_size_hint >= self.peer.cfg.raft_log_gc_size_limit.0
        {
            compact_idx = applied_idx;
        } else if replicated_idx < first_idx
            || replicated_idx - first_idx <= self.peer.cfg.raft_log_gc_threshold
        {
            return;
        } else {
            compact_idx = replicated_idx;
        }

        // Have no idea why subtract 1 here, but original code did this by magic.
        assert!(compact_idx > 0);
        compact_idx -= 1;
        if compact_idx < first_idx {
            // In case compact_idx == first_idx before subtraction.
            return;
        }

        let term = self
            .peer
            .raft_group
            .raft
            .raft_log
            .term(compact_idx)
            .unwrap();

        // Create a compact log request and notify directly.
        let region_id = self.region_id();
        let peer = self.peer.peer.clone();
        let request = new_compact_log_request(region_id, peer, compact_idx, term);
        self.propose_raft_command(request, Callback::None);
        self.schedule_raft_gc_log_tick();
    }

    #[inline]
    fn schedule_split_region_check_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.split_region_check_tick_interval.0,
            PeerTick::SplitRegionCheck,
        );
    }

    fn on_split_region_check_tick(&mut self) {
        // To avoid frequent scan, we only add new scan tasks if all previous tasks
        // have finished.
        if !self.peer.is_leader() {
            return;
        }
        // When restart, the approximate size will be None. The
        // split check will first check the region size, and then
        // check whether the region should split.  This should
        // work even if we change the region max size.
        // If peer says should update approximate size, update region
        // size and check whether the region should split.
        if self.peer.approximate_size.is_some()
            && self.peer.compaction_declined_bytes < self.peer.cfg.region_split_check_diff.0
            && self.peer.size_diff_hint < self.peer.cfg.region_split_check_diff.0
        {
            self.schedule_split_region_check_tick();
            return;
        }

        let task = SplitCheckTask::new(self.peer.region().clone(), true, CheckPolicy::SCAN);
        if let Err(e) = self.split_check_scheduler.schedule(task) {
            error!("{} failed to schedule split check: {}", self.peer.tag, e);
        }
        self.peer.size_diff_hint = 0;
        self.peer.compaction_declined_bytes = 0;

        self.schedule_split_region_check_tick();
    }

    #[inline]
    fn schedule_pd_heartbeat_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.pd_heartbeat_tick_interval.0,
            PeerTick::PdHeartbeat,
        );
    }

    fn on_pd_heartbeat_tick(&mut self) {
        self.peer.check_peers();

        if self.peer.is_leader() {
            self.peer.heartbeat_pd2(&self.pd_scheduler);
            self.schedule_pd_heartbeat_tick();
        }
        // TODO: how to report region count and leader count?
    }

    fn on_consistency_check_tick(&mut self) {
        unimplemented!()
    }

    #[inline]
    fn schedule_consistency_check_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.consistency_check_interval.0,
            PeerTick::ConsistencyCheck,
        );
    }

    fn on_check_merge(&mut self) {
        if let Err(e) = self.schedule_merge() {
            info!(
                "{} failed to schedule merge, rollback: {:?}",
                self.peer.tag, e
            );
            self.rollback_merge();
        } else {
            self.schedule_merge_check_tick();
        }
    }

    #[inline]
    fn schedule_merge_check_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.merge_check_tick_interval.0,
            PeerTick::CheckMerge,
        )
    }

    fn on_check_peer_stale_state_tick(&mut self) {
        if self.peer.pending_remove || self.peer.is_leader() {
            return;
        }

        if self.peer.is_applying_snapshot() || self.peer.has_pending_snapshot() {
            self.schedule_check_peer_stale_state_tick();
            return;
        }

        // If this peer detects the leader is missing for a long long time,
        // it should consider itself as a stale peer which is removed from
        // the original cluster.
        // This most likely happens in the following scenario:
        // At first, there are three peer A, B, C in the cluster, and A is leader.
        // Peer B gets down. And then A adds D, E, F into the cluster.
        // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
        // After all these peer in and out, now the cluster has peer D, E, F.
        // If peer B goes up at this moment, it still thinks it is one of the cluster
        // and has peers A, C. However, it could not reach A, C since they are removed
        // from the cluster or probably destroyed.
        // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
        // In this case, peer B would notice that the leader is missing for a long time,
        // and it would check with pd to confirm whether it's still a member of the cluster.
        // If not, it destroys itself as a stale peer which is removed out already.
        match self.peer.check_stale_state() {
            StaleState::Valid => (),
            StaleState::LeaderMissing => {
                warn!(
                    "{} leader missing longer than abnormal_leader_missing_duration {:?}",
                    self.peer.tag, self.peer.cfg.abnormal_leader_missing_duration.0,
                );
            }
            StaleState::ToValidate => {
                // for peer B in case 1 above
                warn!(
                    "{} leader missing longer than max_leader_missing_duration {:?}. \
                     To check with pd whether it's still valid",
                    self.peer.tag, self.peer.cfg.max_leader_missing_duration.0,
                );
                let task = PdTask::ValidatePeer {
                    peer: self.peer.peer.clone(),
                    region: self.peer.region().clone(),
                    merge_source: None,
                };
                if let Err(e) = self.pd_scheduler.schedule(task) {
                    error!("{} failed to notify pd: {}", self.peer.tag, e)
                }
            }
        }

        // TODO: report leader missing

        self.schedule_check_peer_stale_state_tick();
    }

    #[inline]
    fn schedule_check_peer_stale_state_tick(&self) {
        self.schedule_tick(
            self.peer.cfg.peer_stale_state_check_interval.0,
            PeerTick::CheckPeerStaleState,
        )
    }

    fn report_snapshot_status(&mut self, to_peer_id: u64, status: SnapshotStatus) {
        let to_peer = match self.peer.get_peer_from_cache(to_peer_id) {
            Some(peer) => peer,
            None => {
                // If to_peer is gone, ignore this snapshot status
                warn!(
                    "{} peer {} not found, ignore snapshot status {:?}",
                    self.peer.tag, to_peer_id, status
                );
                return;
            }
        };
        info!(
            "{} report snapshot status {:?} {:?}",
            self.peer.tag, to_peer, status
        );
        self.peer.raft_group.report_snapshot(to_peer_id, status)
    }

    fn on_ingest_sst_result(&mut self, ssts: Vec<SSTMeta>) {
        for sst in &ssts {
            self.peer.size_diff_hint += sst.get_length();
        }

        let task = CleanupSSTTask::DeleteSST { ssts };
        if let Err(e) = self.cleanup_sst_scheduler.schedule(task) {
            error!("{} schedule to delete ssts: {:?}", self.peer.tag, e);
        }
    }

    fn on_apply_result(&mut self, res: ApplyRes) {
        debug!("{} async apply finish: {:?}", self.peer.tag, res);
        let ApplyRes {
            region_id,
            apply_state,
            applied_index_term,
            exec_res,
            metrics,
            merged,
        } = res;
        // handle executing committed log results
        for result in exec_res {
            match result {
                ExecResult::ChangePeer(cp) => self.on_ready_change_peer(cp),
                ExecResult::CompactLog { first_index, state } => if !merged {
                    self.on_ready_compact_log(first_index, state)
                },
                ExecResult::SplitRegion { derived, regions } => {
                    self.on_ready_split_region(derived, regions)
                }
                ExecResult::PrepareMerge { region, state } => {
                    self.on_ready_prepare_merge(region, state, merged);
                }
                ExecResult::CommitMerge { region, source } => {
                    self.on_ready_commit_merge(region, source);
                }
                ExecResult::RollbackMerge { region, commit } => {
                    self.on_ready_rollback_merge(commit, Some(region))
                }
                ExecResult::ComputeHash {
                    region,
                    index,
                    snap,
                } => self.on_ready_compute_hash(region, index, snap),
                ExecResult::VerifyHash { index, hash } => self.on_ready_verify_hash(index, hash),
                ExecResult::DeleteRange { .. } => {
                    // TODO: clean user properties?
                }
                ExecResult::IngestSST { ssts } => self.on_ingest_sst_result(ssts),
            }
        }
        self.has_ready |= self
            .peer
            .post_apply2(apply_state, applied_index_term, merged, &metrics);
    }

    fn on_significant_msg(&mut self, m: SignificantMsg) {
        match m {
            SignificantMsg::SnapshotStatus {
                to_peer_id, status, ..
            } => self.report_snapshot_status(to_peer_id, status),
            SignificantMsg::Unreachable { to_peer_id, .. } => {
                self.peer.raft_group.report_unreachable(to_peer_id)
            }
        }
    }

    fn on_tick(&mut self, tick: PeerTick) {
        match tick {
            PeerTick::Raft => self.on_raft_base_tick(),
            PeerTick::SplitRegionCheck => self.on_split_region_check_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat_tick(),
            PeerTick::RaftLogGc => self.on_raft_gc_log_tick(),
            PeerTick::CheckMerge => self.on_check_merge(),
            PeerTick::CheckPeerStaleState => self.on_check_peer_stale_state_tick(),
            PeerTick::ConsistencyCheck => self.on_consistency_check_tick(),
        }
    }

    fn on_peer_msg(&mut self, m: PeerMsg) {
        match m {
            PeerMsg::RaftMessage(data) => if let Err(e) = self.on_raft_message(data) {
                error!("{} handle raft message err: {:?}", self.peer.tag, e);
            },
            PeerMsg::RaftCmd {
                send_time,
                request,
                callback,
            } => {
                self.raft_metrics
                    .propose
                    .request_wait_time
                    .observe(duration_to_sec(send_time.elapsed()) as f64);
                self.propose_raft_command(request, callback)
            }
            PeerMsg::Tick(t) => self.on_tick(t),
            PeerMsg::ApplyRes(res) => self.on_apply_result(res),
            PeerMsg::SignificantMsg(m) => self.on_significant_msg(m),
            PeerMsg::ComputeHashResult {
                index,
                hash,
            } => self.on_hash_computed(index, hash),
            // TODO: format keys
            PeerMsg::SplitRegion {
                region_epoch,
                split_keys,
                callback,
            } => {
                info!(
                    "{} on split region at key {:?}.",
                    self.peer.tag, split_keys
                );
                self.on_prepare_split_region(region_epoch, split_keys, callback);
            }
            PeerMsg::RegionApproximateSize { size } => {
                self.peer.approximate_size = Some(size);
            }
            PeerMsg::RegionApproximateKeys { keys } => {
                self.peer.approximate_keys = Some(keys);
            }
            PeerMsg::HalfSplitRegion {
                region_epoch,
                policy,
            } => self.on_schedule_half_split_region(&region_epoch, policy),
            PeerMsg::MergeFail => self.on_stale_merge(),
            PeerMsg::Quit => {
                info!("{} receive quit message", self.peer.tag);
                self.stop();
            }
        }
    }

    fn handle_raft_ready(&mut self) {
        let t = SlowTimer::new();
        let previous_ready_metrics = self.raft_metrics.ready.clone();

        self.raft_metrics.ready.pending_region += 1;

        let (kv_wb, raft_wb, append_res, sync_log) = {
            let mut ctx = ReadyContext::new(&mut self.raft_metrics, &self.trans);
            self.peer.handle_raft_ready_append(&mut ctx, &self.pd_scheduler);
            (ctx.kv_wb, ctx.raft_wb, ctx.ready_res, ctx.sync_log)
        };

        if let Some(proposal) = self.peer.take_apply_proposals() {
            self.peer.apply_scheduler
                .schedule(ApplyTask::Proposals(proposal))
                .unwrap();

            // In most cases, if the leader proposes a message, it will also
            // broadcast the message to other followers, so we should flush the
            // messages ASAP.
            self.trans.flush();
        }

        self.raft_metrics.ready.has_ready_region += append_res.is_some() as u64;

        // apply_snapshot, peer_destroy will clear_meta, so we need write region state first.
        // otherwise, if program restart between two write, raft log will be removed,
        // but region state may not changed in disk.
        fail_point!("raft_before_save");
        if !kv_wb.is_empty() {
            // RegionLocalState, ApplyState
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            self.peer.engines
                .kv
                .write_opt(kv_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save append state result: {:?}", self.peer.tag, e);
                });
        }
        fail_point!("raft_between_save");

        if !raft_wb.is_empty() {
            // RaftLocalState, Raft Log Entry
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(self.peer.cfg.sync_log || sync_log);
            self.peer.engines
                .raft
                .write_opt(raft_wb, &write_opts)
                .unwrap_or_else(|e| {
                    panic!("{} failed to save raft append result: {:?}", self.peer.tag, e);
                });
        }
        fail_point!("raft_after_save");

        let ready_result = append_res.map(|(mut ready, invoke_ctx)| {
            let region_id = invoke_ctx.region_id;
            let is_merging = self.peer.pending_merge_state.is_some();
            let res = self.peer.post_raft_ready_append(
                &mut self.raft_metrics,
                &self.trans,
                &mut ready,
                invoke_ctx,
            );
            if is_merging && res.is_some() {
                // After applying a snapshot, merge is rollbacked implicitly.
                self.on_ready_rollback_merge(0, None);
            }
            (ready, res)
        });

        self.raft_metrics
            .append_log
            .observe(duration_to_sec(t.elapsed()) as f64);

        slow_log!(
            t,
            "{} handle {} ready, {} entries, {} messages and {} \
             snapshots",
            self.peer.tag,
            ready_result.is_some() as u64,
            self.raft_metrics.ready.append - previous_ready_metrics.append,
            self.raft_metrics.ready.message - previous_ready_metrics.message,
            self.raft_metrics.ready.snapshot - previous_ready_metrics.snapshot
        );

        if let Some((ready, res)) = ready_result {
            if let Some(apply_task) = self.peer.handle_raft_ready_apply(ready) {
                self.peer.apply_scheduler
                    .schedule(ApplyTask::apply(apply_task))
                    .unwrap();
            }
            if let Some(apply_result) = res {
                self.on_ready_apply_snapshot(apply_result);
            }
        }

        let dur = t.elapsed();
        self.raft_metrics
            .process_ready
            .observe(duration_to_sec(dur) as f64);

        self.trans.flush();

        slow_log!(t, "{} on raft ready", self.peer.tag);
    }
}

impl<T: Transport> Future for PeerAgent<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        let mut msgs;
        match self.receiver.poll() {
            Ok(Async::Ready(Some(m))) => {
                msgs = Vec::with_capacity(self.peer.cfg.messages_per_tick);
                msgs.push(m);
            }
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            _ => unreachable!()
        }
        loop {
            while msgs.len() < self.peer.cfg.messages_per_tick {
                match self.receiver.poll() {
                    Ok(Async::Ready(Some(m))) => msgs.push(m),
                    Ok(Async::NotReady) => break,
                    _ => unreachable!(),
                }
            }
            let keep_going = msgs.len() == self.peer.cfg.messages_per_tick;
            for m in msgs.drain(..) {
                self.on_peer_msg(m);
            }
            if self.has_ready {
                self.handle_raft_ready();
                self.has_ready = false;
            }
            if !self.stopped {
                if self.queued_snapshot {
                    let mut meta = self.store_meta.lock().unwrap();
                    meta.pending_snapshot_regions
                        .retain(|r| r.get_id() != self.region_id());
                }
                if keep_going {
                    continue;
                }
                return Ok(Async::NotReady);
            }
            return Ok(Async::Ready(()));
        }
    }
}

fn new_admin_request(region_id: u64, peer: metapb::Peer) -> RaftCmdRequest {
    let mut request = RaftCmdRequest::new();
    request.mut_header().set_region_id(region_id);
    request.mut_header().set_peer(peer);
    request
}

/// Verify and store the hash to state. return true means the hash has been stored successfully.
fn verify_and_store_hash(
    region_id: u64,
    state: &mut ConsistencyState,
    expected_index: u64,
    expected_hash: Vec<u8>,
) -> bool {
    if expected_index < state.index {
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] has scheduled a new hash: {} > {}, skip.",
            region_id, state.index, expected_index
        );
        return false;
    }

    if state.index == expected_index {
        if state.hash.is_empty() {
            warn!(
                "[region {}] duplicated consistency check detected, skip.",
                region_id
            );
            return false;
        }
        if state.hash != expected_hash {
            panic!(
                "[region {}] hash at {} not correct, want \"{}\", got \"{}\"!!!",
                region_id,
                state.index,
                escape(&expected_hash),
                escape(&state.hash)
            );
        }
        info!(
            "[region {}] consistency check at {} pass.",
            region_id, state.index
        );
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "matched"])
            .inc();
        state.hash = vec![];
        return false;
    }

    if state.index != INVALID_INDEX && !state.hash.is_empty() {
        // Maybe computing is too slow or computed result is dropped due to channel full.
        // If computing is too slow, miss count will be increased twice.
        REGION_HASH_COUNTER_VEC
            .with_label_values(&["verify", "miss"])
            .inc();
        warn!(
            "[region {}] hash belongs to index {}, but we want {}, skip.",
            region_id, state.index, expected_index
        );
    }

    info!(
        "[region {}] save hash of {} for consistency check later.",
        region_id, expected_index
    );
    state.index = expected_index;
    state.hash = expected_hash;
    true
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

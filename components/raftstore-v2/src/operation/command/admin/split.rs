// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains batch split related processing logic.
//!
//! Process Overview
//!
//! Propose:
//! - Nothing special except for validating batch split requests (ex: split keys
//!   are in ascending order).
//!
//! Execution:
//! - exec_batch_split: Create and initialize metapb::region for split regions
//!   and derived regions. Then, create checkpoints of the current talbet for
//!   split regions and derived region to make tablet physical isolated. Update
//!   the parent region's region state without persistency. Send the new regions
//!   (including derived region) back to raftstore.
//!
//! On Apply Result:
//! - on_ready_split_region: Update the relevant in memory meta info of the
//!   parent peer, and wrap and send to the store the relevant info needed to
//!   create and initialize the split regions.
//!
//! Split peer creation and initlization:
//! - init_split_region: In normal cases, the uninitialized split region will be
//!   created by the store, and here init it using the data sent from the parent
//!   peer.
//!
//! Split finish:
//! - handle_peer_split_response: If all split peers are initialized, the region
//!   state of the parent peer can be persisted

use std::collections::VecDeque;

use engine_traits::{
    Checkpointer, DeleteStrategy, KvEngine, OpenOptions, RaftEngine, RaftLogBatch, Range,
    TabletFactory, CF_DEFAULT, SPLIT_PREFIX,
};
use keys::enc_end_key;
use kvproto::{
    metapb::{self, Region, RegionEpoch},
    raft_cmdpb::{AdminRequest, AdminResponse, RaftCmdRequest, SplitRequest},
};
use protobuf::Message;
use raft::RawNode;
use raftstore::{
    coprocessor::{
        split_observer::{is_valid_split_key, strip_timestamp_if_exists},
        RegionChangeReason,
    },
    store::{
        fsm::apply::validate_batch_split,
        metrics::PEER_ADMIN_CMD_COUNTER,
        util::{self, KeysInfoFormatter},
        PeerPessimisticLocks, PeerStat, ProposalContext, Transport, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{error, info, warn, Logger};
use tikv_util::box_err;

use crate::{
    batch::StoreContext,
    fsm::{ApplyResReporter, PeerFsmDelegate},
    operation::AdminCmdResult,
    raft::{raft_config, write_initial_states, Apply, Peer, Storage},
    router::{message::PeerCreation, ApplyRes, PeerMsg, StoreMsg},
};

#[derive(Debug)]
pub struct SplitResult {
    pub regions: Vec<Region>,
    // The index of the derived region in `regions`
    pub derived_index: usize,
    pub tablet_index: u64,
}

#[derive(Debug)]
pub struct SplitRegionInitInfo {
    pub parent_region_id: u64,
    pub parent_epoch: RegionEpoch,
    /// Split region
    pub region: metapb::Region,
    pub parent_is_leader: bool,
    pub parent_stat: PeerStat,
    pub approximate_size: Option<u64>,
    pub approximate_keys: Option<u64>,
    /// In-memory pessimistic locks that should be inherited from parent region
    pub locks: PeerPessimisticLocks,
    pub last_split_region: bool,
}

#[derive(Debug)]
pub struct SplitRegionInitResp {
    pub parent_epoch: RegionEpoch,
    pub child_region_id: u64,
    // FIXME: when it is false
    pub result: bool,
}

pub enum AcrossPeerMsg {
    SplitRegionInit(Box<SplitRegionInitInfo>),
    SplitRegionInitResp(Box<SplitRegionInitResp>),
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn propose_split<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        req: RaftCmdRequest,
    ) -> Result<u64> {
        validate_batch_split(req.get_admin_request(), self.region())?;
        let mut proposal_ctx = ProposalContext::empty();
        proposal_ctx.insert(ProposalContext::SYNC_LOG);
        proposal_ctx.insert(ProposalContext::SPLIT);

        let data = req.write_to_bytes().unwrap();
        self.propose_with_ctx(store_ctx, data, proposal_ctx.to_vec())
    }

    pub fn on_ready_split_region<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        derived_index: usize,
        tablet_index: u64,
        regions: Vec<Region>,
    ) {
        let derived = &regions[derived_index];
        let derived_id = derived.id;
        let derived_epoch = derived.get_region_epoch().clone();
        let region_id = derived.get_id();

        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let region_locks = {
            let mut pessimistic_locks = self.txn_ext().pessimistic_locks.write();
            info!(self.logger, "moving {} locks to new regions", pessimistic_locks.len(); "region_id"=> region_id);
            // Update the version so the concurrent reader will fail due to EpochNotMatch
            // instead of PessimisticLockNotFound.
            pessimistic_locks.version = derived_epoch.get_version();
            pessimistic_locks.group_by_regions(&regions, derived)
        };

        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let estimated_size = self.approximate_size().map(|v| v / new_region_count);
        let estimated_keys = self.approximate_keys().map(|v| v / new_region_count);
        let mut meta = store_ctx.store_meta.lock().unwrap();
        meta.set_region(
            derived.clone(),
            self,
            RegionChangeReason::Split,
            tablet_index,
        );
        self.post_split();

        // It's not correct anymore, so set it to false to schedule a split
        // check task.
        self.set_may_skip_split_check(false);

        let is_leader = self.is_leader();
        if is_leader {
            self.set_approximate_size(estimated_size);
            self.set_approximate_keys(estimated_keys);
            // todo: this may should be doing in handle_peer_split_response.
            self.heartbeat_pd(store_ctx);

            info!(
                self.logger,
                "notify pd with split";
                "region_id" => self.region_id(),
                "peer_id" => self.peer_id(),
                "split_count" => regions.len(),
            );

            // todo: report to PD
        }

        assert!(self.split_progress_mut().is_empty());
        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!("{:?} original region should exist", self.logger.list());
        }
        let last_region_id = regions.last().unwrap().get_id();
        for (new_region, locks) in regions.into_iter().zip(region_locks) {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(enc_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);
                continue;
            }

            let mut raft_message = self.prepare_raft_message();
            raft_message.mut_from_peer().set_id(raft::INVALID_ID);
            raft_message.set_region_id(new_region_id);
            raft_message.set_to_peer(
                new_region
                    .get_peers()
                    .iter()
                    .find(|p| p.store_id == store_ctx.store_id)
                    .unwrap()
                    .clone(),
            );

            self.split_progress_mut().insert(new_region_id, false);
            let init_info = SplitRegionInitInfo {
                region: new_region,
                parent_region_id: derived_id,
                parent_epoch: derived_epoch.clone(),
                parent_is_leader: self.is_leader(),
                parent_stat: self.peer_stat().clone(),
                approximate_keys: estimated_keys,
                approximate_size: estimated_size,
                locks,
                last_split_region: last_region_id == new_region_id,
            };

            store_ctx
                .router
                .send_control(StoreMsg::PeerCreation(PeerCreation {
                    raft_message: Box::new(raft_message),
                    split_region_info: Box::new(init_info),
                }));
        }
        drop(meta);

        if is_leader {
            self.on_split_region_check_tick();
        }
    }

    pub fn init_split_region<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        init_info: Box<SplitRegionInitInfo>,
    ) {
        let SplitRegionInitInfo {
            parent_region_id,
            parent_epoch,
            region,
            parent_is_leader,
            parent_stat,
            approximate_keys,
            approximate_size,
            locks,
            last_split_region,
        } = Box::into_inner(init_info);

        let mut need_schedule_apply_fsm = false;
        let region_id = region.id;

        let replace = region.get_region_epoch().get_version()
            > self
                .storage()
                .region_state()
                .get_region()
                .get_region_epoch()
                .get_version();

        if !self.storage().is_initialized() || replace {
            let mut wb = store_ctx.engine.log_batch(5);
            write_initial_states(&mut wb, region.clone()).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to save split region {:?}: {:?}",
                    self.logger.list(),
                    region,
                    e
                )
            });

            // todo: need to do asynchronously?
            store_ctx.engine.consume(&mut wb, true).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to consume the write: {:?}",
                    self.logger.list(),
                    e,
                )
            });

            let split_temp_path = store_ctx.tablet_factory.tablet_path_with_prefix(
                SPLIT_PREFIX,
                region_id,
                RAFT_INIT_LOG_INDEX,
            );
            let tablet_path = store_ctx
                .tablet_factory
                .tablet_path(region_id, RAFT_INIT_LOG_INDEX);
            std::fs::rename(&split_temp_path, &tablet_path).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to rename from tablet path {:?} to normal path {:?} :{:?}",
                    self.logger.list(),
                    split_temp_path,
                    tablet_path,
                    e
                )
            });

            let storage = Storage::new(
                region_id,
                store_ctx.store_id,
                store_ctx.engine.clone(),
                store_ctx.read_scheduler.clone(),
                &store_ctx.logger,
            )
            .unwrap_or_else(|e| panic!("fail to create storage: {:?}", e))
            .unwrap();

            let applied_index = storage.apply_state().get_applied_index();
            let peer_id = storage.peer().get_id();
            let raft_cfg = raft_config(peer_id, applied_index, &store_ctx.cfg);

            let mut raft_group = RawNode::new(&raft_cfg, storage, &self.logger).unwrap();
            // If this region has only one peer and I am the one, campaign directly.
            if region.get_peers().len() == 1 {
                raft_group.campaign().unwrap();
                self.set_has_ready();
            }
            self.set_raft_group(raft_group);

            need_schedule_apply_fsm = true;
        } else {
            // todo: when reaching here, it is much complexer.
            unimplemented!();
        }

        let mut meta = store_ctx.store_meta.lock().unwrap();

        info!(
            self.logger,
            "init split region";
            "region_id" => region_id,
            "region" => ?region,
        );

        // todo: GlobalReplicationState

        for p in region.get_peers() {
            self.insert_peer_cache(p.clone());
        }

        // New peer derive write flow from parent region,
        // this will be used by balance write flow.
        self.set_peer_stat(parent_stat);
        let last_compacted_idx = self
            .storage()
            .apply_state()
            .get_truncated_state()
            .get_index()
            + 1;
        self.set_last_compacted_idx(last_compacted_idx);

        let campaigned = self.maybe_campaign(parent_is_leader);
        if campaigned {
            self.set_has_ready();
        }

        if parent_is_leader {
            self.set_approximate_size(approximate_size);
            self.set_approximate_keys(approximate_keys);
            *self.txn_ext().pessimistic_locks.write() = locks;
            // The new peer is likely to become leader, send a heartbeat immediately to
            // reduce client query miss.
            self.heartbeat_pd(store_ctx);
        }

        let mut tablet = store_ctx
            .tablet_factory
            .open_tablet(
                region_id,
                Some(RAFT_INIT_LOG_INDEX),
                OpenOptions::default().set_create(true),
            )
            .unwrap();
        self.tablet_mut().set(tablet);

        meta.tablet_caches.insert(region_id, self.tablet().clone());
        meta.regions.insert(region_id, region.clone());
        let not_exist = meta
            .region_ranges
            .insert(enc_end_key(&region), region_id)
            .is_none();
        assert!(not_exist, "[region {}] should not exist", region_id);
        meta.readers
            .insert(region_id, self.generate_read_delegate());
        meta.region_read_progress
            .insert(region_id, self.read_progress().clone());

        drop(meta);

        if last_split_region {
            // todo: check if the last region needs to split again
        }

        store_ctx.router.force_send(
            parent_region_id,
            PeerMsg::AcrossPeerMsg(AcrossPeerMsg::SplitRegionInitResp(Box::new(
                SplitRegionInitResp {
                    parent_epoch,
                    child_region_id: region_id,
                    result: true,
                },
            ))),
        );

        if need_schedule_apply_fsm {
            self.schedule_apply_fsm(store_ctx);
        }
    }

    pub fn handle_peer_split_response<T>(
        &mut self,
        store_ctx: &mut StoreContext<EK, ER, T>,
        resp: Box<SplitRegionInitResp>,
    ) {
        let SplitRegionInitResp {
            parent_epoch,
            child_region_id,
            result,
        } = Box::into_inner(resp);

        assert_eq!(
            parent_epoch,
            *self
                .storage()
                .region_state()
                .get_region()
                .get_region_epoch()
        );

        let mut split_progress = self.split_progress_mut();
        *split_progress.get_mut(&child_region_id).unwrap() = true;

        if split_progress.values().all(|v| *v) {
            // Split can be finished
            let mut wb = store_ctx.engine.log_batch(10);
            // Persist region state, so the tablet index points to the new tablet
            let state = self.storage().region_state();
            wb.put_region_state(self.region_id(), state)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to update region {:?}: {:?}",
                        self.logger.list(),
                        state,
                        e
                    )
                });
            // todo: need to do asynchronously?
            store_ctx.engine.consume(&mut wb, true).unwrap();

            self.split_progress_mut().clear();
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    pub fn apply_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        info!(
            self.logger,
            "split is deprecated, redirect to use batch split";
        );
        let split = req.get_split().to_owned();
        let mut admin_req = AdminRequest::default();
        admin_req
            .mut_splits()
            .set_right_derive(split.get_right_derive());
        admin_req.mut_splits().mut_requests().push(split);
        // This method is executed only when there are unapplied entries after being
        // restarted. So there will be no callback, it's OK to return a response
        // that does not matched with its request.
        self.apply_batch_split(req, log_index)
    }

    pub fn apply_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, AdminCmdResult)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let region = self.region_state().get_region();
        let region_id = region.get_id();
        validate_batch_split(req, self.region_state().get_region())?;

        let mut boundaries: Vec<&[u8]> = Vec::default();
        boundaries.push(self.region_state().get_region().get_start_key());
        for req in req.get_splits().get_requests() {
            boundaries.push(req.get_split_key());
        }
        boundaries.push(self.region_state().get_region().get_end_key());

        info!(
            self.logger,
            "split region";
            "region" => ?region,
            "boundaries" => %KeysInfoFormatter(boundaries.iter()),
        );

        let split_reqs = req.get_splits();
        let new_region_cnt = split_reqs.get_requests().len();
        let new_version = region.get_region_epoch().get_version() + new_region_cnt as u64;

        let mut derived_req = SplitRequest::default();
        derived_req.new_region_id = region.id;
        let derived_req = &[derived_req];

        let right_derive = split_reqs.get_right_derive();
        let reqs = if right_derive {
            split_reqs.get_requests().iter().chain(derived_req)
        } else {
            derived_req.iter().chain(split_reqs.get_requests())
        };

        let regions: Vec<_> = boundaries
            .array_windows::<2>()
            .zip(reqs)
            .map(|([start_key, end_key], req)| {
                let mut new_region = Region::default();
                new_region.set_id(req.get_new_region_id());
                new_region.set_region_epoch(region.get_region_epoch().to_owned());
                new_region.mut_region_epoch().set_version(new_version);
                new_region.set_start_key(start_key.to_vec());
                new_region.set_end_key(end_key.to_vec());
                new_region.set_peers(region.get_peers().to_vec().into());
                // If the `req` is the `derived_req`, the peers are already set correctly and
                // the following loop will not be executed due to the empty `new_peer_ids` in
                // the `derived_req`
                for (peer, peer_id) in new_region
                    .mut_peers()
                    .iter_mut()
                    .zip(req.get_new_peer_ids())
                {
                    peer.set_id(*peer_id);
                }
                new_region
            })
            .collect();

        let derived_index = if right_derive { regions.len() - 1 } else { 0 };

        // We will create checkpoint of the current tablet for both derived region and
        // split regions. Before the creation, we should flush the writes and remove the
        // write batch
        self.flush();

        // todo(SpadeA): Here: we use a temporary solution that we use checkpoint API to
        // clone new tablets. It may cause large jitter as we need to flush the
        // memtable. And more what is more important is that after removing WAL, the API
        // will never flush.
        // We will freeze the memtable rather than flush it in the following PR.
        let tablet = self.tablet().clone();
        let mut checkpointer = tablet.new_checkpointer().unwrap_or_else(|e| {
            panic!(
                "{:?} fails to create checkpoint object: {:?}",
                self.logger.list(),
                e
            )
        });

        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }

            let split_temp_path = self.tablet_factory().tablet_path_with_prefix(
                SPLIT_PREFIX,
                new_region_id,
                RAFT_INIT_LOG_INDEX,
            );
            checkpointer
                .create_at(&split_temp_path, None, 0)
                .unwrap_or_else(|e| {
                    panic!(
                        "{:?} fails to create checkpoint with path {:?}: {:?}",
                        self.logger.list(),
                        split_temp_path,
                        e
                    )
                });
        }

        let derived_path = self.tablet_factory().tablet_path(region_id, log_index);
        checkpointer
            .create_at(&derived_path, None, 0)
            .unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to create checkpoint with path {:?}: {:?}",
                    self.logger.list(),
                    derived_path,
                    e
                )
            });
        let tablet = self
            .tablet_factory()
            .open_tablet(region_id, Some(log_index), OpenOptions::default())
            .unwrap();
        // Remove the old write batch.
        self.write_batch_mut().take();
        self.publish_tablet(tablet);

        self.region_state_mut()
            .set_region(regions[derived_index].clone());
        self.region_state_mut().set_tablet_index(log_index);

        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        Ok((
            resp,
            AdminCmdResult::SplitRegion(SplitResult {
                regions,
                derived_index,
                tablet_index: log_index,
            }),
        ))
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_across_peer_msg(&mut self, msg: AcrossPeerMsg) {
        match msg {
            AcrossPeerMsg::SplitRegionInit(init_info) => {
                self.fsm
                    .peer_mut()
                    .init_split_region(self.store_ctx, init_info);
            }
            AcrossPeerMsg::SplitRegionInitResp(resp) => {
                self.fsm
                    .peer_mut()
                    .handle_peer_split_response(self.store_ctx, resp);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    };

    use collections::HashMap;
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactoryV2,
        raft,
    };
    use engine_traits::{CfOptionsExt, Peekable, WriteBatch, ALL_CFS};
    use futures::channel::mpsc::unbounded;
    use kvproto::{
        metapb::RegionEpoch,
        raft_cmdpb::{AdminCmdType, BatchSplitRequest, PutRequest, RaftCmdResponse, SplitRequest},
        raft_serverpb::{PeerState, RaftApplyState, RegionLocalState},
    };
    use raftstore::store::{cmd_resp::new_error, Config, ReadRunner};
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        codec::bytes::encode_bytes,
        config::VersionTrack,
        store::{new_learner_peer, new_peer},
        worker::{dummy_future_scheduler, dummy_scheduler, FutureScheduler, Scheduler, Worker},
    };

    use super::*;
    use crate::{
        fsm::{ApplyFsm, ApplyResReporter},
        raft::Apply,
        tablet::CachedTablet,
    };

    struct MockReporter {
        sender: Sender<ApplyRes>,
    }

    impl MockReporter {
        fn new() -> (Self, Receiver<ApplyRes>) {
            let (tx, rx) = channel();
            (MockReporter { sender: tx }, rx)
        }
    }

    impl ApplyResReporter for MockReporter {
        fn report(&self, apply_res: ApplyRes) {
            let _ = self.sender.send(apply_res);
        }
    }

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    fn assert_split(
        apply: &mut Apply<engine_test::kv::KvTestEngine, MockReporter>,
        factory: &Arc<TestTabletFactoryV2>,
        parent_id: u64,
        right_derived: bool,
        new_region_ids: Vec<u64>,
        split_keys: Vec<Vec<u8>>,
        children_peers: Vec<Vec<u64>>,
        log_index: u64,
        region_boundries: Vec<(Vec<u8>, Vec<u8>)>,
        expected_region_epoch: RegionEpoch,
        expected_derived_index: usize,
    ) {
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(right_derived);

        for ((new_region_id, children), split_key) in new_region_ids
            .into_iter()
            .zip(children_peers.clone())
            .zip(split_keys)
        {
            splits
                .mut_requests()
                .push(new_split_req(&split_key, new_region_id, children));
        }

        let mut req = AdminRequest::default();
        req.set_splits(splits);

        // Exec batch split
        let (resp, apply_res) = apply.apply_batch_split(&req, log_index).unwrap();

        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == region_boundries.len());

        let mut child_idx = 0;
        for (i, region) in regions.iter().enumerate() {
            assert_eq!(region.get_start_key().to_vec(), region_boundries[i].0);
            assert_eq!(region.get_end_key().to_vec(), region_boundries[i].1);
            assert_eq!(*region.get_region_epoch(), expected_region_epoch);

            if region.id == parent_id {
                let state = apply.region_state();
                assert_eq!(state.tablet_index, log_index);
                assert_eq!(state.get_region(), region);
                let tablet_path = factory.tablet_path(region.id, log_index);
                assert!(factory.exists_raw(&tablet_path));

                match apply_res {
                    AdminCmdResult::SplitRegion(SplitResult {
                        derived_index,
                        tablet_index,
                        ..
                    }) => {
                        assert_eq!(expected_derived_index, derived_index);
                        assert_eq!(tablet_index, log_index);
                    }
                    _ => panic!(),
                }
            } else {
                assert_eq! {
                    region.get_peers().iter().map(|peer| peer.id).collect::<Vec<_>>(),
                    children_peers[child_idx]
                }
                child_idx += 1;

                let tablet_path =
                    factory.tablet_path_with_prefix(SPLIT_PREFIX, region.id, RAFT_INIT_LOG_INDEX);
                assert!(factory.exists_raw(&tablet_path));
            }
        }
    }

    #[test]
    fn test_split() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k10".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path.path(),
            DbOptions::default(),
            cf_opts,
        ));

        let tablet = factory
            .open_tablet(
                region.id,
                Some(5),
                OpenOptions::default().set_create_new(true),
            )
            .unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);

        let (read_scheduler, rx) = dummy_scheduler();
        let (reporter, _) = MockReporter::new();
        let mut apply = Apply::new(
            region
                .get_peers()
                .iter()
                .find(|p| p.store_id == store_id)
                .unwrap()
                .clone(),
            region_state,
            reporter,
            CachedTablet::new(Some(tablet)),
            factory.clone(),
            read_scheduler,
            logger.clone(),
        );

        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits.mut_requests().push(new_split_req(b"k1", 1, vec![]));
        let mut req = AdminRequest::default();
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // 3 followers are required.
        assert!(err.to_string().contains("invalid new peer id count"));

        splits.mut_requests().clear();
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // Empty requests should be rejected.
        assert!(err.to_string().contains("missing split requests"));

        splits
            .mut_requests()
            .push(new_split_req(b"k11", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let resp = new_error(apply.apply_batch_split(&req, 0).unwrap_err());
        // Out of range keys should be rejected.
        assert!(
            resp.get_header().get_error().has_key_not_in_region(),
            "{:?}",
            resp
        );

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // Empty key will not in any region exclusively.
        assert!(err.to_string().contains("missing split key"), "{:?}", err);

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // keys should be in ascend order.
        assert!(
            err.to_string().contains("invalid split request"),
            "{:?}",
            err
        );

        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k1", 1, vec![11, 12, 13]));
        splits
            .mut_requests()
            .push(new_split_req(b"k2", 1, vec![11, 12]));
        req.set_splits(splits.clone());
        let err = apply.apply_batch_split(&req, 0).unwrap_err();
        // All requests should be checked.
        assert!(err.to_string().contains("id count"), "{:?}", err);

        let cases = vec![
            // region 1["", "k10"]
            // After split: region  1 ["", "k09"],
            //              region 10 ["k09", "k10"]
            (
                1,
                false,
                vec![10],
                vec![b"k09".to_vec()],
                vec![vec![11, 12, 13]],
                10,
                vec![
                    (b"".to_vec(), b"k09".to_vec()),
                    (b"k09".to_vec(), b"k10".to_vec()),
                ],
                4,
                0,
            ),
            // region 1 ["", "k09"]
            // After split: region 20 ["", "k01"],
            //               region 1 ["k01", "k09"]
            (
                1,
                true,
                vec![20],
                vec![b"k01".to_vec()],
                vec![vec![21, 22, 23]],
                20,
                vec![
                    (b"".to_vec(), b"k01".to_vec()),
                    (b"k01".to_vec(), b"k09".to_vec()),
                ],
                5,
                1,
            ),
            // region 1 ["k01", "k09"]
            // After split: region 30 ["k01", "k02"],
            //              region 40 ["k02", "k03"],
            //              region  1 ["k03", "k09"]
            (
                1,
                true,
                vec![30, 40],
                vec![b"k02".to_vec(), b"k03".to_vec()],
                vec![vec![31, 32, 33], vec![41, 42, 43]],
                30,
                vec![
                    (b"k01".to_vec(), b"k02".to_vec()),
                    (b"k02".to_vec(), b"k03".to_vec()),
                    (b"k03".to_vec(), b"k09".to_vec()),
                ],
                7,
                2,
            ),
            // region 1 ["k03", "k09"]
            // After split: region  1 ["k03", "k07"],
            //              region 50 ["k07", "k08"],
            //              region 60 ["k08", "k09"]
            (
                1,
                false,
                vec![50, 60],
                vec![b"k07".to_vec(), b"k08".to_vec()],
                vec![vec![51, 52, 53], vec![61, 62, 63]],
                40,
                vec![
                    (b"k03".to_vec(), b"k07".to_vec()),
                    (b"k07".to_vec(), b"k08".to_vec()),
                    (b"k08".to_vec(), b"k09".to_vec()),
                ],
                9,
                0,
            ),
        ];

        for (
            parent_id,
            right_derive,
            new_region_ids,
            split_keys,
            children_peers,
            log_index,
            region_boundries,
            version,
            expected_derived_index,
        ) in cases
        {
            let mut expected_epoch = RegionEpoch::new();
            expected_epoch.set_version(version);

            assert_split(
                &mut apply,
                &factory,
                parent_id,
                right_derive,
                new_region_ids,
                split_keys,
                children_peers,
                log_index,
                region_boundries,
                expected_epoch,
                expected_derived_index,
            );
        }

        // Split will create checkpoint tablet, so if there are some writes before
        // split, they should be flushed immediately.
        apply.apply_put(CF_DEFAULT, b"k04", b"v4").unwrap();
        assert!(!WriteBatch::is_empty(
            apply.write_batch_mut().as_ref().unwrap()
        ));
        splits.mut_requests().clear();
        splits
            .mut_requests()
            .push(new_split_req(b"k05", 70, vec![71, 72, 73]));
        req.set_splits(splits);
        apply.apply_batch_split(&req, 50).unwrap();
        assert!(apply.write_batch_mut().is_none());
        assert_eq!(apply.tablet().get_value(b"k04").unwrap().unwrap(), b"v4");
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains the peer implementation for batch system.

use std::borrow::Cow;

use batch_system::{BasicMailbox, Fsm};
use crossbeam::channel::TryRecvError;
use engine_traits::{KvEngine, RaftEngine, TabletFactory};
use raftstore::store::{Config, LocksStatus, Transport};
use slog::{debug, error, info, trace, warn, Logger};
use tikv_util::{
    is_zero_duration,
    mpsc::{self, LooseBoundedSender, Receiver},
    time::{duration_to_sec, Instant},
    yatp_pool::FuturePool,
};

use super::ApplyFsm;
use crate::{
    batch::StoreContext,
    raft::{Peer, Storage},
    router::{PeerMsg, PeerTick},
    Result,
};

pub type SenderFsmPair<EK, ER> = (LooseBoundedSender<PeerMsg>, Box<PeerFsm<EK, ER>>);

pub struct PeerFsm<EK: KvEngine, ER: RaftEngine> {
    peer: Peer<EK, ER>,
    mailbox: Option<BasicMailbox<PeerFsm<EK, ER>>>,
    receiver: Receiver<PeerMsg>,
    /// A registry for all scheduled ticks. This can avoid scheduling ticks
    /// twice accidentally.
    tick_registry: u16,
    is_stopped: bool,
    reactivate_memory_lock_ticks: usize,
}

impl<EK: KvEngine, ER: RaftEngine> PeerFsm<EK, ER> {
    pub fn new(
        cfg: &Config,
        tablet_factory: &dyn TabletFactory<EK>,
        storage: Storage<EK, ER>,
    ) -> Result<SenderFsmPair<EK, ER>> {
        let peer = Peer::new(cfg, tablet_factory, storage)?;
        info!(peer.logger, "create peer");
        let (tx, rx) = mpsc::loose_bounded(cfg.notify_capacity);
        let fsm = Box::new(PeerFsm {
            peer,
            mailbox: None,
            receiver: rx,
            tick_registry: 0,
            is_stopped: false,
            reactivate_memory_lock_ticks: 0,
        });
        Ok((tx, fsm))
    }

    #[inline]
    pub fn peer(&self) -> &Peer<EK, ER> {
        &self.peer
    }

    #[inline]
    pub fn peer_mut(&mut self) -> &mut Peer<EK, ER> {
        &mut self.peer
    }

    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.peer.logger
    }

    /// Fetches messages to `peer_msg_buf`. It will stop when the buffer
    /// capacity is reached or there is no more pending messages.
    ///
    /// Returns how many messages are fetched.
    pub fn recv(&mut self, peer_msg_buf: &mut Vec<PeerMsg>, batch_size: usize) -> usize {
        let l = peer_msg_buf.len();
        for i in l..batch_size {
            match self.receiver.try_recv() {
                Ok(msg) => peer_msg_buf.push(msg),
                Err(e) => {
                    if let TryRecvError::Disconnected = e {
                        self.is_stopped = true;
                    }
                    return i - l;
                }
            }
        }
        batch_size - l
    }
}

impl<EK: KvEngine, ER: RaftEngine> Fsm for PeerFsm<EK, ER> {
    type Message = PeerMsg;

    #[inline]
    fn is_stopped(&self) -> bool {
        self.is_stopped
    }

    /// Set a mailbox to FSM, which should be used to send message to itself.
    fn set_mailbox(&mut self, mailbox: Cow<'_, BasicMailbox<Self>>)
    where
        Self: Sized,
    {
        self.mailbox = Some(mailbox.into_owned());
    }

    /// Take the mailbox from FSM. Implementation should ensure there will be
    /// no reference to mailbox after calling this method.
    fn take_mailbox(&mut self) -> Option<BasicMailbox<Self>>
    where
        Self: Sized,
    {
        self.mailbox.take()
    }
}

pub struct PeerFsmDelegate<'a, EK: KvEngine, ER: RaftEngine, T> {
    pub fsm: &'a mut PeerFsm<EK, ER>,
    pub store_ctx: &'a mut StoreContext<EK, ER, T>,
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn new(fsm: &'a mut PeerFsm<EK, ER>, store_ctx: &'a mut StoreContext<EK, ER, T>) -> Self {
        Self { fsm, store_ctx }
    }

    fn schedule_pending_ticks(&mut self) {
        let pending_ticks = self.fsm.peer.take_pending_ticks();
        for tick in pending_ticks {
            if tick == PeerTick::ReactivateMemoryLock {
                self.fsm.reactivate_memory_lock_ticks = 0;
            }
            self.schedule_tick(tick);
        }
    }

    pub fn schedule_tick(&mut self, tick: PeerTick) {
        assert!(PeerTick::VARIANT_COUNT <= u16::BITS as usize);
        let idx = tick as usize;
        let key = 1u16 << (idx as u16);
        if self.fsm.tick_registry & key != 0 {
            return;
        }
        if is_zero_duration(&self.store_ctx.tick_batch[idx].wait_duration) {
            return;
        }
        trace!(
            self.fsm.logger(),
            "schedule tick";
            "tick" => ?tick,
            "timeout" => ?self.store_ctx.tick_batch[idx].wait_duration,
        );

        let region_id = self.fsm.peer.region_id();
        let mb = match self.store_ctx.router.mailbox(region_id) {
            Some(mb) => mb,
            None => {
                error!(
                    self.fsm.logger(),
                    "failed to get mailbox";
                    "tick" => ?tick,
                );
                return;
            }
        };
        self.fsm.tick_registry |= key;
        let logger = self.fsm.logger().clone();
        // TODO: perhaps following allocation can be removed.
        let cb = Box::new(move || {
            // This can happen only when the peer is about to be destroyed
            // or the node is shutting down. So it's OK to not to clean up
            // registry.
            if let Err(e) = mb.force_send(PeerMsg::Tick(tick)) {
                debug!(
                    logger,
                    "failed to schedule peer tick";
                    "tick" => ?tick,
                    "err" => %e,
                );
            }
        });
        self.store_ctx.tick_batch[idx].ticks.push(cb);
    }

    fn on_start(&mut self) {
        self.schedule_tick(PeerTick::Raft);
        self.schedule_tick(PeerTick::SplitRegionCheck);
        self.schedule_tick(PeerTick::PdHeartbeat);
        if self.fsm.peer.storage().is_initialized() {
            self.fsm.peer.schedule_apply_fsm(self.store_ctx);
        }
    }

    #[inline]
    fn on_receive_command(&self, send_time: Instant) {
        self.store_ctx
            .raft_metrics
            .propose_wait_time
            .observe(duration_to_sec(send_time.saturating_elapsed()));
    }

    fn on_tick(&mut self, tick: PeerTick) {
        let key = 1u16 << (tick as u16);
        if self.fsm.tick_registry & key == key {
            self.fsm.tick_registry ^= key;
        }
        macro_rules! unimp {
            ($tick:expr) => {
                error!(self.fsm.logger(), "unsupported tick"; "tick" => ?$tick)
            };
        }
        match tick {
            PeerTick::Raft => self.on_raft_tick(),
            PeerTick::PdHeartbeat => self.on_pd_heartbeat(),
            PeerTick::RaftLogGc => self.on_raft_log_gc(),
            PeerTick::EntryCacheEvict => self.on_entry_cache_evict(),
            PeerTick::SplitRegionCheck => self.on_split_region_check(),
            PeerTick::CheckMerge => unimp!(tick),
            PeerTick::CheckPeerStaleState => unimp!(tick),
            PeerTick::CheckLeaderLease => unimp!(tick),
            PeerTick::ReactivateMemoryLock => unimp!(tick),
            PeerTick::ReportBuckets => unimp!(tick),
            PeerTick::CheckLongUncommitted => unimp!(tick),
        }
    }

    fn on_raft_log_gc(&mut self) {
        use kvproto::{
            metapb,
            raft_cmdpb::{AdminCmdType, AdminRequest, RaftCmdRequest},
        };
        use raftstore::store::{fsm::new_admin_request, needs_evict_entry_cache};
        use tikv_util::sys::memory_usage_reaches_high_water;

        info!(self.fsm.logger(), "on_raft_log_gc");

        if !self.fsm.peer.is_leader() {
            // `compact_cache_to` is called when apply, there is no need to call
            // `compact_to` here, snapshot generating has already been cancelled
            // when the role becomes follower.
            return;
        }
        self.schedule_tick(PeerTick::RaftLogGc);

        // As leader, we would not keep caches for the peers that didn't response
        // heartbeat in the last few seconds. That happens probably because
        // another TiKV is down. In this case if we do not clean up the cache,
        // it may keep growing.
        let drop_cache_duration = self.store_ctx.cfg.raft_heartbeat_interval()
            + self.store_ctx.cfg.raft_entry_cache_life_time.0;
        let cache_alive_limit = std::time::Instant::now() - drop_cache_duration;

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
        // `alive_cache_idx` is the smallest `replicated_index` of healthy up nodes.
        // `alive_cache_idx` is only used to gc cache.
        let applied_idx = self.fsm.peer.entry_storage().applied_index();
        let truncated_idx = self.fsm.peer.entry_storage().truncated_index();
        let first_idx = self.fsm.peer.entry_storage().first_index();
        let last_idx = self.fsm.peer.entry_storage().last_index();

        let (mut replicated_idx, mut alive_cache_idx) = (last_idx, last_idx);
        for (peer_id, p) in self.fsm.peer.raft_group().raft.prs().iter() {
            if replicated_idx > p.matched {
                replicated_idx = p.matched;
            }
            if let Some(last_heartbeat) = self.fsm.peer.peer_heartbeats.get(peer_id) {
                if *last_heartbeat > cache_alive_limit {
                    if alive_cache_idx > p.matched && p.matched >= truncated_idx {
                        alive_cache_idx = p.matched;
                    } else if p.matched == 0 {
                        // the new peer is still applying snapshot, do not compact cache now
                        alive_cache_idx = 0;
                    }
                }
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
        }

        // leader may call `get_term()` on the latest replicated index, so compact
        // entries before `alive_cache_idx` instead of `alive_cache_idx + 1`.
        self.fsm
            .peer
            .entry_storage_mut()
            .compact_entry_cache(std::cmp::min(alive_cache_idx, applied_idx + 1));
        if needs_evict_entry_cache(self.store_ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm.peer.entry_storage_mut().evict_entry_cache(true);
            if !self.fsm.peer.entry_storage().is_entry_cache_empty() {
                self.schedule_tick(PeerTick::EntryCacheEvict);
            }
        }

        let mut compact_idx = if (applied_idx > first_idx
            && applied_idx - first_idx >= self.store_ctx.cfg.raft_log_gc_count_limit())
            || (self.fsm.peer.raft_log_size_hint >= self.store_ctx.cfg.raft_log_gc_size_limit().0)
        {
            std::cmp::max(first_idx + (last_idx - first_idx) / 2, replicated_idx)
        } else if replicated_idx < first_idx || last_idx - first_idx < 3 {
            return;
        } else if replicated_idx - first_idx < self.store_ctx.cfg.raft_log_gc_threshold
            && self.fsm.peer.skip_gc_raft_log_ticks < self.store_ctx.cfg.raft_log_reserve_max_ticks
        {
            self.fsm.peer.skip_gc_raft_log_ticks += 1;
            return;
        } else {
            replicated_idx
        };
        assert!(compact_idx >= first_idx);
        // Have no idea why subtract 1 here, but original code did this by magic.
        compact_idx -= 1;
        if compact_idx < first_idx {
            return;
        }

        // Create a compact log request and notify directly.
        let region_id = self.fsm.peer.region().get_id();
        let peer = self.fsm.peer.peer().clone();
        // TODO
        let term = self
            .fsm
            .peer
            .raft_group()
            .raft
            .raft_log
            .term(compact_idx)
            .unwrap();
        fn new_compact_log_request(
            region_id: u64,
            peer: metapb::Peer,
            compact_index: u64,
            compact_term: u64,
        ) -> RaftCmdRequest {
            let mut request = new_admin_request(region_id, peer);

            let mut admin = AdminRequest::default();
            admin.set_cmd_type(AdminCmdType::CompactLog);
            admin.mut_compact_log().set_compact_index(compact_index);
            admin.mut_compact_log().set_compact_term(compact_term);
            request.set_admin_request(admin);
            request
        }
        let request = new_compact_log_request(region_id, peer, compact_idx, term);
        self.fsm.peer.propose_command(self.store_ctx, request);
    }

    fn on_entry_cache_evict(&mut self) {
        use raftstore::store::needs_evict_entry_cache;
        use tikv_util::sys::memory_usage_reaches_high_water;

        info!(self.fsm.logger(), "on_entry_cache_evict");

        if needs_evict_entry_cache(self.store_ctx.cfg.evict_cache_on_memory_ratio) {
            self.fsm.peer.entry_storage_mut().evict_entry_cache(true);
        }
        let mut _usage = 0;
        if memory_usage_reaches_high_water(&mut _usage)
            && !self.fsm.peer.entry_storage().is_entry_cache_empty()
        {
            self.schedule_tick(PeerTick::EntryCacheEvict);
        }
    }

    pub fn on_msgs(&mut self, peer_msgs_buf: &mut Vec<PeerMsg>) {
        for msg in peer_msgs_buf.drain(..) {
            match msg {
                PeerMsg::RaftMessage(msg) => {
                    self.fsm.peer.on_raft_message(self.store_ctx, msg);
                    if self.fsm.peer.need_schedule_tick() {
                        self.fsm.peer.reset_need_schedule_tick();
                        self.schedule_pending_ticks();
                    }
                }
                PeerMsg::RaftQuery(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.on_query(cmd.request, cmd.ch)
                }
                PeerMsg::RaftCommand(cmd) => {
                    self.on_receive_command(cmd.send_time);
                    self.on_command(cmd.request, cmd.ch)
                }
                PeerMsg::Tick(tick) => self.on_tick(tick),
                PeerMsg::ApplyRes(res) => {
                    self.fsm.peer.on_apply_res(self.store_ctx, res);
                    if !self.fsm.peer.may_skip_split_check() {
                        self.schedule_tick(PeerTick::SplitRegionCheck);
                    }
                }
                PeerMsg::SplitInit(msg) => self.fsm.peer.on_split_init(self.store_ctx, msg),
                PeerMsg::Start => self.on_start(),
                PeerMsg::Noop => unimplemented!(),
                PeerMsg::Persisted {
                    peer_id,
                    ready_number,
                    need_scheduled,
                } => self.fsm.peer_mut().on_persisted(
                    self.store_ctx,
                    peer_id,
                    ready_number,
                    need_scheduled,
                ),
                PeerMsg::LogsFetched(fetched_logs) => {
                    self.fsm.peer_mut().on_logs_fetched(fetched_logs)
                }
                PeerMsg::SnapshotGenerated(snap_res) => {
                    self.fsm.peer_mut().on_snapshot_generated(snap_res)
                }
                PeerMsg::QueryDebugInfo(ch) => self.fsm.peer_mut().on_query_debug_info(ch),
                PeerMsg::SplitRegion(sr) => self.fsm.peer_mut().on_prepare_split_region(
                    self.store_ctx,
                    sr.region_epoch,
                    sr.split_keys,
                    Some(sr.ch),
                    &sr.source,
                ),
                #[cfg(feature = "testexport")]
                PeerMsg::WaitFlush(ch) => self.fsm.peer_mut().on_wait_flush(ch),
            }
        }
        // TODO: instead of propose pending commands immediately, we should use timeout.
        self.fsm.peer.propose_pending_writes(self.store_ctx);
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_reactivate_memory_lock_tick(&mut self) {
        let mut pessimistic_locks = self.fsm.peer.txn_ext().pessimistic_locks.write();

        // If it is not leader, we needn't reactivate by tick. In-memory pessimistic
        // lock will be enabled when this region becomes leader again.
        // And this tick is currently only used for the leader transfer failure case.
        if !self.fsm.peer().is_leader()
            || pessimistic_locks.status != LocksStatus::TransferringLeader
        {
            return;
        }

        self.fsm.reactivate_memory_lock_ticks += 1;
        let transferring_leader = self.fsm.peer.raft_group().raft.lead_transferee.is_some();
        // `lead_transferee` is not set immediately after the lock status changes. So,
        // we need the tick count condition to avoid reactivating too early.
        if !transferring_leader
            && self.fsm.reactivate_memory_lock_ticks
                >= self.store_ctx.cfg.reactive_memory_lock_timeout_tick
        {
            pessimistic_locks.status = LocksStatus::Normal;
            self.fsm.reactivate_memory_lock_ticks = 0;
        } else {
            drop(pessimistic_locks);
            self.register_reactivate_memory_lock_tick();
        }
    }

    pub fn register_reactivate_memory_lock_tick(&mut self) {
        self.fsm.reactivate_memory_lock_ticks = 0;
        self.schedule_tick(PeerTick::ReactivateMemoryLock)
    }
}

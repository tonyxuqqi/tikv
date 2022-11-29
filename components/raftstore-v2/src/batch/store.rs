// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use batch_system::{
    BasicMailbox, BatchRouter, BatchSystem, HandleResult, HandlerBuilder, PollHandler,
};
use causal_ts::CausalTsProviderImpl;
use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use crossbeam::channel::{Sender, TrySendError};
use engine_traits::{Engines, KvEngine, RaftEngine, TabletFactory};
use file_system::{set_io_type, IoType};
use futures::{compat::Future01CompatExt, FutureExt};
use kvproto::{
    disk_usage::DiskUsage,
    metapb::Store,
    raft_serverpb::{PeerState, RaftMessage},
};
use pd_client::PdClient;
use raft::{StateRole, INVALID_ID};
use raftstore::{
    coprocessor::RegionChangeEvent,
    store::{
        fsm::store::PeerTickBatch, local_metrics::RaftMetrics, util::LockManagerNotifier, Config,
        ReadRunner, ReadTask, StoreWriters, TabletSnapManager, Transport, WriteSenders,
    },
    DiscardReason,
};
use slog::Logger;
use tikv_util::{
    box_err,
    config::{Tracker, VersionTrack},
    defer,
    future::poll_future_notify,
    sys::SysQuota,
    time::Instant as TiInstant,
    timer::SteadyTimer,
    worker::{Scheduler, Worker},
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
    Either,
};
use time::Timespec;

use crate::{
    fsm::{PeerFsm, PeerFsmDelegate, SenderFsmPair, StoreFsm, StoreFsmDelegate, StoreMeta},
    raft::Storage,
    router::{PeerMsg, PeerTick, StoreMsg},
    worker::{PdRunner, PdTask, RaftLogGcRunner, RaftLogGcTask},
    Error, Result,
};

const ENTRY_CACHE_EVICT_TICK_DURATION: Duration = Duration::from_secs(1);

/// A per-thread context shared by the [`StoreFsm`] and multiple [`PeerFsm`]s.
pub struct StoreContext<EK: KvEngine, ER: RaftEngine, T> {
    /// A logger without any KV. It's clean for creating new PeerFSM.
    pub logger: Logger,
    /// The transport for sending messages to peers on other stores.
    pub trans: T,
    pub current_time: Option<Timespec>,
    pub has_ready: bool,
    pub raft_metrics: RaftMetrics,
    /// The latest configuration.
    pub cfg: Config,
    pub router: StoreRouter<EK, ER>,
    /// The tick batch for delay ticking. It will be flushed at the end of every
    /// round.
    pub tick_batch: Vec<PeerTickBatch>,
    /// The precise timer for scheduling tick.
    pub timer: SteadyTimer,
    pub write_senders: WriteSenders<EK, ER>,
    /// store meta
    pub store_meta: Arc<Mutex<StoreMeta<EK>>>,
    pub engine: ER,
    pub tablet_factory: Arc<dyn TabletFactory<EK>>,
    pub apply_pool: FuturePool,
    pub read_scheduler: Scheduler<ReadTask<EK>>,
    pub pd_scheduler: Scheduler<PdTask>,
    pub raft_log_gc_scheduler: Scheduler<RaftLogGcTask>,
    pub snap_mgr: TabletSnapManager,

    /// Disk usage for the store itself.
    pub self_disk_usage: DiskUsage,

    pub lock_manager_observer: Arc<dyn LockManagerNotifier>,
}

impl<EK: KvEngine, ER: RaftEngine, T> StoreContext<EK, ER, T> {
    pub fn update_ticks_timeout(&mut self) {
        self.tick_batch[PeerTick::Raft as usize].wait_duration = self.cfg.raft_base_tick_interval.0;
        self.tick_batch[PeerTick::RaftLogGc as usize].wait_duration =
            self.cfg.raft_log_gc_tick_interval.0;
        self.tick_batch[PeerTick::EntryCacheEvict as usize].wait_duration =
            ENTRY_CACHE_EVICT_TICK_DURATION;
        self.tick_batch[PeerTick::PdHeartbeat as usize].wait_duration =
            self.cfg.pd_heartbeat_tick_interval.0;
        self.tick_batch[PeerTick::SplitRegionCheck as usize].wait_duration =
            self.cfg.split_region_check_tick_interval.0;
        self.tick_batch[PeerTick::CheckPeerStaleState as usize].wait_duration =
            self.cfg.peer_stale_state_check_interval.0;
        self.tick_batch[PeerTick::CheckMerge as usize].wait_duration =
            self.cfg.merge_check_tick_interval.0;
        self.tick_batch[PeerTick::CheckLeaderLease as usize].wait_duration =
            self.cfg.check_leader_lease_interval.0;
        self.tick_batch[PeerTick::ReactivateMemoryLock as usize].wait_duration =
            self.cfg.reactive_memory_lock_tick_interval.0;
        self.tick_batch[PeerTick::ReportBuckets as usize].wait_duration =
            self.cfg.report_region_buckets_tick_interval.0;
        self.tick_batch[PeerTick::CheckLongUncommitted as usize].wait_duration =
            self.cfg.check_long_uncommitted_interval.0;
    }
}

/// A [`PollHandler`] that handles updates of [`StoreFsm`]s and [`PeerFsm`]s.
///
/// It is responsible for:
///
/// - Keeping the local [`StoreContext`] up-to-date.
/// - Receiving and sending messages in and out of these FSMs.
struct StorePoller<EK: KvEngine, ER: RaftEngine, T> {
    poll_ctx: StoreContext<EK, ER, T>,
    cfg_tracker: Tracker<Config>,
    /// Buffers to hold in-coming messages.
    store_msg_buf: Vec<StoreMsg>,
    peer_msg_buf: Vec<PeerMsg>,
    /// These fields controls the timing of flushing messages generated by
    /// FSMs.
    last_flush_time: TiInstant,
    need_flush_events: bool,
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePoller<EK, ER, T> {
    pub fn new(poll_ctx: StoreContext<EK, ER, T>, cfg_tracker: Tracker<Config>) -> Self {
        Self {
            poll_ctx,
            cfg_tracker,
            store_msg_buf: Vec::new(),
            peer_msg_buf: Vec::new(),
            last_flush_time: TiInstant::now(),
            need_flush_events: false,
        }
    }

    /// Updates the internal buffer to match the latest configuration.
    fn apply_buf_capacity(&mut self) {
        let new_cap = self.messages_per_tick();
        tikv_util::set_vec_capacity(&mut self.store_msg_buf, new_cap);
        tikv_util::set_vec_capacity(&mut self.peer_msg_buf, new_cap);
    }

    #[inline]
    fn messages_per_tick(&self) -> usize {
        self.poll_ctx.cfg.messages_per_tick
    }

    fn flush_events(&mut self) {
        self.schedule_ticks();
    }

    fn schedule_ticks(&mut self) {
        assert_eq!(PeerTick::all_ticks().len(), self.poll_ctx.tick_batch.len());
        for batch in &mut self.poll_ctx.tick_batch {
            batch.schedule(&self.poll_ctx.timer);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine, T: Transport + 'static> PollHandler<PeerFsm<EK, ER>, StoreFsm>
    for StorePoller<EK, ER, T>
{
    fn begin<F>(&mut self, _batch_size: usize, update_cfg: F)
    where
        for<'a> F: FnOnce(&'a batch_system::Config),
    {
        if self.store_msg_buf.capacity() == 0 || self.peer_msg_buf.capacity() == 0 {
            self.apply_buf_capacity();
        }
        // Apply configuration changes.
        if let Some(cfg) = self.cfg_tracker.any_new().map(|c| c.clone()) {
            let last_messages_per_tick = self.messages_per_tick();
            self.poll_ctx.cfg = cfg;
            if self.poll_ctx.cfg.messages_per_tick != last_messages_per_tick {
                self.apply_buf_capacity();
            }
            update_cfg(&self.poll_ctx.cfg.store_batch_system);
        }
    }

    fn handle_control(&mut self, fsm: &mut StoreFsm) -> Option<usize> {
        debug_assert!(self.store_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.store_msg_buf, batch_size);
        let expected_msg_count = if received_cnt == batch_size {
            None
        } else {
            Some(0)
        };
        let mut delegate = StoreFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.handle_msgs(&mut self.store_msg_buf);
        expected_msg_count
    }

    fn handle_normal(&mut self, fsm: &mut impl DerefMut<Target = PeerFsm<EK, ER>>) -> HandleResult {
        debug_assert!(self.peer_msg_buf.is_empty());
        let batch_size = self.messages_per_tick();
        let received_cnt = fsm.recv(&mut self.peer_msg_buf, batch_size);
        let handle_result = if received_cnt == batch_size {
            HandleResult::KeepProcessing
        } else {
            HandleResult::stop_at(0, false)
        };
        let mut delegate = PeerFsmDelegate::new(fsm, &mut self.poll_ctx);
        delegate.on_msgs(&mut self.peer_msg_buf);
        delegate
            .fsm
            .peer_mut()
            .handle_raft_ready(delegate.store_ctx);
        handle_result
    }

    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }

        let now = TiInstant::now();
        if now.saturating_duration_since(self.last_flush_time) >= Duration::from_millis(1) {
            self.last_flush_time = now;
            self.need_flush_events = false;
            self.flush_events();
        } else {
            self.need_flush_events = true;
        }
    }

    fn end(&mut self, batch: &mut [Option<impl DerefMut<Target = PeerFsm<EK, ER>>>]) {}

    fn pause(&mut self) {
        if self.poll_ctx.trans.need_flush() {
            self.poll_ctx.trans.flush();
        }

        if self.need_flush_events {
            self.last_flush_time = TiInstant::now();
            self.need_flush_events = false;
            self.flush_events();
        }
    }
}

struct StorePollerBuilder<EK: KvEngine, ER: RaftEngine, T> {
    cfg: Arc<VersionTrack<Config>>,
    store_id: u64,
    engine: ER,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    trans: T,
    router: StoreRouter<EK, ER>,
    read_scheduler: Scheduler<ReadTask<EK>>,
    pd_scheduler: Scheduler<PdTask>,
    raft_log_gc_scheduler: Scheduler<RaftLogGcTask>,
    write_senders: WriteSenders<EK, ER>,
    apply_pool: FuturePool,
    logger: Logger,
    store_meta: Arc<Mutex<StoreMeta<EK>>>,
    snap_mgr: TabletSnapManager,
    lock_manager_observer: Arc<dyn LockManagerNotifier>,
}

impl<EK: KvEngine, ER: RaftEngine, T> StorePollerBuilder<EK, ER, T> {
    pub fn new(
        cfg: Arc<VersionTrack<Config>>,
        store_id: u64,
        engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        trans: T,
        router: StoreRouter<EK, ER>,
        read_scheduler: Scheduler<ReadTask<EK>>,
        pd_scheduler: Scheduler<PdTask>,
        raft_log_gc_scheduler: Scheduler<RaftLogGcTask>,
        store_writers: &mut StoreWriters<EK, ER>,
        logger: Logger,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        snap_mgr: TabletSnapManager,
        lock_manager_observer: Arc<dyn LockManagerNotifier>,
    ) -> Self {
        let pool_size = cfg.value().apply_batch_system.pool_size;
        let max_pool_size = std::cmp::max(
            pool_size,
            std::cmp::max(4, SysQuota::cpu_cores_quota() as usize),
        );
        let apply_pool = YatpPoolBuilder::new(DefaultTicker::default())
            .thread_count(1, pool_size, max_pool_size)
            .after_start(move || set_io_type(IoType::ForegroundWrite))
            .name_prefix("apply")
            .build_future_pool();
        StorePollerBuilder {
            cfg,
            store_id,
            engine,
            tablet_factory,
            trans,
            router,
            read_scheduler,
            pd_scheduler,
            raft_log_gc_scheduler,
            apply_pool,
            logger,
            write_senders: store_writers.senders(),
            store_meta,
            snap_mgr,
            lock_manager_observer,
        }
    }

    /// Initializes all the existing raft machines and cleans up stale tablets.
    fn init(&self) -> Result<HashMap<u64, SenderFsmPair<EK, ER>>> {
        let mut regions = HashMap::default();
        let cfg = self.cfg.value();
        let mut meta = self.store_meta.lock().unwrap();
        self.engine
            .for_each_raft_group::<Error, _>(&mut |region_id| {
                assert_ne!(region_id, INVALID_ID);
                let storage = match Storage::new(
                    region_id,
                    self.store_id,
                    self.engine.clone(),
                    self.read_scheduler.clone(),
                    &self.logger,
                )? {
                    Some(p) => p,
                    None => return Ok(()),
                };
                self.lock_manager_observer.on_region_changed(
                    storage.region_state().get_region(),
                    RegionChangeEvent::Create,
                    StateRole::Follower,
                );

                let (sender, peer_fsm) = PeerFsm::new(&cfg, &*self.tablet_factory, storage)?;
                meta.region_read_progress
                    .insert(region_id, peer_fsm.as_ref().peer().read_progress().clone());

                let prev = regions.insert(region_id, (sender, peer_fsm));
                if let Some((_, p)) = prev {
                    return Err(box_err!(
                        "duplicate region {:?} vs {:?}",
                        p.logger().list(),
                        regions[&region_id].1.logger().list()
                    ));
                }
                Ok(())
            })?;
        self.clean_up_tablets(&regions)?;
        Ok(regions)
    }

    fn clean_up_tablets(&self, peers: &HashMap<u64, SenderFsmPair<EK, ER>>) -> Result<()> {
        // TODO: list all available tablets and destroy those which are not in the
        // peers.
        Ok(())
    }
}

impl<EK, ER, T> HandlerBuilder<PeerFsm<EK, ER>, StoreFsm> for StorePollerBuilder<EK, ER, T>
where
    ER: RaftEngine,
    EK: KvEngine,
    T: Transport + 'static,
{
    type Handler = StorePoller<EK, ER, T>;

    fn build(&mut self, priority: batch_system::Priority) -> Self::Handler {
        let cfg = self.cfg.value().clone();
        let mut poll_ctx = StoreContext {
            logger: self.logger.clone(),
            trans: self.trans.clone(),
            current_time: None,
            has_ready: false,
            raft_metrics: RaftMetrics::new(cfg.waterfall_metrics),
            cfg,
            router: self.router.clone(),
            tick_batch: vec![PeerTickBatch::default(); PeerTick::VARIANT_COUNT],
            timer: SteadyTimer::default(),
            write_senders: self.write_senders.clone(),
            store_meta: self.store_meta.clone(),
            engine: self.engine.clone(),
            tablet_factory: self.tablet_factory.clone(),
            apply_pool: self.apply_pool.clone(),
            read_scheduler: self.read_scheduler.clone(),
            pd_scheduler: self.pd_scheduler.clone(),
            raft_log_gc_scheduler: self.raft_log_gc_scheduler.clone(),
            snap_mgr: self.snap_mgr.clone(),
            self_disk_usage: DiskUsage::Normal,
            lock_manager_observer: self.lock_manager_observer.clone(),
        };
        poll_ctx.update_ticks_timeout();
        let cfg_tracker = self.cfg.clone().tracker("raftstore".to_string());
        StorePoller::new(poll_ctx, cfg_tracker)
    }
}

/// A set of background threads that will processing offloaded work from
/// raftstore.
struct Workers<EK: KvEngine, ER: RaftEngine> {
    /// Worker for fetching raft logs asynchronously
    async_read_worker: Worker,
    pd_worker: Worker,
    raft_log_gc_worker: Worker,
    store_writers: StoreWriters<EK, ER>,
}

impl<EK: KvEngine, ER: RaftEngine> Default for Workers<EK, ER> {
    fn default() -> Self {
        Self {
            async_read_worker: Worker::new("async-read-worker"),
            pd_worker: Worker::new("pd-worker"),
            raft_log_gc_worker: Worker::new("raft-log-gc-worker"),
            store_writers: StoreWriters::default(),
        }
    }
}

/// The system used for polling Raft activities.
pub struct StoreSystem<EK: KvEngine, ER: RaftEngine> {
    system: BatchSystem<PeerFsm<EK, ER>, StoreFsm>,
    workers: Option<Workers<EK, ER>>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> StoreSystem<EK, ER> {
    pub fn start<T, C>(
        &mut self,
        store_id: u64,
        cfg: Arc<VersionTrack<Config>>,
        raft_engine: ER,
        tablet_factory: Arc<dyn TabletFactory<EK>>,
        trans: T,
        pd_client: Arc<C>,
        router: &StoreRouter<EK, ER>,
        store_meta: Arc<Mutex<StoreMeta<EK>>>,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        lock_manager_observer: Arc<dyn LockManagerNotifier>,
    ) -> Result<()>
    where
        T: Transport + 'static,
        C: PdClient + 'static,
    {
        let sync_router = Mutex::new(router.clone());
        pd_client.handle_reconnect(move || {
            sync_router
                .lock()
                .unwrap()
                .broadcast_normal(|| PeerMsg::Tick(PeerTick::PdHeartbeat));
        });

        let mut workers = Workers::default();
        workers
            .store_writers
            .spawn(store_id, raft_engine.clone(), None, router, &trans, &cfg)?;
        let mut read_runner = ReadRunner::new(router.clone(), raft_engine.clone());
        read_runner.set_snap_mgr(snap_mgr.clone());
        let read_scheduler = workers
            .async_read_worker
            .start("async-read-worker", read_runner);
        let pd_scheduler = workers.pd_worker.start(
            "pd-worker",
            PdRunner::new(
                store_id,
                pd_client,
                raft_engine.clone(),
                tablet_factory.clone(),
                router.clone(),
                workers.pd_worker.remote(),
                concurrency_manager,
                causal_ts_provider,
                self.logger.clone(),
            ),
        );

        let raft_log_gc_scheduler = workers.raft_log_gc_worker.start_with_timer(
            "raft-log-gc",
            RaftLogGcRunner::new(raft_engine.clone(), self.logger.clone()),
        );

        let mut builder = StorePollerBuilder::new(
            cfg.clone(),
            store_id,
            raft_engine,
            tablet_factory,
            trans,
            router.clone(),
            read_scheduler,
            pd_scheduler,
            raft_log_gc_scheduler,
            &mut workers.store_writers,
            self.logger.clone(),
            store_meta.clone(),
            snap_mgr,
            lock_manager_observer,
        );
        self.workers = Some(workers);
        let peers = builder.init()?;
        // Choose a different name so we know what version is actually used. rs stands
        // for raft store.
        let tag = format!("rs-{}", store_id);
        self.system.spawn(tag, builder);

        let mut mailboxes = Vec::with_capacity(peers.len());
        let mut address = Vec::with_capacity(peers.len());
        {
            let mut meta = store_meta.as_ref().lock().unwrap();
            for (region_id, (tx, fsm)) in peers {
                meta.readers
                    .insert(region_id, fsm.peer().generate_read_delegate());
                meta.tablet_caches
                    .insert(region_id, fsm.peer().tablet().clone());

                address.push(region_id);
                mailboxes.push((
                    region_id,
                    BasicMailbox::new(tx, fsm, router.state_cnt().clone()),
                ));
            }
        }
        router.register_all(mailboxes);

        // Make sure Msg::Start is the first message each FSM received.
        for addr in address {
            router.force_send(addr, PeerMsg::Start).unwrap();
        }
        router.send_control(StoreMsg::Start(store_id)).unwrap();
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if self.workers.is_none() {
            return;
        }
        let mut workers = self.workers.take().unwrap();

        // TODO: gracefully shutdown future pool

        self.system.shutdown();

        workers.store_writers.shutdown();
        workers.async_read_worker.stop();
    }
}

#[derive(Clone)]
pub struct StoreRouter<EK: KvEngine, ER: RaftEngine> {
    router: BatchRouter<PeerFsm<EK, ER>, StoreFsm>,
    logger: Logger,
}

impl<EK: KvEngine, ER: RaftEngine> StoreRouter<EK, ER> {
    #[inline]
    pub fn logger(&self) -> &Logger {
        &self.logger
    }

    pub fn send_raft_message(
        &self,
        msg: Box<RaftMessage>,
    ) -> std::result::Result<(), TrySendError<Box<RaftMessage>>> {
        let id = msg.get_region_id();
        let peer_msg = PeerMsg::RaftMessage(msg);
        let store_msg = match self.router.try_send(id, peer_msg) {
            Either::Left(Ok(())) => return Ok(()),
            Either::Left(Err(TrySendError::Full(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Full(m));
            }
            Either::Left(Err(TrySendError::Disconnected(PeerMsg::RaftMessage(m)))) => {
                return Err(TrySendError::Disconnected(m));
            }
            Either::Right(PeerMsg::RaftMessage(m)) => StoreMsg::RaftMessage(m),
            _ => unreachable!(),
        };
        match self.router.send_control(store_msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(StoreMsg::RaftMessage(m))) => Err(TrySendError::Full(m)),
            Err(TrySendError::Disconnected(StoreMsg::RaftMessage(m))) => {
                Err(TrySendError::Disconnected(m))
            }
            _ => unreachable!(),
        }
    }

    pub fn send_peer_msg(&self, region_id: u64, msg: PeerMsg) -> Result<()> {
        match self.router.send(region_id, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(Error::Transport(DiscardReason::Full)),
            Err(TrySendError::Disconnected(_)) => Err(Error::RegionNotFound(region_id)),
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Deref for StoreRouter<EK, ER> {
    type Target = BatchRouter<PeerFsm<EK, ER>, StoreFsm>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.router
    }
}

impl<EK: KvEngine, ER: RaftEngine> DerefMut for StoreRouter<EK, ER> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.router
    }
}

/// Creates the batch system for polling raft activities.
pub fn create_store_batch_system<EK, ER>(
    cfg: &Config,
    logger: Logger,
) -> (StoreRouter<EK, ER>, StoreSystem<EK, ER>)
where
    EK: KvEngine,
    ER: RaftEngine,
{
    let (store_tx, store_fsm) = StoreFsm::new(cfg, logger.clone());
    let (router, system) =
        batch_system::create_system(&cfg.store_batch_system, store_tx, store_fsm);
    let system = StoreSystem {
        system,
        workers: None,
        logger: logger.clone(),
    };
    (StoreRouter { router, logger }, system)
}

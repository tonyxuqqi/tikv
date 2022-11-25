// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! This module is the same as `server` but setup server for raftstore v2.
//!
//! After all functionalities of v1 are supported by v2, this file should be
//! merged with v1.

use std::{
    cmp,
    collections::HashMap,
    env, fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use causal_ts::CausalTsProviderImpl;
use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    raw::{Cache, Env},
    FlowInfo, RocksEngine,
};
use engine_traits::{
    CfOptions, CfOptionsExt, Engines, FlowControlFactorsExt, KvEngine, RaftEngine, TabletFactory,
    CF_DEFAULT, CF_LOCK, CF_WRITE,
};
use error_code::ErrorCodeExt;
use file_system::{get_io_rate_limiter, File, IoBudgetAdjustor};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use grpcio_health::HealthService;
use kvproto::{
    deadlock::create_deadlock, diagnosticspb::create_diagnostics, kvrpcpb::ApiVersion,
    resource_usage_agent::create_resource_metering_pub_sub,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::store::{
    FlowStatsReporter, GlobalReplicationState, ReadStats, TabletSnapManager, WriteStats,
};
use raftstore_v2::{
    router::{RaftRouter, StoreRouterCompactedEventSender},
    StoreRouter, StoreSystem,
};
use security::SecurityManager;
use slog::o;
use tikv::{
    config::{ConfigController, DbConfigManger, DbType, LogConfigManager, TikvConfig},
    coprocessor, coprocessor_v2,
    read_pool::{build_yatp_read_pool, ReadPool},
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        lock_manager::LockManager,
        resolve,
        service::DiagnosticsService,
        status_server::StatusServer,
        KvEngineFactoryBuilder, NodeV2, RaftKvV2, RouterWrap, Server, CPU_CORES_QUOTA_GAUGE,
        DEFAULT_CLUSTER_ID, GRPC_THREAD_PREFIX,
    },
    storage::{
        self,
        txn::flow_controller::{FlowController, TabletFlowController},
        Engine, Storage,
    },
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, RaftDataStateMachine, VersionTrack},
    math::MovingAvgU32,
    metrics::INSTANCE_BACKEND_CPU_QUOTA,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{cpu_time::ProcessStat, disk, register_memory_usage_high_water, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Worker},
};
use tokio::runtime::Builder;

use crate::{
    raft_engine_switch::*, setup::*, signal_handler, tikv_util::sys::thread::ThreadBuildWrapper,
};

// minimum number of core kept for background requests
const BACKGROUND_REQUEST_CORE_LOWER_BOUND: f64 = 1.0;
// max ratio of core quota for background requests
const BACKGROUND_REQUEST_CORE_MAX_RATIO: f64 = 0.95;
// default ratio of core quota for background requests = core_number * 0.5
const BACKGROUND_REQUEST_CORE_DEFAULT_RATIO: f64 = 0.5;
// indication of TiKV instance is short of cpu
const SYSTEM_BUSY_THRESHOLD: f64 = 0.80;
// indication of TiKV instance in healthy state when cpu usage is in [0.5, 0.80)
const SYSTEM_HEALTHY_THRESHOLD: f64 = 0.50;
// pace of cpu quota adjustment
const CPU_QUOTA_ADJUSTMENT_PACE: f64 = 200.0; // 0.2 vcpu

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);

#[inline]
fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(config: TikvConfig) {
    let mut tikv = TikvServer::<CER>::init::<F>(config);

    // Must be called after `TikvServer::init`.
    let memory_limit = tikv.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.check_conflict_addr();
    tikv.init_fs();
    tikv.init_yatp();
    tikv.init_encryption();
    let listener = tikv.init_flow_receiver();
    let (raft_engine, _engines_info) = tikv.init_raw_engines(listener);
    let (engine, server_config) = tikv.init_servers::<F>(raft_engine.clone());
    tikv.register_services();
    tikv.init_metrics_flusher(raft_engine);
    tikv.run_server(server_config);
    tikv.run_status_server(engine.raft_extension().clone());
    tikv.init_quota_tuning_task(tikv.quota_limiter.clone());

    signal_handler::wait_for_signal(None as Option<Engines<RocksEngine, CER>>);
    tikv.stop();
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub fn run_tikv(mut config: TikvConfig) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    if config.raft_store.store_io_pool_size == 0 {
        // v2 always use async write.
        config.raft_store.store_io_pool_size = 1;
    }

    // Print version information.
    let build_timestamp = option_env!("TIKV_BUILD_TIME");
    tikv::log_tikv_info(build_timestamp);

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl::<RocksEngine, API>(config)
        } else {
            run_impl::<RaftLogEngine, API>(config)
        }
    })
}

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL: Duration = Duration::from_secs(5);

/// A complete TiKV server.
struct TikvServer<ER: RaftEngine> {
    config: TikvConfig,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: StoreRouter<RocksEngine, ER>,
    flow_info_sender: Option<mpsc::Sender<FlowInfo>>,
    flow_info_receiver: Option<mpsc::Receiver<FlowInfo>>,
    system: Option<StoreSystem<RocksEngine, ER>>,
    resolver: resolve::PdStoreAddrResolver,
    state: Arc<Mutex<GlobalReplicationState>>,
    store_path: PathBuf,
    snap_mgr: Option<TabletSnapManager>, // Will be filled in `init_servers`.
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    servers: Option<Servers<RocksEngine, ER>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
    tablet_factory: Option<Arc<dyn TabletFactory<RocksEngine> + Send + Sync>>,
}

struct Servers<EK: KvEngine, ER: RaftEngine> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    node: NodeV2<RpcClient, EK, ER>,
    rsmeter_pubsub_service: resource_metering::PubSubService,
}

// TODO: remove following after pd is supported.

#[derive(Clone)]
struct DummyReporter;

impl FlowStatsReporter for DummyReporter {
    fn report_read_stats(&self, _read_stats: ReadStats) {}
    fn report_write_stats(&self, _write_stats: WriteStats) {}
}

type LocalServer<EK, ER> = Server<RouterWrap<EK, ER>, resolve::PdStoreAddrResolver>;
type LocalRaftKv<EK, ER> = RaftKvV2<EK, ER>;

impl<ER: RaftEngine> TikvServer<ER> {
    fn init<F: KvFormat>(mut config: TikvConfig) -> Self {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        let logger = slog_global::borrow_global().new(o!());
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let pd_client =
            Self::connect_to_pd_cluster(&mut config, env.clone(), Arc::clone(&security_mgr));
        // check if TiKV need to run in snapshot recovery mode
        let is_recovering_marked = match pd_client.is_recovering_marked() {
            Err(e) => {
                warn!(
                    "failed to get recovery mode from PD";
                    "error" => ?e,
                );
                false
            }
            Ok(marked) => marked,
        };

        if is_recovering_marked {
            // Run a TiKV server in recovery modeß
            info!("TiKV running in Snapshot Recovery Mode");
            snap_recovery::init_cluster::enter_snap_recovery_mode(&mut config);
            // connect_to_pd_cluster retreived the cluster id from pd
            let cluster_id = config.server.cluster_id;
            snap_recovery::init_cluster::start_recovery(
                config.clone(),
                cluster_id,
                pd_client.clone(),
            );
        }

        // Initialize and check config
        let cfg_controller = Self::init_config(config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        // Initialize raftstore channels.
        let (router, system) =
            raftstore_v2::create_store_batch_system(&config.raft_store, logger.clone());

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();
        let (resolver, state) = resolve::new_resolver(
            Arc::clone(&pd_client),
            &background_worker,
            RouterWrap::new(router.clone()),
        );

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        // use different quota for front-end and back-end requests
        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.background_cpu_time,
            config.quota.background_write_bandwidth,
            config.quota.background_read_bandwidth,
            config.quota.max_delay_duration,
            config.quota.enable_auto_tune,
        ));

        let mut causal_ts_provider = None;
        if let ApiVersion::V2 = F::TAG {
            let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
                pd_client.clone(),
                config.causal_ts.renew_interval.0,
                config.causal_ts.alloc_ahead_buffer.0,
                config.causal_ts.renew_batch_min_size,
                config.causal_ts.renew_batch_max_size,
            ));
            if let Err(e) = tso {
                fatal!("Causal timestamp provider initialize failed: {:?}", e);
            }
            causal_ts_provider = Some(Arc::new(tso.unwrap().into()));
            info!("Causal timestamp provider startup.");
        }

        TikvServer {
            config,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver,
            state,
            store_path,
            snap_mgr: None,
            encryption_key_manager: None,
            servers: None,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            flow_info_sender: None,
            flow_info_receiver: None,
            sst_worker: None,
            quota_limiter,
            causal_ts_provider,
            tablet_factory: None,
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TikvConfig) -> ConfigController {
        validate_and_persist_config(&mut config, true);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_pd_cluster(
        config: &mut TikvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(lock_dir);
        file_system::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in file_system::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let _ = try_lock_conflict_addr(file_path);
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have
        // enough space to do compaction and region migration when TiKV recover.
        // This file is created in data_dir rather than db_path, because we must not
        // increase store size of db_path.
        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }
        let mut reserve_space = self.config.storage.reserve_space.0;
        if self.config.storage.reserve_space.0 != 0 {
            reserve_space = cmp::max(
                (capacity as f64 * 0.05) as u64,
                self.config.storage.reserve_space.0,
            );
        }
        disk::set_disk_reserved_space(reserve_space);
        let path =
            Path::new(&self.config.storage.data_dir).join(file_system::SPACE_PLACEHOLDER_FILE);
        if let Err(e) = file_system::remove_file(path) {
            warn!("failed to remove space holder on starting: {}", e);
        }

        let available = disk_stats.available_space();
        // place holder file size is 20% of total reserved space.
        if available > reserve_space {
            file_system::reserve_space_for_recover(
                &self.config.storage.data_dir,
                reserve_space / 5,
            )
            .map_err(|e| panic!("Failed to reserve space for recovery: {}.", e))
            .unwrap();
        } else {
            warn!("no enough disk space left to create the place holder file");
        }
    }

    fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("tikv"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_EXEC_DURATION.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_POLL_DURATION.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::TASK_EXEC_TIMES.clone())).unwrap();
    }

    fn init_encryption(&mut self) {
        self.encryption_key_manager = data_key_manager_from_config(
            &self.config.security.encryption,
            &self.config.storage.data_dir,
        )
        .map_err(|e| {
            panic!(
                "Encryption failed to initialize: {}. code: {}",
                e,
                e.error_code()
            )
        })
        .unwrap()
        .map(Arc::new);
    }

    fn init_flow_receiver(&mut self) -> engine_rocks::FlowListener {
        let (tx, rx) = mpsc::channel();
        self.flow_info_sender = Some(tx.clone());
        self.flow_info_receiver = Some(rx);
        engine_rocks::FlowListener::new(tx)
    }

    fn init_servers<F: KvFormat>(
        &mut self,
        raft_engine: ER,
    ) -> (
        LocalRaftKv<RocksEngine, ER>,
        Arc<VersionTrack<ServerConfig>>,
    ) {
        let flow_controller = Arc::new(FlowController::Tablet(TabletFlowController::new(
            &self.config.storage.flow_control,
            self.tablet_factory.clone().unwrap(),
            self.flow_info_receiver.take().unwrap(),
        )));

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        cfg_controller.register(tikv::config::Module::Log, Box::new(LogConfigManager));

        let lock_mgr = LockManager::new(&self.config.pessimistic_txn);
        cfg_controller.register(
            tikv::config::Module::PessimisticTxn,
            Box::new(lock_mgr.config_manager()),
        );
        let role_change_notifier = lock_mgr.generate_notifier();

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );

        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(self.config.resource_metering.precision.as_millis());
        self.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.config.resource_metering.clone(),
                collector_reg_handle,
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle.clone(),
        );
        self.to_stop.push(single_target_worker);
        let rsmeter_pubsub_service = resource_metering::PubSubService::new(data_sink_reg_handle);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        // Create snapshot manager, server.
        let snap_path = self
            .store_path
            .join(Path::new("snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let snap_mgr = TabletSnapManager::new(snap_path);

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate(
                self.config.coprocessor.region_split_size,
                self.config.coprocessor.enable_region_bucket,
                self.config.coprocessor.region_bucket_size,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.config.raft_store.clone()));
        let health_service = HealthService::default();
        let mut node = NodeV2::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store,
            self.pd_client.clone(),
            self.state.clone(),
            self.background_worker.clone(),
            None,
            self.tablet_factory.clone().unwrap(),
        );
        node.try_bootstrap_store(&raft_engine)
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        info!("using raft kv v2");
        let raft_router = RaftRouter::new(node.id(), self.router.clone());
        let raft_kv_v2 = LocalRaftKv::new(raft_router.clone());

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                DummyReporter,
                raft_kv_v2.clone(),
            ))
        } else {
            None
        };

        let storage_read_pool_handle = if self.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.config.readpool.storage,
                DummyReporter,
                raft_kv_v2.clone(),
            ));
            storage_read_pools.handle()
        };

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                DummyReporter,
                raft_kv_v2.clone(),
            ));
            cop_read_pools.handle()
        };

        let storage = Storage::<_, _, F>::from_engine(
            raft_kv_v2.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            lock_mgr.get_storage_dynamic_configs(),
            flow_controller,
            DummyReporter,
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            self.causal_ts_provider.clone(),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

        self.snap_mgr = Some(snap_mgr.clone());
        let (check_leader_scheduler, _) = tikv_util::worker::dummy_scheduler();
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                resource_tag_factory,
                Arc::clone(&self.quota_limiter),
            ),
            coprocessor_v2::Endpoint::new(&self.config.coprocessor_v2),
            raft_kv_v2.raft_extension().clone(),
            self.resolver.clone(),
            None,
            self.snap_mgr.clone(),
            None,
            check_leader_scheduler,
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
            health_service,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));
        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
                server.get_grpc_mem_quota().clone(),
            )),
        );

        node.start(
            raft_engine.clone(),
            server.transport(),
            &raft_router,
            snap_mgr,
            Arc::new(role_change_notifier),
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        // Start auto gc. Must after `Node::start` because `node_id` is initialized
        // there.
        assert!(node.id() > 0); // Node id should never be 0.

        initial_metric(&self.config.metric);

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            rsmeter_pubsub_service,
        });

        (raft_kv_v2, server_config)
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.config.log.file.filename.clone(),
            self.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Lock manager.
        if servers
            .server
            .register_service(create_deadlock(servers.lock_mgr.deadlock_service()))
            .is_some()
        {
            fatal!("failed to register deadlock service");
        }

        servers
            .lock_mgr
            .start(
                servers.node.id(),
                self.pd_client.clone(),
                self.resolver.clone(),
                self.security_mgr.clone(),
                &self.config.pessimistic_txn,
            )
            .unwrap_or_else(|e| fatal!("failed to start lock manager: {}", e));

        if servers
            .server
            .register_service(create_resource_metering_pub_sub(
                servers.rsmeter_pubsub_service.clone(),
            ))
            .is_some()
        {
            warn!("failed to register resource metering pubsub service");
        }
    }

    fn init_metrics_flusher(&mut self, raft_engine: ER) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.tablet_factory.clone().unwrap(),
            raft_engine,
        );

        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                engine_metrics.flush(now);
            });
    }

    // Only background cpu quota tuning is implemented at present. iops and frontend
    // quota tuning is on the way
    fn init_quota_tuning_task(&self, quota_limiter: Arc<QuotaLimiter>) {
        // No need to do auto tune when capacity is really low
        if SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO
            < BACKGROUND_REQUEST_CORE_LOWER_BOUND
        {
            return;
        };

        // Determine the base cpu quota
        let base_cpu_quota =
            // if cpu quota is not specified, start from optimistic case
            if quota_limiter.cputime_limiter(false).is_infinite() {
                1000_f64
                    * f64::max(
                        BACKGROUND_REQUEST_CORE_LOWER_BOUND,
                        SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_DEFAULT_RATIO,
                    )
            } else {
                quota_limiter.cputime_limiter(false) / 1000_f64
            };

        // Calculate the celling and floor quota
        let celling_quota = f64::min(
            base_cpu_quota * 2.0,
            1_000_f64 * SysQuota::cpu_cores_quota() * BACKGROUND_REQUEST_CORE_MAX_RATIO,
        );
        let floor_quota = f64::max(
            base_cpu_quota * 0.5,
            1_000_f64 * BACKGROUND_REQUEST_CORE_LOWER_BOUND,
        );

        let mut proc_stats: ProcessStat = ProcessStat::cur_proc_stat().unwrap();
        self.background_worker.spawn_interval_task(
            DEFAULT_QUOTA_LIMITER_TUNE_INTERVAL,
            move || {
                if quota_limiter.auto_tune_enabled() {
                    let cputime_limit = quota_limiter.cputime_limiter(false);
                    let old_quota = if cputime_limit.is_infinite() {
                        base_cpu_quota
                    } else {
                        cputime_limit / 1000_f64
                    };
                    let cpu_usage = match proc_stats.cpu_usage() {
                        Ok(r) => r,
                        Err(_e) => 0.0,
                    };
                    // Try tuning quota when cpu_usage is correctly collected.
                    // rule based tuning:
                    // - if instance is busy, shrink cpu quota for analyze by one quota pace until
                    //   lower bound is hit;
                    // - if instance cpu usage is healthy, no op;
                    // - if instance is idle, increase cpu quota by one quota pace  until upper
                    //   bound is hit.
                    if cpu_usage > 0.0f64 {
                        let mut target_quota = old_quota;

                        let cpu_util = cpu_usage / SysQuota::cpu_cores_quota();
                        if cpu_util >= SYSTEM_BUSY_THRESHOLD {
                            target_quota =
                                f64::max(target_quota - CPU_QUOTA_ADJUSTMENT_PACE, floor_quota);
                        } else if cpu_util < SYSTEM_HEALTHY_THRESHOLD {
                            target_quota =
                                f64::min(target_quota + CPU_QUOTA_ADJUSTMENT_PACE, celling_quota);
                        }

                        if old_quota != target_quota {
                            quota_limiter.set_cpu_time_limit(target_quota as usize, false);
                            debug!(
                                "cpu_time_limiter tuned for backend request";
                                "cpu_util" => ?cpu_util,
                                "new quota" => ?target_quota);
                            INSTANCE_BACKEND_CPU_QUOTA.set(target_quota as i64);
                        }
                    }
                }
            },
        );
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self, rt: RouterWrap<RocksEngine, ER>) {
        // Create a status server.
        let status_enabled = !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                self.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                rt,
                self.store_path.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    fn stop(self) {
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();

        servers.lock_mgr.stop();

        if let Some(sst_worker) = self.sst_worker {
            sst_worker.stop_worker();
        }

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

pub trait ConfiguredRaftEngine: RaftEngine {
    fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Option<Cache>,
    ) -> Self;
    fn as_rocks_engine(&self) -> Option<&RocksEngine>;
    fn register_config(&self, _cfg_controller: &mut ConfigController, _share_cache: bool);
}

impl<T: RaftEngine> ConfiguredRaftEngine for T {
    default fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Option<Cache>,
    ) -> Self {
        unimplemented!()
    }
    default fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        None
    }
    default fn register_config(&self, _cfg_controller: &mut ConfigController, _share_cache: bool) {}
}

impl ConfiguredRaftEngine for RocksEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Option<Cache>,
    ) -> Self {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_engine.config().dir,
            &config.raft_store.raftdb_path,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_db_path = &config.raft_store.raftdb_path;
        let config_raftdb = &config.raftdb;
        let mut raft_db_opts = config_raftdb.build_opt();
        raft_db_opts.set_env(env.clone());
        let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
        let mut raftdb =
            engine_rocks::util::new_engine_opt(raft_db_path, raft_db_opts, raft_cf_opts)
                .expect("failed to open raftdb");
        raftdb.set_shared_block_cache(block_cache.is_some());

        if should_dump {
            let raft_engine =
                RaftLogEngine::new(config.raft_engine.config(), key_manager.clone(), None)
                    .expect("failed to open raft engine for migration");
            dump_raft_engine_to_raftdb(&raft_engine, &raftdb, 8 /* threads */);
            raft_engine.stop();
            drop(raft_engine);
            raft_data_state_machine.after_dump_data();
        }
        raftdb
    }

    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        Some(self)
    }

    fn register_config(&self, cfg_controller: &mut ConfigController, share_cache: bool) {
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DbConfigManger::new(
                Arc::new(self.clone()),
                DbType::Raft,
                share_cache,
            )),
        );
    }
}

impl ConfiguredRaftEngine for RaftLogEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Option<Cache>,
    ) -> Self {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_store.raftdb_path,
            &config.raft_engine.config().dir,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_config = config.raft_engine.config();
        let raft_engine =
            RaftLogEngine::new(raft_config, key_manager.clone(), get_io_rate_limiter())
                .expect("failed to open raft engine");

        if should_dump {
            let config_raftdb = &config.raftdb;
            let mut raft_db_opts = config_raftdb.build_opt();
            raft_db_opts.set_env(env.clone());
            let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
            let raftdb = engine_rocks::util::new_engine_opt(
                &config.raft_store.raftdb_path,
                raft_db_opts,
                raft_cf_opts,
            )
            .expect("failed to open raftdb for migration");
            dump_raftdb_to_raft_engine(&raftdb, &raft_engine, 8 /* threads */);
            raftdb.stop();
            drop(raftdb);
            raft_data_state_machine.after_dump_data();
        }
        raft_engine
    }
}

impl<CER: ConfiguredRaftEngine> TikvServer<CER> {
    fn init_raw_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
    ) -> (CER, Arc<EnginesResourceInfo>) {
        let block_cache = self.config.storage.block_cache.build_shared_cache();
        let env = self
            .config
            .build_shared_rocks_env(self.encryption_key_manager.clone(), get_io_rate_limiter())
            .unwrap();

        // Create raft engine
        let raft_engine = CER::build(
            &self.config,
            &env,
            &self.encryption_key_manager,
            &block_cache,
        );

        // Create kv engine.
        let mut builder = KvEngineFactoryBuilder::new(env, &self.config, &self.store_path)
            .compaction_event_sender(Arc::new(StoreRouterCompactedEventSender {
                router: Mutex::new(self.router.clone()),
            }))
            .flow_listener(flow_listener);
        if let Some(cache) = block_cache {
            builder = builder.block_cache(cache);
        }
        let factory = Arc::new(builder.build_v2());

        self.tablet_factory = Some(factory.clone());
        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        raft_engine.register_config(cfg_controller, self.config.storage.block_cache.shared);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            factory,
            raft_engine.as_rocks_engine().cloned(),
            180, // max_samples_to_preserve
        ));

        (raft_engine, engines_info)
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

fn check_system_config(config: &TikvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl<R> Stop for StatusServer<R>
where
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

pub struct EngineMetricsManager<EK: KvEngine, ER: RaftEngine> {
    tablet_factory: Arc<dyn TabletFactory<EK> + Sync + Send>,
    raft_engine: ER,
    last_reset: Instant,
}

impl<EK: KvEngine, ER: RaftEngine> EngineMetricsManager<EK, ER> {
    pub fn new(tablet_factory: Arc<dyn TabletFactory<EK> + Sync + Send>, raft_engine: ER) -> Self {
        EngineMetricsManager {
            tablet_factory,
            raft_engine,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        let should_reset =
            now.saturating_duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL;
        let mut is_first_instance = true;
        self.tablet_factory
            .for_each_opened_tablet(&mut |_, _, db: &EK| {
                KvEngine::flush_metrics(db, "kv", is_first_instance);
                is_first_instance = false;
                if should_reset {
                    KvEngine::reset_statistics(db);
                }
            });
        self.raft_engine.flush_metrics("raft", true);
        if should_reset {
            self.raft_engine.reset_statistics();
            self.last_reset = now;
        }
    }
}

pub struct EnginesResourceInfo {
    tablet_factory: Arc<dyn TabletFactory<RocksEngine> + Sync + Send>,
    raft_engine: Option<RocksEngine>,
    latest_normalized_pending_bytes: AtomicU32,
    normalized_pending_bytes_collector: MovingAvgU32,
}

impl EnginesResourceInfo {
    const SCALE_FACTOR: u64 = 100;

    fn new(
        tablet_factory: Arc<dyn TabletFactory<RocksEngine> + Sync + Send>,
        raft_engine: Option<RocksEngine>,
        max_samples_to_preserve: usize,
    ) -> Self {
        EnginesResourceInfo {
            tablet_factory,
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(
        &self,
        _now: Instant,
        cached_latest_tablets: &mut HashMap<u64, (u64, RocksEngine)>,
    ) {
        let mut normalized_pending_bytes = 0;

        fn fetch_engine_cf(engine: &RocksEngine, cf: &str, normalized_pending_bytes: &mut u32) {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                if let Ok(Some(b)) = engine.get_cf_pending_compaction_bytes(cf) {
                    if cf_opts.get_soft_pending_compaction_bytes_limit() > 0 {
                        *normalized_pending_bytes = std::cmp::max(
                            *normalized_pending_bytes,
                            (b * EnginesResourceInfo::SCALE_FACTOR
                                / cf_opts.get_soft_pending_compaction_bytes_limit())
                                as u32,
                        );
                    }
                }
            }
        }

        if let Some(raft_engine) = &self.raft_engine {
            fetch_engine_cf(raft_engine, CF_DEFAULT, &mut normalized_pending_bytes);
        }

        self.tablet_factory
            .for_each_opened_tablet(
                &mut |id, suffix, db: &RocksEngine| match cached_latest_tablets.entry(id) {
                    collections::HashMapEntry::Occupied(mut slot) => {
                        if slot.get().0 < suffix {
                            slot.insert((suffix, db.clone()));
                        }
                    }
                    collections::HashMapEntry::Vacant(slot) => {
                        slot.insert((suffix, db.clone()));
                    }
                },
            );

        // todo(SpadeA): Now, there's a potential race condition problem where the
        // tablet could be destroyed after the clone and before the fetching
        // which could result in programme panic. It's okay now as the single global
        // kv_engine will not be destroyed in normal operation and v2 is not
        // ready for operation. Furthermore, this race condition is general to v2 as
        // tablet clone is not a case exclusively happened here. We should
        // propose another PR to tackle it such as destory tablet lazily in a GC
        // thread.

        for (_, (_, tablet)) in cached_latest_tablets.iter() {
            for cf in &[CF_DEFAULT, CF_WRITE, CF_LOCK] {
                fetch_engine_cf(tablet, cf, &mut normalized_pending_bytes);
            }
        }

        // Clear ensures that these tablets are not hold forever.
        cached_latest_tablets.clear();

        let (_, avg) = self
            .normalized_pending_bytes_collector
            .add(normalized_pending_bytes);
        self.latest_normalized_pending_bytes.store(
            std::cmp::max(normalized_pending_bytes, avg),
            Ordering::Relaxed,
        );
    }
}

impl IoBudgetAdjustor for EnginesResourceInfo {
    fn adjust(&self, total_budgets: usize) -> usize {
        let score = self.latest_normalized_pending_bytes.load(Ordering::Relaxed) as f32
            / Self::SCALE_FACTOR as f32;
        // Two reasons for adding `sqrt` on top:
        // 1) In theory the convergence point is independent of the value of pending
        //    bytes (as long as backlog generating rate equals consuming rate, which is
        //    determined by compaction budgets), a convex helps reach that point while
        //    maintaining low level of pending bytes.
        // 2) Variance of compaction pending bytes grows with its magnitude, a filter
        //    with decreasing derivative can help balance such trend.
        let score = score.sqrt();
        // The target global write flow slides between Bandwidth / 2 and Bandwidth.
        let score = 0.5 + score / 2.0;
        (total_budgets as f32 * score) as usize
    }
}
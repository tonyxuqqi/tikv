// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use collections::{HashMap, HashSet};
use concurrency_manager::ConcurrencyManager;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_test::raft::RaftTestEngine;
use engine_traits::{
    util::check_key_in_range, Peekable, RaftEngineReadOnly, TabletFactory, CF_DEFAULT, CF_WRITE,
};
use futures::executor::block_on;
use keys::Prefix;
use kvproto::{
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, Response},
    raft_serverpb::RaftMessage,
};
use pd_client::PdClient;
use raft::{prelude::MessageType, StateRole};
use raftstore::{
    coprocessor::{RegionChangeEvent, RoleChange},
    errors::Error as RaftError,
    store::{
        cmd_resp, copy_snapshot,
        util::{check_key_in_region, LockManagerNotifier},
        RegionSnapshot, SnapKey, TabletSnapKey, TabletSnapManager, Transport,
    },
    Result,
};
use raftstore_v2::{
    router::{QueryResult, RaftRouter},
    StoreMeta, StoreRouter, StoreSystem,
};
use test_pd_client::TestPdClient;
use test_raftstore::{new_snap_cmd, Config, Filter};
use tikv::{
    config::ConfigController,
    server::{tablet_snap::copy_tablet_snapshot, NodeV2, Result as ServerResult},
};
use tikv_util::{
    box_err,
    config::{ReadableSize, VersionTrack},
    time::ThreadReadId,
    worker::Builder as WorkerBuilder,
};

use crate::{
    cluster::Cluster,
    transport_simulate::{RaftStoreRouter, SimulateTransport, SnapshotRouter},
};

#[derive(Clone)]
pub struct ChannelTransport {
    core: Arc<Mutex<ChannelTransportCore>>,
}

impl ChannelTransport {
    pub fn new() -> ChannelTransport {
        ChannelTransport {
            core: Arc::new(Mutex::new(ChannelTransportCore {
                snap_paths: HashMap::default(),
                routers: HashMap::default(),
            })),
        }
    }

    pub fn core(&self) -> &Arc<Mutex<ChannelTransportCore>> {
        &self.core
    }
}

impl Transport for ChannelTransport {
    fn send(&mut self, msg: RaftMessage) -> raftstore::Result<()> {
        let from_store = msg.get_from_peer().get_store_id();
        let to_store = msg.get_to_peer().get_store_id();
        let is_snapshot = msg.get_message().get_msg_type() == MessageType::MsgSnapshot;

        if is_snapshot {
            let snap = msg.get_message().get_snapshot();
            let key = TabletSnapKey::from_region_snap(
                msg.get_region_id(),
                msg.get_to_peer().get_id(),
                snap,
            );
            let sender_snap_mgr = match self.core.lock().unwrap().snap_paths.get(&from_store) {
                Some(snap_mgr) => snap_mgr.clone(),
                None => return Err(box_err!("missing snap manager for store {}", from_store)),
            };
            let recver_snap_mgr = match self.core.lock().unwrap().snap_paths.get(&to_store) {
                Some(snap_mgr) => snap_mgr.clone(),
                None => return Err(box_err!("missing snap manager for store {}", to_store)),
            };

            if let Err(e) =
                copy_tablet_snapshot(key, msg.clone(), &sender_snap_mgr, &recver_snap_mgr)
            {
                return Err(box_err!("copy tablet snapshot failed: {:?}", e));
            }
        }

        let core = self.core.lock().unwrap();
        match core.routers.get(&to_store) {
            Some(h) => {
                h.send_raft_msg(msg)?;
                // report snapshot status if needed
                Ok(())
            }
            _ => Err(box_err!("missing sender for store {}", to_store)),
        }
    }

    fn set_store_allowlist(&mut self, _allowlist: Vec<u64>) {
        unimplemented!();
    }

    fn need_flush(&self) -> bool {
        false
    }

    fn flush(&mut self) {}
}

pub struct ChannelTransportCore {
    pub snap_paths: HashMap<u64, TabletSnapManager>,
    pub routers: HashMap<u64, SimulateTransport<RaftRouter<RocksEngine, RaftTestEngine>>>,
}

impl Default for ChannelTransport {
    fn default() -> Self {
        Self::new()
    }
}

type SimulateChannelTransport = SimulateTransport<ChannelTransport>;

pub struct NodeCluster {
    trans: ChannelTransport,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, NodeV2<TestPdClient, RocksEngine, RaftTestEngine>>,
    simulate_trans: HashMap<u64, SimulateChannelTransport>,
    concurrency_managers: HashMap<u64, ConcurrencyManager>,

    snap_mgrs: HashMap<u64, TabletSnapManager>,
}

// to be replaced by Simulator
impl NodeCluster {
    pub fn new(pd_client: Arc<TestPdClient>) -> NodeCluster {
        NodeCluster {
            trans: ChannelTransport::new(),
            pd_client,
            nodes: HashMap::default(),
            simulate_trans: HashMap::default(),
            concurrency_managers: HashMap::default(),
            snap_mgrs: HashMap::default(),
        }
    }

    pub fn channel_transport(&self) -> &ChannelTransport {
        &self.trans
    }

    pub fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<RocksEngine>>>,
        router: StoreRouter<RocksEngine, RaftTestEngine>,
        system: StoreSystem<RocksEngine, RaftTestEngine>,
        raft_engine: RaftTestEngine,
        factory: Arc<dyn TabletFactory<RocksEngine>>,
    ) -> ServerResult<u64> {
        assert!(node_id == 0 || !self.nodes.contains_key(&node_id));

        let simulate_trans = SimulateTransport::new(self.trans.clone());
        let mut raft_store = cfg.raft_store.clone();
        raft_store
            .validate(
                cfg.coprocessor.region_split_size,
                cfg.coprocessor.enable_region_bucket,
                cfg.coprocessor.region_bucket_size,
            )
            .unwrap();

        let bg_worker = WorkerBuilder::new("background").thread_count(2).create();
        let mut node = NodeV2::new(
            system,
            &cfg.server,
            Arc::new(VersionTrack::new(raft_store)),
            Arc::clone(&self.pd_client),
            Arc::default(),
            bg_worker,
            None,
            factory,
        );

        let snap_mgr = if node_id == 0
            || !self
                .trans
                .core
                .lock()
                .unwrap()
                .snap_paths
                .contains_key(&node_id)
        {
            // todo
            let snap_path = test_util::temp_dir("test_cluster", cfg.prefer_mem)
                .path()
                .join(Path::new(format!("snap_{}", node_id).as_str()))
                .to_str()
                .unwrap()
                .to_owned();
            TabletSnapManager::new(snap_path)
        } else {
            let trans = self.trans.core.lock().unwrap();
            trans.snap_paths[&node_id].clone()
        };

        let cm = ConcurrencyManager::new(1.into());
        self.concurrency_managers.insert(node_id, cm.clone());

        let cfg_controller = ConfigController::new(cfg.tikv.clone());

        node.try_bootstrap_store(&raft_engine)?;

        let raft_router = RaftRouter::new_with_store_meta(node_id, router, store_meta);
        node.start(
            raft_engine.clone(),
            simulate_trans.clone(),
            &raft_router,
            snap_mgr.clone(),
            Arc::new(DummyLockManagerObserver {}),
        )?;
        assert!(
            raft_engine
                .get_prepare_bootstrap_region()
                .unwrap()
                .is_none()
        );
        assert!(node_id == 0 || node_id == node.id());
        let node_id = node.id();

        let region_split_size = cfg.coprocessor.region_split_size;
        let enable_region_bucket = cfg.coprocessor.enable_region_bucket;
        let region_bucket_size = cfg.coprocessor.region_bucket_size;
        let mut raftstore_cfg = cfg.tikv.raft_store;
        raftstore_cfg
            .validate(region_split_size, enable_region_bucket, region_bucket_size)
            .unwrap();
        // let raft_store = Arc::new(VersionTrack::new(raftstore_cfg));
        // cfg_controller.register(
        //     Module::Raftstore,
        //     Box::new(RaftstoreConfigManager::new(
        //         node.refresh_config_scheduler(),
        //         raft_store,
        //     )),
        // );

        self.trans
            .core
            .lock()
            .unwrap()
            .snap_paths
            .insert(node_id, snap_mgr);

        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .insert(node_id, SimulateTransport::new(raft_router));

        self.nodes.insert(node_id, node);
        self.simulate_trans.insert(node_id, simulate_trans);
        Ok(node_id)
    }

    pub fn stop_node(&mut self, node_id: u64) {
        if let Some(mut node) = self.nodes.remove(&node_id) {
            node.stop();
        }
        self.trans
            .core
            .lock()
            .unwrap()
            .routers
            .remove(&node_id)
            .unwrap();
    }

    pub fn get_node_ids(&self) -> HashSet<u64> {
        self.nodes.keys().cloned().collect()
    }

    pub fn read(&mut self, req: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = req.get_header().get_peer().get_store_id();
        if !self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            let mut resp = RaftCmdResponse::default();
            let e: RaftError = box_err!("missing sender for store {}", node_id);
            resp.mut_header().set_error(e.into());
            return Ok(resp);
        }

        let mut guard = self.trans.core.lock().unwrap();
        let router = guard.routers.get_mut(&node_id).unwrap();
        let mut req_clone = req.clone();
        req_clone.clear_requests();
        req_clone.mut_requests().push(new_snap_cmd());
        match router.snapshot(req_clone, timeout) {
            Ok(snap) => {
                let requests = req.get_requests();
                let mut response = RaftCmdResponse::default();
                let mut responses = Vec::with_capacity(requests.len());
                for req in requests {
                    let cmd_type = req.get_cmd_type();
                    match cmd_type {
                        CmdType::Get => {
                            let mut resp = Response::default();
                            let key = req.get_get().get_key();
                            let cf = req.get_get().get_cf();
                            let region = snap.get_region();

                            if let Err(e) = check_key_in_region(key, region) {
                                return Ok(cmd_resp::new_error(e));
                            }

                            let res = if cf.is_empty() {
                                snap.get_value(key).unwrap_or_else(|e| {
                                    panic!(
                                        "[region {}] failed to get {} with cf {}: {:?}",
                                        snap.get_region().get_id(),
                                        log_wrappers::Value::key(key),
                                        cf,
                                        e
                                    )
                                })
                            } else {
                                snap.get_value_cf(cf, key).unwrap_or_else(|e| {
                                    panic!(
                                        "[region {}] failed to get {}: {:?}",
                                        snap.get_region().get_id(),
                                        log_wrappers::Value::key(key),
                                        e
                                    )
                                })
                            };
                            if let Some(res) = res {
                                resp.mut_get().set_value(res.to_vec());
                            }
                            resp.set_cmd_type(cmd_type);
                            responses.push(resp);
                        }
                        _ => unimplemented!(),
                    }
                }
                response.set_responses(responses.into());

                Ok(response)
            }
            Err(e) => Ok(e),
        }
    }

    pub fn call_command(&self, req: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = req.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, req, timeout)
    }

    pub fn call_command_on_node(
        &self,
        node_id: u64,
        req: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        if !self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            return Err(box_err!("missing sender for store {}", node_id));
        }

        let router = self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap();

        match router.send_command(req) {
            Ok(sub) => {
                // todo: unwrap and timeout
                let a = block_on(sub.result());
                if a.is_none() {
                    println!("here");
                }
                Ok(a.unwrap())
            }
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                Ok(resp)
            }
        }
    }

    pub fn call_query(&self, req: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = req.get_header().get_peer().get_store_id();

        if !self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .contains_key(&node_id)
        {
            return Err(box_err!("missing sender for store {}", node_id));
        }

        let router = self
            .trans
            .core
            .lock()
            .unwrap()
            .routers
            .get(&node_id)
            .cloned()
            .unwrap();

        match router.send_query(req) {
            Ok(sub) => {
                // todo: unwrap and timeout
                match block_on(sub.result()).unwrap() {
                    QueryResult::Read(a) => unreachable!(),
                    QueryResult::Response(resp) => return Ok(resp),
                }
            }
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                Ok(resp)
            }
        }
    }

    pub fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .add_filter(filter);
    }

    pub fn clear_send_filters(&mut self, node_id: u64) {
        self.simulate_trans
            .get_mut(&node_id)
            .unwrap()
            .clear_filters();
    }
}

pub fn new_node_cluster(id: u64, count: usize) -> Cluster {
    let pd_client = Arc::new(TestPdClient::new(id, false));
    let node_cluster = NodeCluster::new(Arc::clone(&pd_client));
    Cluster::new(id, count, node_cluster, pd_client)
}

struct DummyLockManagerObserver {}

impl LockManagerNotifier for DummyLockManagerObserver {
    fn on_region_changed(&self, _: &metapb::Region, _: RegionChangeEvent, _: StateRole) {}

    fn on_role_change(&self, _: &metapb::Region, _: RoleChange) {}
}

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
use futures::{executor::block_on, Future};
use keys::{data_key, Prefix};
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
    router::{PeerMsg, QueryResult, RaftRouter},
    StoreMeta, StoreRouter, StoreSystem,
};
use test_pd_client::TestPdClient;
use test_raftstore::{
    must_get_equal, must_get_none, new_get_cmd, new_peer, new_request, new_snap_cmd, Config,
};
use tikv::{
    config::ConfigController,
    server::{tablet_snap::copy_tablet_snapshot, NodeV2, Result as ServerResult},
};
use tikv_util::{
    box_err,
    config::{ReadableDuration, ReadableSize, VersionTrack},
    time::ThreadReadId,
    worker::Builder as WorkerBuilder,
};

use crate::{
    cluster::Cluster,
    transport_simulate::{RaftStoreRouter, SimulateTransport, SnapshotRouter},
    util::{self, put_cf_till_size, put_till_size},
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
        let to_peer_id = msg.get_to_peer().get_id();
        let region_id = msg.get_region_id();
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
                .join(Path::new("snap"))
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

fn new_conf_change_peer(store: &metapb::Store, pd_client: &Arc<TestPdClient>) -> metapb::Peer {
    let peer_id = pd_client.alloc_id().unwrap();
    new_peer(store.get_id(), peer_id)
}

#[test]
fn test_node_pd_conf_change() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_pd_conf_change(&mut cluster);
}

fn test_pd_conf_change(cluster: &mut Cluster) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();

    cluster.start().unwrap();

    let region = &pd_client.get_region(b"").unwrap();
    let region_id = region.get_id();

    let mut stores = pd_client.get_stores().unwrap();

    // Must have only one peer
    assert_eq!(region.get_peers().len(), 1);

    let peer = &region.get_peers()[0];

    let i = stores
        .iter()
        .position(|store| store.get_id() == peer.get_store_id())
        .unwrap();
    stores.swap(0, i);

    // Now the first store has first region. others have none.

    let (key, value) = (b"k1", b"v1");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let peer2 = new_conf_change_peer(&stores[1], &pd_client);
    let engine_2 = cluster.get_engine_of_key(peer2.get_store_id(), b"k1");
    // Does not exist
    assert!(engine_2.is_none());
    // add new peer to first region.
    pd_client.must_add_peer(region_id, peer2.clone());

    let (key, value) = (b"k2", b"v2");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));

    let engine_2 = cluster
        .get_engine_of_key(peer2.get_store_id(), b"k1")
        .unwrap();
    // now peer 2 must have v1 and v2;
    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k2", b"v2");

    // add new peer to first region.
    let peer3 = new_conf_change_peer(&stores[2], &pd_client);
    pd_client.must_add_peer(region_id, peer3.clone());
    println!("add peer 3");
    let engine_3 = cluster
        .get_engine_of_key(peer3.get_store_id(), b"k1")
        .unwrap();
    must_get_equal(&engine_3, b"k1", b"v1");

    // Remove peer2 from first region.
    pd_client.must_remove_peer(region_id, peer2);

    let (key, value) = (b"k3", b"v3");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer 3 must have v1, v2 and v3
    must_get_equal(&engine_3, b"k1", b"v1");
    must_get_equal(&engine_3, b"k2", b"v2");
    must_get_equal(&engine_3, b"k3", b"v3");

    // peer 2 has nothing
    must_get_none(&engine_2, b"k1");
    must_get_none(&engine_2, b"k2");
    // add peer4 to first region 1.
    let peer4 = new_conf_change_peer(&stores[1], &pd_client);
    pd_client.must_add_peer(region_id, peer4.clone());
    // Remove peer3 from first region.
    pd_client.must_remove_peer(region_id, peer3);

    let (key, value) = (b"k4", b"v4");
    cluster.must_put(key, value);
    assert_eq!(cluster.get(key), Some(value.to_vec()));
    // now peer4 must have v1, v2, v3, v4, we check v1 and v4 here.
    let engine_2 = cluster
        .get_engine_of_key(peer4.get_store_id(), b"k4")
        .unwrap();

    must_get_equal(&engine_2, b"k1", b"v1");
    must_get_equal(&engine_2, b"k4", b"v4");

    // peer 3 has nothing, we check v1 and v4 here.
    must_get_none(&engine_3, b"k1");
    must_get_none(&engine_3, b"k4");

    // TODO: add more tests.
}

pub const REGION_MAX_SIZE: u64 = 10000;
pub const REGION_SPLIT_SIZE: u64 = 6000;

#[test]
fn test_node_auto_split_region() {
    let count = 5;
    let mut cluster = new_node_cluster(0, count);
    test_auto_split_region(&mut cluster);
}

fn test_auto_split_region(cluster: &mut Cluster) {
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(100);
    cluster.cfg.coprocessor.region_max_size = Some(ReadableSize(REGION_MAX_SIZE));
    cluster.cfg.coprocessor.region_split_size = ReadableSize(REGION_SPLIT_SIZE);

    // tood: remove / 5
    let check_size_diff = cluster.cfg.raft_store.region_split_check_diff().0 / 5;
    let mut range = 1..;

    cluster.run();

    let pd_client = Arc::clone(&cluster.pd_client);

    let region = pd_client.get_region(b"").unwrap();

    let last_key = put_till_size(cluster, REGION_SPLIT_SIZE, &mut range);

    // it should be finished in millis if split.
    thread::sleep(Duration::from_millis(300));

    let target = pd_client.get_region(&last_key).unwrap();

    assert_eq!(region, target);

    let max_key = put_cf_till_size(
        cluster,
        CF_WRITE,
        REGION_MAX_SIZE - REGION_SPLIT_SIZE + check_size_diff,
        &mut range,
    );

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    if left == right {
        cluster.wait_region_split(&region);
    }

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();

    assert_ne!(left, right);
    // assert_eq!(region.get_start_key(), left.get_start_key());
    // assert_eq!(right.get_start_key(), left.get_end_key());
    // assert_eq!(region.get_end_key(), right.get_end_key());
    // assert_eq!(pd_client.get_region(&max_key).unwrap(), right);
    // assert_eq!(pd_client.get_region(left.get_end_key()).unwrap(), right);

    let middle_key = left.get_end_key();
    let leader = cluster.leader_of_region(left.get_id()).unwrap();
    let store_id = leader.get_store_id();
    let mut size = 0;

    let region_ids = cluster.region_ids(store_id);
    for id in region_ids {
        cluster
            .scan(store_id, id, CF_DEFAULT, b"", middle_key, false, |k, v| {
                size += k.len() as u64;
                size += v.len() as u64;
                Ok(true)
            })
            .expect("");
    }

    assert!(size <= REGION_SPLIT_SIZE);
    // although size may be smaller than REGION_SPLIT_SIZE, but the diff should
    // be small.

    // todo: now, split only uses approxiamte split keys
    // assert!(size > REGION_SPLIT_SIZE - 1000);

    let epoch = left.get_region_epoch().clone();
    let get = new_request(left.get_id(), epoch, vec![new_get_cmd(&max_key)], false);
    let resp = cluster
        .call_command_on_leader(get, Duration::from_secs(5))
        .unwrap();
    assert!(resp.get_header().has_error());
    assert!(resp.get_header().get_error().has_key_not_in_region());
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::TabletFactory;
use kvproto::metapb;
use raft::StateRole;
use raftstore::{
    coprocessor::{RegionChangeEvent, RoleChange},
    store::{util::LockManagerNotifier, TabletSnapManager},
};
use raftstore_v2::{router::RaftRouter, StoreMeta, StoreRouter, StoreSystem};
use test_pd_client::TestPdClient;
use test_raftstore::{Config, SimulateTransport};
use tikv::server::{NodeV2, Result as ServerResult};
use tikv_util::config::VersionTrack;

#[derive(Clone)]
pub struct ChannelTransport {
    core: Arc<Mutex<ChannelTransportCore>>,
}

pub struct ChannelTransportCore {
    snap_paths: HashMap<u64, TabletSnapManager>,
}

pub struct NodeCluster {
    trans: ChannelTransport,
    pd_client: Arc<TestPdClient>,
    nodes: HashMap<u64, Node<TestPdClient, RocksEngine, RaftTestEngine>>,

    snap_mgrs: HashMap<u64, TabletSnapManage>,
}

// to be replaced by Simulator
impl NodeCluster {
    pub fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta<RocksEngine>>>,
        raft_router: RaftRouter<RocksEngine, RaftTestEngine>,
        system: StoreSystem<RocksEngine, RaftTestEngine>,
        raft_engine: RaftTestEngine,
        factory: Arc<dyn TabletFactory<EK>>,
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
            unimplemented!()
        } else {
            let trans = self.trans.core.lock().unwrap();
            trans.snap_paths[&node_id].clone()
        };

        node.try_bootstrap_store(&raft_engine)?;
        node.start(
            raft_engine,
            simulate_trans,
            &raft_router,
            snap_mgr,
            DummyLockManagerObserver {},
        )?;
    }
}

struct DummyLockManagerObserver {}

impl LockManagerNotifier for DummyLockManagerObserver {
    fn on_region_changed(&self, _: &metapb::Region, _: RegionChangeEvent, _: StateRole) {}

    fn on_role_change(&self, _: &metapb::Region, _: RoleChange) {}
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry as MapEntry,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::TabletFactory;
use raftstore_v2::{create_store_batch_system, StoreMeta};
use test_raftstore::Config;
use tikv::server::Result as ServerResult;
use tikv_util::debug;

use crate::node::NodeCluster;

pub struct Cluster {
    pub cfg: Config,
    pub tablet_factories: HashMap<u64, Arc<dyn TabletFactory<RocksEngine> + Send + Sync>>,
    pub raft_engines: HashMap<u64, RaftTestEngine>,

    pub store_metas: HashMap<u64, Arc<Mutex<StoreMeta<RocksEngine>>>>,
    // todo: remove
    pub node_cluster: NodeCluster,
}

impl Cluster {
    pub fn new(id: u64, count: usize, pd_client: Arc<TestPdClient>) {}

    pub fn run_node(&mut self, node_id: u64) -> ServerResult<()> {
        debug!("starting node {}", node_id);
        let logger = slog_global::borrow_global().new(o!());
        let factory = self.tablet_factories[&node_id].clone();
        let raft_engine = self.raft_engines[&node_id].clone();
        let mut cfg = self.cfg.clone();
        let (router, system) = create_store_batch_system(&cfg, logger.clone());
        let store_meta = match self.store_metas.entry(node_id) {
            MapEntry::Occupied(o) => {
                let mut meta = o.get().lock().unwrap();
                *meta = StoreMeta::new();
                o.get().clone()
            }
            MapEntry::Vacant(v) => v.insert(Arc::new(Mutex::new(StoreMeta::new()))).clone(),
        };

        // let props = GroupProperties::default();
        // self.group_props.insert(node_id, props.clone());
        // tikv_util::thread_group::set_properties(Some(props));

        debug!("calling run node"; "node_id" => node_id);
        self.node_cluster.run_node(
            node_id,
            cfg,
            store_meta,
            router,
            system,
            raft_engine,
            factory,
        )?;
        debug!("node {} started", node_id);
        Ok(())
    }
}

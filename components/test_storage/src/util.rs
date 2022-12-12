// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::KvFormat;
use kvproto::kvrpcpb::Context;
use test_raftstore::{new_server_cluster, Cluster, ServerCluster, SimulateEngine};
use test_raftstore_v2::{
    new_server_cluster as new_server_cluster_v2, Cluster as ClusterV2,
    ServerCluster as ServerClusterV2, SimulateEngine as SimulateEngineV2,
};
use tikv_util::HandyRwLock;

use super::*;

pub fn new_raft_engine(
    count: usize,
    key: &str,
) -> (Cluster<ServerCluster>, SimulateEngine, Context) {
    let mut cluster = new_server_cluster(0, count);
    cluster.run();
    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b""), None);
    let region = cluster.get_region(key.as_bytes());
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let engine = cluster.sim.rl().storages[&leader.get_id()].clone();
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);
    (cluster, engine, ctx)
}

pub fn new_raft_engine_v2(
    count: usize,
    key: &str,
) -> (ClusterV2<ServerClusterV2>, SimulateEngineV2, Context) {
    let mut cluster = new_server_cluster_v2(0, count);
    cluster.run();
    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b""), None);
    let region = cluster.get_region(key.as_bytes());
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let engine = cluster.sim.rl().storages[&leader.get_id()].clone();
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);
    (cluster, engine, ctx)
}

pub fn new_raft_storage_with_store_count<F: KvFormat>(
    count: usize,
    key: &str,
) -> (
    Cluster<ServerCluster>,
    SyncTestStorage<SimulateEngine, F>,
    Context,
) {
    let (cluster, engine, ctx) = new_raft_engine(count, key);
    (
        cluster,
        SyncTestStorageBuilder::from_engine(engine)
            .build(ctx.peer.as_ref().unwrap().store_id)
            .unwrap(),
        ctx,
    )
}

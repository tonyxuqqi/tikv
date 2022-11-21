// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{thread, time::Duration};

use engine_traits::{MiscExt, OpenOptions, Peekable, TabletAccessor, TabletFactory};
use futures::executor::block_on;
use kvproto::{
    metapb, pdpb,
    raft_cmdpb::{
        AdminCmdType, AdminRequest, CmdType, RaftCmdRequest, RaftCmdResponse, Request, SplitRequest,
    },
};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::PeerMsg;
use tikv_util::{config::ReadableSize, store::new_peer};
use txn_types::Key;

use crate::{
    cluster::{v2_default_config, Cluster, TestRouter},
    test_transfer_leader::must_transfer_leader,
};

fn new_batch_split_region_request(
    split_keys: Vec<Vec<u8>>,
    ids: Vec<pdpb::SplitId>,
    right_derive: bool,
) -> AdminRequest {
    let mut req = AdminRequest::default();
    req.set_cmd_type(AdminCmdType::BatchSplit);
    req.mut_splits().set_right_derive(right_derive);
    let mut requests = Vec::with_capacity(ids.len());
    for (mut id, key) in ids.into_iter().zip(split_keys) {
        let mut split = SplitRequest::default();
        split.set_split_key(key);
        split.set_new_region_id(id.get_new_region_id());
        split.set_new_peer_ids(id.take_new_peer_ids());
        requests.push(split);
    }
    req.mut_splits().set_requests(requests.into());
    req
}

fn must_split(region_id: u64, req: RaftCmdRequest, cluster: &Cluster, router: &mut TestRouter) {
    let (msg, mut sub) = PeerMsg::raft_command(req);
    router.send(region_id, msg).unwrap();
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_proposed()));

    cluster.trig_heartbeat(0, region_id);
    cluster.dispatch(region_id, vec![]);

    cluster.trig_heartbeat(0, region_id);
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_committed()));
    block_on(sub.result()).unwrap();

    // TODO: when persistent implementation is ready, we can use tablet index of
    // the parent to check whether the split is done. Now, just sleep a second.
    thread::sleep(Duration::from_millis(100));
}

fn put_data(
    cluster: &Cluster,
    region_id: u64,
    node_off: usize,
    node_off_for_verify: usize,
    key: &[u8],
) {
    put_data_with_value(
        cluster,
        region_id,
        node_off,
        node_off_for_verify,
        key,
        b"value",
    );
}

fn put_data_with_value(
    cluster: &Cluster,
    region_id: u64,
    node_off: usize,
    node_off_for_verify: usize,
    key: &[u8],
    value: &[u8],
) {
    let router = cluster.router(node_off);
    let mut req = router.new_request_for(region_id);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(key.to_vec());
    put_req.mut_put().set_value(value.to_vec());
    req.mut_requests().push(put_req);

    router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    let tablet_factory = cluster.node(node_off).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert!(tablet.get_value(key).unwrap().is_none());
    let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    router.send(region_id, msg).unwrap();
    cluster.dispatch(region_id, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(block_on(sub.wait_proposed()));

    std::thread::sleep(std::time::Duration::from_millis(20));
    cluster.trig_heartbeat(0, region_id);
    cluster.dispatch(region_id, vec![]);
    // triage send snapshot
    std::thread::sleep(std::time::Duration::from_millis(100));
    cluster.trig_heartbeat(0, region_id);
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_committed()));

    let resp = block_on(sub.result()).unwrap();

    assert!(!resp.get_header().has_error(), "{:?}", resp);
    assert_eq!(
        tablet
            .get_value(keys::data_key(key).as_slice())
            .unwrap()
            .unwrap(),
        value
    );
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Verify the data is ready in the other node
    cluster.trig_heartbeat(node_off, region_id);
    cluster.dispatch(region_id, vec![]);
    let tablet_factory = cluster.node(node_off_for_verify).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert_eq!(
        tablet
            .get_value(keys::data_key(key).as_slice())
            .unwrap()
            .unwrap(),
        value
    );
}

fn put(router: &mut TestRouter, cluster: &Cluster, region_id: u64, key: &[u8]) -> RaftCmdResponse {
    let mut req = router.new_request_for(region_id);

    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(key.to_vec());
    put_req.mut_put().set_value(b"v1".to_vec());
    req.mut_requests().push(put_req);

    let (msg, mut sub) = PeerMsg::raft_command(req.clone());
    router.send(region_id, msg).unwrap();
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_proposed()));
    cluster.dispatch(region_id, vec![]);
    assert!(block_on(sub.wait_committed()));
    cluster.dispatch(region_id, vec![]);
    block_on(sub.result()).unwrap()
}

// Split the region according to the parameters
// return the updated original region
fn split_region(
    cluster: &Cluster,
    router: &mut TestRouter,
    region: metapb::Region,
    peer: metapb::Peer,
    split_region_id: u64,
    split_peer: metapb::Peer,
    left_key: &[u8],
    right_key: &[u8],
    split_key: &[u8],
    right_derive: bool,
) -> (metapb::Region, metapb::Region) {
    let region_id = region.id;
    let mut req = RaftCmdRequest::default();
    req.mut_header().set_region_id(region_id);
    req.mut_header()
        .set_region_epoch(region.get_region_epoch().clone());
    req.mut_header().set_peer(peer);

    let mut split_id = pdpb::SplitId::new();
    split_id.new_region_id = split_region_id;
    split_id.new_peer_ids = vec![split_peer.id, split_peer.id + 1];
    let admin_req =
        new_batch_split_region_request(vec![split_key.to_vec()], vec![split_id], right_derive);
    req.mut_requests().clear();
    req.set_admin_request(admin_req);

    must_split(region_id, req, cluster, router);

    cluster.dispatch(region_id, vec![]);
    cluster.dispatch(split_region_id, vec![]);

    let (left, right) = if !right_derive {
        (
            router.region_detail(region_id),
            router.region_detail(split_region_id),
        )
    } else {
        (
            router.region_detail(split_region_id),
            router.region_detail(region_id),
        )
    };

    // The end key of left region is `split_key`
    // So writing `right_key` will fail
    let resp = put(router, cluster, left.id, right_key);
    assert!(resp.get_header().has_error(), "{:?}", resp);
    // But `left_key` should succeed
    let resp = put(router, cluster, left.id, left_key);
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // Mirror of above case
    let resp = put(router, cluster, right.id, left_key);
    assert!(resp.get_header().has_error(), "{:?}", resp);
    let resp = put(router, cluster, right.id, right_key);
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    assert_eq!(left.get_end_key(), split_key);
    assert_eq!(right.get_start_key(), split_key);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    (left, right)
}

#[test]
fn test_split() {
    let cluster = Cluster::with_node_count(3, None);
    let mut router0 = cluster.router(0);
    let region_id = cluster.root_region_id();

    // Add another peer node
    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddNode);
    let peer1 = new_peer(cluster.node(1).id(), 5);
    admin_req.mut_change_peer().set_peer(peer1.clone());
    let req_clone = req.clone();
    let resp = router0.command(region_id, req_clone).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, peer1.clone()]);
    let peer0_id = meta.raft_status.id;

    cluster.dispatch(region_id, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let mut router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(peer0_id, meta.raft_status.soft_state.leader_id);
    assert_eq!(meta.raft_status.id, peer1.id, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);

    put_data(&cluster, region_id, 0, 1, b"key1");

    let store_id = cluster.node(0).id();
    let peer_id = cluster.node(0).peer_id(region_id).unwrap();
    let peer = new_peer(store_id, peer_id);
    let region = router0.region_detail(region_id);
    router0.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    let (left, right) = split_region(
        &cluster,
        &mut router0,
        region,
        peer,
        2000,
        new_peer(store_id, 10),
        b"k11",
        b"k33",
        b"k22",
        false,
    );

    println!("must_transfer_leader peer1");
    // Perform transfer leader
    must_transfer_leader(&cluster, region_id, 0, 1, peer1.clone());

    let _ = split_region(
        &cluster,
        &mut router1,
        left,
        peer1,
        2001,
        new_peer(store_id, 15),
        b"k00",
        b"k11",
        b"k11",
        false,
    );

    // region_id 1000, store 4, peer_id 11
    println!("must_transfer_leader peer2");
    let peer2 = new_peer(cluster.node(1).id(), 11);
    must_transfer_leader(&cluster, 2000, 0, 1, peer2.clone());

    let _ = split_region(
        &cluster,
        &mut router1,
        right,
        peer2,
        2002,
        new_peer(store_id, 20),
        b"k22",
        b"k33",
        b"k33",
        false,
    );
}

#[test]
fn test_split_check() {
    let mut config = v2_default_config();
    config.region_split_size = ReadableSize::mb(2);
    config.region_max_size = ReadableSize::mb(4);
    let val = vec![0; 1024 * 1024];
    let val = val.as_slice();
    let cluster = Cluster::with_node_count(1, Some(config));
    let router0 = cluster.router(0);
    let node = cluster.node(0);
    let tablet_factory = node.tablet_factory();
    let region_id = node.root_region_id();
    for i in 0..5 {
        let key = Key::from_raw(format!("{:05}", i).as_bytes());
        let key = key.append_ts(1.into());
        let key = key.as_encoded();
        put_data_with_value(&cluster, region_id, 0, 0, key, val);
        tablet_factory.for_each_opened_tablet(&mut |_id, _suffix, db| {
            db.flush_cfs(true).unwrap();
        });
    }

    router0.wait_applied_to_current_term(region_id, Duration::from_secs(3));

    let region = router0.region_detail(region_id);
    assert!(region.get_start_key().is_empty() && region.get_end_key().is_empty());

    cluster.trig_split_check(0, region_id);
    for _i in 0..5 {
        std::thread::sleep(std::time::Duration::from_millis(100));
        cluster.trig_heartbeat(0, region_id);
    }
    std::thread::sleep(std::time::Duration::from_millis(1000));
    let region = router0.region_detail(region_id);
    assert!(!region.get_start_key().is_empty() || !region.get_end_key().is_empty());
}

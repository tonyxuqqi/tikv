// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    assert_matches::assert_matches,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use engine_traits::{OpenOptions, Peekable, TabletFactory};
use futures::executor::block_on;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::{PeerMsg, PeerTick};
use tikv_util::store::{new_learner_peer, new_peer};

use crate::cluster::Cluster;

#[test]
fn test_add_learner() {
    let cluster = Cluster::with_node_count(2, None);
    let region_id = cluster.root_region_id();
    let router0 = cluster.router(0);
    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = router0.command(region_id, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    let match_index = meta.raft_apply.applied_index;
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // So heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );
    // Trigger the raft tick to replica the log to the learner and execute the
    // snapshot task.
    router0
        .send(region_id, PeerMsg::Tick(PeerTick::Raft))
        .unwrap();
    cluster.dispatch(region_id, vec![]);

    // write one kv after snapshot
    let (key, val) = (b"key", b"value");
    let mut write_req = router0.new_request_for(region_id);
    let mut put_req = Request::default();
    put_req.set_cmd_type(CmdType::Put);
    put_req.mut_put().set_key(key.to_vec());
    put_req.mut_put().set_value(val.to_vec());
    write_req.mut_requests().push(put_req);
    let (msg, _) = PeerMsg::raft_command(write_req.clone());
    router0.send(region_id, msg).unwrap();
    std::thread::sleep(Duration::from_millis(1000));
    cluster.dispatch(region_id, vec![]);

    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    // the learner truncated index muse be equal the leader applied index and can
    // read the new written kv.
    assert_eq!(match_index, meta.raft_apply.truncated_state.index);
    assert!(meta.raft_apply.applied_index >= match_index);
    let tablet_factory = cluster.node(1).tablet_factory();
    let tablet = tablet_factory
        .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
        .unwrap();
    assert_eq!(
        tablet
            .get_value(keys::data_key(key).as_slice())
            .unwrap()
            .unwrap(),
        val
    );

    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = router0.command(region_id, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer]);
}

#[test]
fn test_simple_change() {
    let cluster = Cluster::with_node_count(2, None);
    let router0 = cluster.router(0);
    let region_id = cluster.root_region_id();
    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddLearnerNode);
    let store_id = cluster.node(1).id();
    let new_peer = new_learner_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(new_peer.clone());
    let resp = router0.command(region_id, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, new_peer]);

    // So heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );

    req.mut_header()
        .mut_region_epoch()
        .set_conf_ver(new_conf_ver);
    req.mut_admin_request()
        .mut_change_peer()
        .set_change_type(ConfChangeType::RemoveNode);
    let resp = router0.command(region_id, req.clone()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer]);
    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.
}

#[test]
fn test_config_change_and_apply_snapshot() {
    let cluster = Cluster::with_node_count(3, None);
    let router0 = cluster.router(0);
    let flag = Arc::new(AtomicBool::new(false));
    let region_id = cluster.root_region_id();
    // let th = std::thread::spawn(|| {
    // loop {
    // if flag.load(Ordering::Relaxed) {
    // break;
    // }
    // cluster_ref.dispatch(2, vec![]);
    // std::thread::sleep(std::time::Duration::from_millis(20));
    // }
    // });
    {
        let router = cluster.router(0);
        let mut req = router.new_request_for(region_id);
        let mut put_req = Request::default();
        put_req.set_cmd_type(CmdType::Put);
        put_req.mut_put().set_key(b"key".to_vec());
        put_req.mut_put().set_value(b"value".to_vec());
        req.mut_requests().push(put_req);

        router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

        let tablet_factory = cluster.node(0).tablet_factory();
        let tablet = tablet_factory
            .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert!(
            tablet
                .get_value(keys::data_key(b"key").as_slice())
                .unwrap()
                .is_none()
        );
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(region_id, msg).unwrap();
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(
            tablet
                .get_value(keys::data_key(b"key").as_slice())
                .unwrap()
                .unwrap(),
            b"value"
        );

        let mut delete_req = Request::default();
        delete_req.set_cmd_type(CmdType::Delete);
        delete_req.mut_delete().set_key(b"key".to_vec());
        req.clear_requests();
        req.mut_requests().push(delete_req);
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(region_id, msg).unwrap();
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_matches!(
            tablet.get_value(keys::data_key(b"key").as_slice()),
            Ok(None)
        );
    }
    println!("finish write test");
    let mut req = router0.new_request_for(region_id);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddNode);
    let store_id = cluster.node(1).id();
    let peer1 = new_peer(store_id, 10);
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
    assert_eq!(meta.region_state.peers, vec![leader_peer, peer1]);

    // So heartbeat will create a learner.
    cluster.dispatch(region_id, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(region_id, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.raft_status.id, 10, "{:?}", meta);
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(
        meta.raft_status.soft_state.leader_id,
        req.get_header().get_peer().get_id()
    );
    println!("finish adding a peer");

    std::thread::sleep(std::time::Duration::from_millis(20));
    // cluster.dispatch(2, vec![]);
    {
        println!("start 2nd write test");
        let router = cluster.router(0);
        let mut req = router.new_request_for(region_id);
        let mut put_req = Request::default();
        put_req.set_cmd_type(CmdType::Put);
        put_req.mut_put().set_key(b"key".to_vec());
        put_req.mut_put().set_value(b"value".to_vec());
        req.mut_requests().push(put_req);

        router.wait_applied_to_current_term(region_id, Duration::from_secs(3));

        let tablet_factory = cluster.node(0).tablet_factory();
        let tablet = tablet_factory
            .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert!(
            tablet
                .get_value(keys::data_key(b"key").as_slice())
                .unwrap()
                .is_none()
        );
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(region_id, msg).unwrap();
        cluster.dispatch(region_id, vec![]);
        std::thread::sleep(std::time::Duration::from_millis(100));
        println!("before wait proposed");
        assert!(block_on(sub.wait_proposed()));
        std::thread::sleep(std::time::Duration::from_millis(100));
        cluster.trig_heartbeat(0, region_id);
        cluster.dispatch(region_id, vec![]);
        println!("before wait_committed");
        // triage send snapshot
        std::thread::sleep(std::time::Duration::from_millis(1000));
        cluster.trig_heartbeat(0, region_id);
        cluster.dispatch(region_id, vec![]);
        assert!(block_on(sub.wait_committed()));
        println!("after wait_committed");
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(
            tablet
                .get_value(keys::data_key(b"key").as_slice())
                .unwrap()
                .unwrap(),
            b"value"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));

        // use tablet with node 1 to verify the write.
        cluster.trig_heartbeat(0, region_id);
        cluster.dispatch(region_id, vec![]);

        let tablet_factory_1 = cluster.node(1).tablet_factory();
        let tablet_1 = tablet_factory_1
            .open_tablet(region_id, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert_eq!(
            tablet_1
                .get_value(keys::data_key(b"key").as_slice())
                .unwrap()
                .unwrap(),
            b"value"
        );

        let mut delete_req = Request::default();
        delete_req.set_cmd_type(CmdType::Delete);
        delete_req.mut_delete().set_key(b"key".to_vec());
        req.clear_requests();
        req.mut_requests().push(delete_req);
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(region_id, msg).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        cluster.dispatch(region_id, vec![]);
        println!("before wait proposed delete");
        assert!(block_on(sub.wait_proposed()));
        std::thread::sleep(std::time::Duration::from_millis(10));
        cluster.dispatch(region_id, vec![]);
        println!("before wait_committed delete");
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_matches!(
            tablet.get_value(keys::data_key(b"key").as_slice()),
            Ok(None)
        );
    }
    println!("finish 2nd write test");

    // cluster.dispatch(2, vec![]);
    flag.store(true, Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(20));
    println!("finish last dispatch");
    // th.join();

    // let store_id = cluster.node(2).id();
    // let peer2 = new_peer(store_id, 10);
    // admin_req.mut_change_peer().set_peer(peer2.clone());
    // let resp = router0.command(2, req.clone()).unwrap();
    // assert!(!resp.get_header().has_error(), "{:?}", resp);
    //
    // So heartbeat will create a learner.
    // cluster.dispatch(2, vec![]);
    // let router1 = cluster.router(2);
    // let meta = router1
    // .must_query_debug_info(2, Duration::from_secs(3))
    // .unwrap();
    // assert_eq!(meta.raft_status.id, 11, "{:?}", meta);

    // req.mut_header()
    // .mut_region_epoch()
    // .set_conf_ver(new_conf_ver);
    // req.mut_admin_request()
    // .mut_change_peer()
    // .set_change_type(ConfChangeType::RemoveNode);
    // let resp = router0.command(2, req.clone()).unwrap();
    // assert!(!resp.get_header().has_error(), "{:?}", resp);
    // let epoch = req.get_header().get_region_epoch();
    // let new_conf_ver = epoch.get_conf_ver() + 1;
    // let leader_peer = req.get_header().get_peer().clone();
    // let meta = router0
    // .must_query_debug_info(2, Duration::from_secs(3))
    // .unwrap();
    // assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    // assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    // assert_eq!(meta.region_state.peers, vec![leader_peer]);
    // TODO: check if the peer is removed once life trace is implemented or
    // snapshot is implemented.
}

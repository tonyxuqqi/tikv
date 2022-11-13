// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{assert_matches::assert_matches, time::Duration};
use std::sync::{Arc, atomic::AtomicBool, atomic::Ordering};

use engine_traits::{OpenOptions, Peekable, TabletFactory};
use futures::executor::block_on;
use kvproto::raft_cmdpb::{AdminCmdType, CmdType, Request};
use raft::prelude::ConfChangeType;
use raftstore_v2::router::PeerMsg;
use tikv_util::store::new_peer;

use crate::cluster::Cluster;

#[test]
fn test_simple_change() {
    let cluster = Cluster::with_node_count(2, None, false);
    let router0 = cluster.router(0);
    let flag = Arc::new(AtomicBool::new(false));
    {
        let router = cluster.router(0);
        let mut req = router.new_request_for(2);
        let mut put_req = Request::default();
        put_req.set_cmd_type(CmdType::Put);
        put_req.mut_put().set_key(b"key".to_vec());
        put_req.mut_put().set_value(b"value".to_vec());
        req.mut_requests().push(put_req);

        router.wait_applied_to_current_term(2, Duration::from_secs(3));

        let tablet_factory = cluster.node(0).tablet_factory();
        let tablet = tablet_factory
            .open_tablet(2, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert!(tablet.get_value(b"key").unwrap().is_none());
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(2, msg).unwrap();
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(tablet.get_value(b"key").unwrap().unwrap(), b"value");

        let mut delete_req = Request::default();
        delete_req.set_cmd_type(CmdType::Delete);
        delete_req.mut_delete().set_key(b"key".to_vec());
        req.clear_requests();
        req.mut_requests().push(delete_req);
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(2, msg).unwrap();
        assert!(block_on(sub.wait_proposed()));
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_matches!(tablet.get_value(b"key"), Ok(None));
    }
    println!("finish write test");
    let mut req = router0.new_request_for(2);
    let admin_req = req.mut_admin_request();
    admin_req.set_cmd_type(AdminCmdType::ChangePeer);
    admin_req
        .mut_change_peer()
        .set_change_type(ConfChangeType::AddNode);
    let store_id = cluster.node(1).id();
    let peer1 = new_peer(store_id, 10);
    admin_req.mut_change_peer().set_peer(peer1.clone());
    let req_clone = req.clone();
    let resp = router0.command(2, req_clone).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    let epoch = req.get_header().get_region_epoch();
    let new_conf_ver = epoch.get_conf_ver() + 1;
    let leader_peer = req.get_header().get_peer().clone();
    let meta = router0
        .must_query_debug_info(2, Duration::from_secs(3))
        .unwrap();
    assert_eq!(meta.region_state.epoch.version, epoch.get_version());
    assert_eq!(meta.region_state.epoch.conf_ver, new_conf_ver);
    assert_eq!(meta.region_state.peers, vec![leader_peer, peer1]);

    // So heartbeat will create a learner.
    cluster.dispatch(2, vec![]);
    std::thread::sleep(std::time::Duration::from_millis(20));
    let router1 = cluster.router(1);
    let meta = router1
        .must_query_debug_info(2, Duration::from_secs(3))
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
    //cluster.dispatch(2, vec![]);
    {
        println!("start 2nd write test");
        let router = cluster.router(0);
        let mut req = router.new_request_for(2);
        let mut put_req = Request::default();
        put_req.set_cmd_type(CmdType::Put);
        put_req.mut_put().set_key(b"key".to_vec());
        put_req.mut_put().set_value(b"value".to_vec());
        req.mut_requests().push(put_req);

        router.wait_applied_to_current_term(2, Duration::from_secs(3));

        let tablet_factory = cluster.node(0).tablet_factory();
        let tablet = tablet_factory
            .open_tablet(2, None, OpenOptions::default().set_cache_only(true))
            .unwrap();
        assert!(tablet.get_value(b"key").unwrap().is_none());
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(2, msg).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        cluster.dispatch(2, vec![]);
        println!("before wait proposed");
        assert!(block_on(sub.wait_proposed()));
        for i in 0..60 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            cluster.dispatch(2, vec![]);
        }
        println!("before wait_committed");
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(tablet.get_value(b"key").unwrap().unwrap(), b"value");

        let mut delete_req = Request::default();
        delete_req.set_cmd_type(CmdType::Delete);
        delete_req.mut_delete().set_key(b"key".to_vec());
        req.clear_requests();
        req.mut_requests().push(delete_req);
        let (msg, mut sub) = PeerMsg::raft_command(req.clone());
        router.send(2, msg).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        cluster.dispatch(2, vec![]);
        println!("before wait proposed delete");
        assert!(block_on(sub.wait_proposed()));
        println!("before wait_committed delete");
        assert!(block_on(sub.wait_committed()));
        let resp = block_on(sub.result()).unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_matches!(tablet.get_value(b"key"), Ok(None));
    }
    println!("finish 2nd write test");

    //cluster.dispatch(2, vec![]);
    flag.store(true, Ordering::Relaxed);
    std::thread::sleep(std::time::Duration::from_millis(20));
    println!("finish last dispatch");
    //th.join();

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

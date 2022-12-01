// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Write,
    sync::{Arc, Mutex},
};

use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::{TabletFactory, CF_DEFAULT};
use file_system::IoRateLimiter;
use raftstore_v2::{
    router::{RaftRouter, StoreRouterCompactedEventSender},
    StoreRouter,
};
use rand::RngCore;
use server::server_v2::ConfiguredRaftEngine;
use tempfile::TempDir;
use test_raftstore::{new_put_cf_cmd, Config};
use tikv::server::KvEngineFactoryBuilder;

use crate::cluster::Cluster;

pub fn create_test_engine(
    // TODO: pass it in for all cases.
    router: Option<StoreRouter<RocksEngine, RaftTestEngine>>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    Arc<dyn TabletFactory<RocksEngine> + Sync + Send>,
    RaftTestEngine,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let mut cfg = cfg.clone();
    cfg.storage.data_dir = dir.path().to_str().unwrap().to_string();
    cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
    cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
    let key_manager =
        data_key_manager_from_config(&cfg.security.encryption, dir.path().to_str().unwrap())
            .unwrap()
            .map(Arc::new);

    let env = cfg
        .build_shared_rocks_env(key_manager.clone(), limiter)
        .unwrap();
    let cache = cfg.storage.block_cache.build_shared_cache();
    let raft_engine = RaftTestEngine::build(&cfg, &env, &key_manager, &cache);

    let mut builder = KvEngineFactoryBuilder::new(env, &cfg.tikv, dir.path());
    if let Some(cache) = cache {
        builder = builder.block_cache(cache);
    }
    if let Some(router) = router {
        builder = builder.compaction_event_sender(Arc::new(StoreRouterCompactedEventSender {
            router: Mutex::new(router),
        }));
    }

    // let sst_worker = LazyWorker::new("sst-recovery");
    // let scheduler = sst_worker.scheduler();
    let factory = Arc::new(builder.build_v2());

    (factory, raft_engine, key_manager, dir)
}

/// Keep putting random kvs until specified size limit is reached.
pub fn put_till_size(
    cluster: &mut Cluster,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    put_cf_till_size(cluster, CF_DEFAULT, limit, range)
}

pub fn put_cf_till_size(
    cluster: &mut Cluster,
    cf: &'static str,
    limit: u64,
    range: &mut dyn Iterator<Item = u64>,
) -> Vec<u8> {
    assert!(limit > 0);
    let mut len = 0;
    let mut rng = rand::thread_rng();
    let mut key = String::new();
    let mut value = vec![0; 64];
    while len < limit {
        let batch_size = std::cmp::min(1024, limit - len);
        let mut reqs = vec![];
        for _ in 0..batch_size / 74 + 1 {
            key.clear();
            let key_id = range.next().unwrap();
            write!(key, "{:09}", key_id).unwrap();
            rng.fill_bytes(&mut value);
            // plus 1 for the extra encoding prefix
            len += key.len() as u64 + 1;
            len += value.len() as u64;
            reqs.push(new_put_cf_cmd(cf, key.as_bytes(), &value));
        }
        cluster.batch_put(key.as_bytes(), reqs).unwrap();
        // Approximate size of memtable is inaccurate for small data,
        // we flush it to SST so we can use the size properties instead.
        cluster.must_flush_cf(cf, true);
    }
    key.into_bytes()
}

// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::KvEngine;
use crate::errors::Result;
use crate::options::WriteOptions;
use crate::raft_engine::RaftEngine;
use crate::write_batch::WriteBatch;
use std::fmt::{self, Debug};
use std::path::{Path, PathBuf};

pub trait TabletFactory<EK> {
    fn loop_tablet_cache(&self, _f: Box<dyn FnMut(u64, u64, &EK) + '_>) {}
    fn destroy_tablet(&self, _id: u64, _suffix: u64) -> crate::Result<()> {
        Ok(())
    }
    fn create_tablet(&self, id: u64, suffix: u64) -> EK;
    fn open_tablet(&self, id: u64, suffix: u64) -> EK {
        self.open_tablet_raw(&self.tablet_path(id, suffix), false)
    }
    fn open_tablet_cache(&self, id: u64, suffix: u64) -> Option<EK> {
        Some(self.open_tablet_raw(&self.tablet_path(id, suffix), false))
    }
    fn open_tablet_cache_any(&self, id: u64) -> Option<EK> {
        Some(self.open_tablet_raw(&self.tablet_path(id, 0), false))
    }
    fn open_tablet_raw(&self, path: &Path, readonly: bool) -> EK;
    fn create_root_db(&self) -> EK;
    #[inline]
    fn exists(&self, id: u64, suffix: u64) -> bool {
        self.exists_raw(&self.tablet_path(id, suffix))
    }
    fn exists_raw(&self, path: &Path) -> bool;
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf;
    fn tablets_path(&self) -> PathBuf;
    fn clone(&self) -> Box<dyn TabletFactory<EK> + Send>;
    fn load_tablet(&self, _path: &Path, id: u64, suffix: u64) -> EK {
        self.open_tablet(id, suffix)
    }
    fn mark_tombstone(&self, _region_id: u64, _suffix: u64) {}
    fn is_tombstoned(&self, _region_id: u64, _suffix: u64) -> bool {
        false
    }
}

pub struct DummyFactory<EK>
where
    EK: KvEngine,
{
    pub engine: Option<EK>,
    pub root_path: String,
}

impl<EK> TabletFactory<EK> for DummyFactory<EK>
where
    EK: KvEngine,
{
    fn create_tablet(&self, _id: u64, _suffix: u64) -> EK {
        return self.engine.as_ref().unwrap().clone();
    }
    fn open_tablet_raw(&self, _path: &Path, _readonly: bool) -> EK {
        return self.engine.as_ref().unwrap().clone();
    }
    fn create_root_db(&self) -> EK {
        return self.engine.as_ref().unwrap().clone();
    }
    fn exists_raw(&self, _path: &Path) -> bool {
        return true;
    }
    fn tablet_path(&self, _id: u64, _suffix: u64) -> PathBuf {
        return PathBuf::from(&self.root_path);
    }
    fn tablets_path(&self) -> PathBuf {
        return PathBuf::from(&self.root_path);
    }

    fn clone(&self) -> Box<dyn TabletFactory<EK> + Send> {
        if self.engine.is_none() {
            return Box::<DummyFactory<EK>>::new(DummyFactory {
                engine: None,
                root_path: self.root_path.clone(),
            });
        }
        Box::<DummyFactory<EK>>::new(DummyFactory {
            engine: Some(self.engine.as_ref().unwrap().clone()),
            root_path: self.root_path.clone(),
        })
    }
}

impl<EK> DummyFactory<EK>
where
    EK: KvEngine,
{
    pub fn new() -> DummyFactory<EK> {
        DummyFactory {
            engine: None,
            root_path: "/dummy_root".to_string(),
        }
    }
}

pub struct Engines<K, R> {
    pub kv: K,
    pub raft: R,
    pub tablets: Box<dyn TabletFactory<K> + Send>,
}

impl<K: KvEngine, R: RaftEngine> Engines<K, R> {
    pub fn new(kv_engine: K, raft_engine: R) -> Self {
        let path = kv_engine.path().to_string();
        Engines {
            kv: kv_engine.clone(),
            raft: raft_engine,
            tablets: Box::<DummyFactory<K>>::new(DummyFactory {
                engine: Some(kv_engine),
                root_path: path,
            }),
        }
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        wb.write()
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        wb.write_opt(opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }
}

impl<K: Clone, R: Clone> Clone for Engines<K, R> {
    #[inline]
    fn clone(&self) -> Engines<K, R> {
        Engines {
            kv: self.kv.clone(),
            raft: self.raft.clone(),
            tablets: self.tablets.clone(),
        }
    }
}

impl<K: Debug, R: Debug> Debug for Engines<K, R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Engines")
            .field("kv", &self.kv)
            .field("raft", &self.raft)
            .field("tablets", &self.tablets.tablets_path().display())
            .finish()
    }
}

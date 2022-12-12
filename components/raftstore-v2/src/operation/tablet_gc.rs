// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module drives the compaction of Raft logs stored in entry cache and
//! Raft engine.

use std::time::Duration;

use engine_traits::{KvEngine, RaftEngine};
use raftstore::store::Transport;
use slog::info;
use tikv_util::time::duration_to_sec;

use crate::{
    batch::StoreContext,
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{PeerMsg, PeerTick},
};

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_tablet_gc(&mut self) {
        self.fsm.peer_mut().tablet_gc_imp(self.store_ctx);
        self.schedule_tick(PeerTick::TabletGc);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    fn tablet_gc_imp<T>(&mut self, store_ctx: &mut StoreContext<EK, ER, T>) {
        if (self.pending_gc_tablets().is_empty()
            || duration_to_sec(
                self.pending_gc_tablets()
                    .front()
                    .unwrap()
                    .0
                    .saturating_elapsed(),
            ) < 90.0)
        {
            return;
        }

        let path = self.pending_gc_tablets().front().unwrap().1.as_str();
        info!(self.logger, "removing tablet"; "dir" => path);
        let _ = std::fs::remove_dir_all(path);
        self.pending_gc_tablets_mut().pop_front();
    }
}

// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display, Formatter},
    io::Error as IoError,
    mem,
    num::NonZeroU64,
    result,
    sync::Arc,
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::{CfName, KvEngine, MvccProperties, Snapshot, TabletFactory};
use kvproto::{
    errorpb,
    kvrpcpb::{Context, IsolationLevel},
    metapb,
    raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, RaftRequestHeader, Request, Response},
};
use raft::{
    eraftpb::{self, MessageType},
    StateRole,
};
use raftstore::{
    coprocessor::{
        dispatcher::BoxReadIndexObserver, Coprocessor, CoprocessorHost, ReadIndexObserver,
    },
    errors::Error as RaftServerError,
    router::{LocalReadRouter, RaftStoreRouter},
    store::{
        Callback as StoreCallback, RaftCmdExtraOpts, ReadIndexContext, ReadResponse,
        RegionSnapshot, WriteResponse,
    },
};
use thiserror::Error;
use tikv_util::{codec::number::NumberEncoder, time::Instant};
use txn_types::{Key, TimeStamp, TxnExtra, TxnExtraScheduler, WriteBatchFlags};

use super::metrics::*;
use crate::storage::{
    self, kv,
    kv::{
        write_modifies, Callback, Engine, Error as KvError, ErrorInner as KvErrorInner,
        ExtCallback, Modify, SnapContext, WriteData,
    },
};
use crate::server::raftkv::*;

/// `RaftKv` is a storage engine base on `RaftStore`.
#[derive(Clone)]
pub struct RaftTablet<E>
where
    E: KvEngine,
{
    txn_extra_scheduler: Option<Arc<dyn TxnExtraScheduler>>,
    tablet_factory: Box<dyn TabletFactory<E> + Send>,
}

impl<E> RaftTablet<E>
where
    E: KvEngine,
{
    /// Create a RaftKv using specified configuration.
    pub fn new(tablet_factory: Box<dyn TabletFactory<E> + Send>) -> RaftTablet<E> {
        RaftTablet {
            txn_extra_scheduler: None,
            tablet_factory,
        }
    }

    pub fn set_txn_extra_scheduler(&mut self, txn_extra_scheduler: Arc<dyn TxnExtraScheduler>) {
        self.txn_extra_scheduler = Some(txn_extra_scheduler);
    } 
}

impl<E> Display for RaftTablet<E>
where
    E: KvEngine,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftTablet")
    }
}

impl<E> Debug for RaftTablet<E>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RaftTablet")
    }
}

impl<E, S> Engine for RaftTablet<E, S>
where
    E: KvEngine,
    S: RaftStoreRouter<E> + LocalReadRouter<E> + 'static,
{
    type Snap = RegionSnapshot<E::Snapshot>;
    type Local = E;

    fn kv_engine(&self) -> E {
        unimplemented!() 
    }

    fn kv_tablet(&self, region_id: u64) -> Option<E> {
        self.tablet_factory.open_tablet_cache_latest(region_id)
    }

    fn snapshot_on_kv_engine(&self, start_key: &[u8], end_key: &[u8]) -> kv::Result<Self::Snap> {
        unimplemented!()
    }

    fn modify_on_kv_engine(&self, mut modifies: Vec<Modify>) -> kv::Result<()> {
        unimplemented!()
    }

    fn async_write(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
    ) -> kv::Result<()> {
        self.async_write_ext(ctx, batch, write_cb, None, None)
    }

    fn async_write_ext(
        &self,
        ctx: &Context,
        batch: WriteData,
        write_cb: Callback<()>,
        proposed_cb: Option<ExtCallback>,
        committed_cb: Option<ExtCallback>,
    ) -> kv::Result<()> {
        fail_point!("raftkv_async_write");
        if batch.modifies.is_empty() {
            return Err(KvError::from(KvErrorInner::EmptyRequest));
        }

        ASYNC_REQUESTS_COUNTER_VEC.write.all.inc();
        let begin_instant = Instant::now_coarse();

        self.exec_write_requests(
            ctx,
            batch,
            Box::new(move |res| match res {
                Ok(CmdRes::Resp(_)) => {
                    ASYNC_REQUESTS_COUNTER_VEC.write.success.inc();
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .write
                        .observe(begin_instant.saturating_elapsed_secs());
                    fail_point!("raftkv_async_write_finish");
                    write_cb(Ok(()))
                }
                Ok(CmdRes::Snap(_)) => {
                    write_cb(Err(box_err!("unexpect snapshot, should mutate instead.")))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
                    write_cb(Err(e))
                }
            }),
            proposed_cb,
            committed_cb,
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.write.get(status_kind).inc();
            e.into()
        })
    }

    fn async_snapshot(&self, mut ctx: SnapContext<'_>, cb: Callback<Self::Snap>) -> kv::Result<()> {
        fail_point!("raftkv_async_snapshot_err", |_| Err(box_err!(
            "injected error for async_snapshot"
        )));

        let mut req = Request::default();
        req.set_cmd_type(CmdType::Snap);
        if !ctx.key_ranges.is_empty() && !ctx.start_ts.is_zero() {
            req.mut_read_index().set_start_ts(ctx.start_ts.into_inner());
            req.mut_read_index()
                .set_key_ranges(mem::take(&mut ctx.key_ranges).into());
        }
        ASYNC_REQUESTS_COUNTER_VEC.snapshot.all.inc();
        let begin_instant = Instant::now_coarse();
        self.exec_snapshot(
            ctx,
            req,
            Box::new(move |res| match res {
                Ok(CmdRes::Resp(mut r)) => {
                    let e = if r
                        .get(0)
                        .map(|resp| resp.get_read_index().has_locked())
                        .unwrap_or(false)
                    {
                        let locked = r[0].take_read_index().take_locked();
                        KvError::from(KvErrorInner::KeyIsLocked(locked))
                    } else {
                        invalid_resp_type(CmdType::Snap, r[0].get_cmd_type()).into()
                    };
                    cb(Err(e))
                }
                Ok(CmdRes::Snap(s)) => {
                    ASYNC_REQUESTS_DURATIONS_VEC
                        .snapshot
                        .observe(begin_instant.saturating_elapsed_secs());
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.success.inc();
                    cb(Ok(s))
                }
                Err(e) => {
                    let status_kind = get_status_kind_from_engine_error(&e);
                    ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
                    cb(Err(e))
                }
            }),
        )
        .map_err(|e| {
            let status_kind = get_status_kind_from_error(&e);
            ASYNC_REQUESTS_COUNTER_VEC.snapshot.get(status_kind).inc();
            e.into()
        })
    }

    fn release_snapshot(&self) {
        self.router.release_snapshot_cache();
    }

    fn get_mvcc_properties_cf(
        &self,
        cf: CfName,
        safe_point: TimeStamp,
        start: &[u8],
        end: &[u8],
    ) -> Option<MvccProperties> {
        let start = keys::data_key(start);
        let end = keys::data_end_key(end);
        self.engine
            .get_mvcc_properties_cf(cf, safe_point, &start, &end)
    }

    fn schedule_txn_extra(&self, txn_extra: TxnExtra) {
        if let Some(tx) = self.txn_extra_scheduler.as_ref() {
            if !txn_extra.is_empty() {
                tx.schedule(txn_extra);
            }
        }
    }
}

#[derive(Clone)]
pub struct ReplicaReadLockChecker {
    concurrency_manager: ConcurrencyManager,
}

impl ReplicaReadLockChecker {
    pub fn new(concurrency_manager: ConcurrencyManager) -> Self {
        ReplicaReadLockChecker {
            concurrency_manager,
        }
    }

    pub fn register<E: KvEngine + 'static>(self, host: &mut CoprocessorHost<E>) {
        host.registry
            .register_read_index_observer(1, BoxReadIndexObserver::new(self));
    }
}

impl Coprocessor for ReplicaReadLockChecker {}

impl ReadIndexObserver for ReplicaReadLockChecker {
    fn on_step(&self, msg: &mut eraftpb::Message, role: StateRole) {
        // Only check and return result if the current peer is a leader.
        // If it's not a leader, the read index request will be redirected to the leader later.
        if msg.get_msg_type() != MessageType::MsgReadIndex || role != StateRole::Leader {
            return;
        }
        assert_eq!(msg.get_entries().len(), 1);
        let mut rctx = ReadIndexContext::parse(msg.get_entries()[0].get_data()).unwrap();
        if let Some(mut request) = rctx.request.take() {
            let begin_instant = Instant::now();

            let start_ts = request.get_start_ts().into();
            self.concurrency_manager.update_max_ts(start_ts);
            for range in request.mut_key_ranges().iter_mut() {
                let key_bound = |key: Vec<u8>| {
                    if key.is_empty() {
                        None
                    } else {
                        Some(txn_types::Key::from_encoded(key))
                    }
                };
                let start_key = key_bound(range.take_start_key());
                let end_key = key_bound(range.take_end_key());
                // The replica read is not compatible with `RcCheckTs` isolation level yet.
                // It's ensured in the tidb side when `RcCheckTs` is enabled for read requests,
                // the replica read would not be enabled at the same time.
                let res = self.concurrency_manager.read_range_check(
                    start_key.as_ref(),
                    end_key.as_ref(),
                    |key, lock| {
                        txn_types::Lock::check_ts_conflict(
                            Cow::Borrowed(lock),
                            key,
                            start_ts,
                            &Default::default(),
                            IsolationLevel::Si,
                        )
                    },
                );
                if let Err(txn_types::Error(box txn_types::ErrorInner::KeyIsLocked(lock))) = res {
                    rctx.locked = Some(lock);
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .locked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                } else {
                    REPLICA_READ_LOCK_CHECK_HISTOGRAM_VEC_STATIC
                        .unlocked
                        .observe(begin_instant.saturating_elapsed().as_secs_f64());
                }
            }
            msg.mut_entries()[0].set_data(rctx.to_bytes().into());
        }
    }
}

#[cfg(test)]
mod tests {
    use kvproto::raft_cmdpb;
    use uuid::Uuid;

    use super::*;

    // This test ensures `ReplicaReadLockChecker` won't change UUID context of read index.
    #[test]
    fn test_replica_read_lock_checker_for_single_uuid() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let uuid = Uuid::new_v4();
        let mut e = eraftpb::Entry::default();
        e.set_data(uuid.as_bytes().to_vec().into());
        m.mut_entries().push(e);

        checker.on_step(&mut m, StateRole::Leader);
        assert_eq!(m.get_entries()[0].get_data(), uuid.as_bytes());
    }

    #[test]
    fn test_replica_read_lock_check_when_not_leader() {
        let cm = ConcurrencyManager::new(1.into());
        let checker = ReplicaReadLockChecker::new(cm);
        let mut m = eraftpb::Message::default();
        m.set_msg_type(MessageType::MsgReadIndex);
        let mut request = raft_cmdpb::ReadIndexRequest::default();
        request.set_start_ts(100);
        let rctx = ReadIndexContext {
            id: Uuid::new_v4(),
            request: Some(request),
            locked: None,
        };
        let mut e = eraftpb::Entry::default();
        e.set_data(rctx.to_bytes().into());
        m.mut_entries().push(e);

        checker.on_step(&mut m, StateRole::Follower);
        assert_eq!(m.get_entries()[0].get_data(), rctx.to_bytes());
    }
}

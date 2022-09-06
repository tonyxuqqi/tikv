// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! There are two types of Query: KV read and status query.
//!
//! KV Read is implemented in local module and query module.
//! Read will be executed in callee thread if in lease, which is
//! implemented in local module. If lease is expired, it will extend the lease
//! first. Lease maintainance is implemented in lease module.
//!
//! Status query is implemented in the root module directly.
//! Follower's read index and replica read is implemenented replica.rs.
//! Leader's read index and lease renew is implemented in lease.rs.
//! Stale read check is implemented stale.rs.

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_cmdpb::{CmdType, RaftCmdRequest, RaftCmdResponse, StatusCmdType};
use raftstore::{
    store::{
        cmd_resp, msg::ErrorCallback, region_meta::RegionMeta, util, util::LeaseState, GroupState,
        ReadCallback, RequestInspector, RequestPolicy,
    },
    Error, Result,
};
use tikv_util::box_err;
use txn_types::WriteBatchFlags;

use crate::{
    fsm::PeerFsmDelegate,
    raft::Peer,
    router::{DebugInfoChannel, QueryResChannel, QueryResult, ReadResponse},
};

mod lease;
mod local;
mod replica;
mod stale;

impl<'a, EK: KvEngine, ER: RaftEngine, T: raftstore::store::Transport>
    PeerFsmDelegate<'a, EK, ER, T>
{
    fn inspect_read(&mut self, req: &RaftCmdRequest) -> Result<RequestPolicy> {
        if req.has_admin_request() {
            return Err(box_err!("PeerMsg::RaftQuery does not allow admin requests"));
        }

        for r in req.get_requests() {
            if r.get_cmd_type() != CmdType::Get
                && r.get_cmd_type() != CmdType::Snap
                && r.get_cmd_type() != CmdType::ReadIndex
            {
                return Err(box_err!(
                    "PeerMsg::RaftQuery does not allow write requests: {:?}",
                    r.get_cmd_type()
                ));
            }
        }

        let flags = WriteBatchFlags::from_bits_check(req.get_header().get_flags());
        if flags.contains(WriteBatchFlags::STALE_READ) {
            return Ok(RequestPolicy::StaleRead);
        }

        if req.get_header().get_read_quorum() {
            return Ok(RequestPolicy::ReadIndex);
        }

        // TODO
        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        // if !self.has_applied_to_current_term() {
        // return Ok(RequestPolicy::ReadIndex);
        // }

        match self.fsm.peer_mut().inspect_lease() {
            LeaseState::Valid => Ok(RequestPolicy::ReadLocal),
            LeaseState::Expired | LeaseState::Suspect => {
                // Perform a consistent read to Raft quorum and try to renew the leader lease.
                Ok(RequestPolicy::ReadIndex)
            }
        }
    }

    #[inline]
    pub fn on_query(&mut self, req: RaftCmdRequest, ch: QueryResChannel) {
        if !req.has_status_request() {
            let policy = self.inspect_read(&req);
            match policy {
                Ok(RequestPolicy::ReadIndex) => {
                    self.fsm.peer_mut().read_index(self.store_ctx, req, ch);
                }
                Ok(RequestPolicy::ReadLocal) => {
                    self.store_ctx.raft_metrics.propose.local_read.inc();
                    let read_resp = ReadResponse::new(0);
                    ch.set_result(QueryResult::Read(read_resp));
                }
                Ok(RequestPolicy::StaleRead) => {
                    self.store_ctx.raft_metrics.propose.local_read.inc();
                    self.fsm.peer_mut().can_stale_read(req, true, None, ch);
                }
                Err(err) => {
                    let mut err_resp = RaftCmdResponse::default();
                    let term = self.fsm.peer().term();
                    cmd_resp::bind_term(&mut err_resp, term);
                    cmd_resp::bind_error(&mut err_resp, err);
                    ch.report_error(err_resp);
                }
                _ => {
                    unimplemented!();
                }
            };
        } else {
            self.fsm.peer_mut().on_query_status(&req, ch);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Status command is used to query target region information.
    #[inline]
    fn on_query_status(&mut self, req: &RaftCmdRequest, ch: QueryResChannel) {
        let mut response = RaftCmdResponse::default();
        if let Err(e) = self.query_status(req, &mut response) {
            cmd_resp::bind_error(&mut response, e);
        }
        ch.set_result(QueryResult::Response(response));
    }

    fn query_status(&mut self, req: &RaftCmdRequest, resp: &mut RaftCmdResponse) -> Result<()> {
        util::check_store_id(req, self.peer().get_store_id())?;
        let cmd_type = req.get_status_request().get_cmd_type();
        let status_resp = resp.mut_status_response();
        status_resp.set_cmd_type(cmd_type);
        match cmd_type {
            StatusCmdType::RegionLeader => {
                if let Some(leader) = self.leader() {
                    status_resp.mut_region_leader().set_leader(leader);
                }
            }
            StatusCmdType::RegionDetail => {
                if !self.storage().is_initialized() {
                    let region_id = req.get_header().get_region_id();
                    return Err(Error::RegionNotInitialized(region_id));
                }
                status_resp
                    .mut_region_detail()
                    .set_region(self.region().clone());
                if let Some(leader) = self.leader() {
                    status_resp.mut_region_detail().set_leader(leader);
                }
            }
            StatusCmdType::InvalidStatus => {
                return Err(box_err!("{:?} invalid status command!", self.logger.list()));
            }
        }

        // Bind peer current term here.
        cmd_resp::bind_term(resp, self.term());
        Ok(())
    }

    /// Query internal states for debugging purpose.
    pub fn on_query_debug_info(&self, ch: DebugInfoChannel) {
        let entry_storage = self.storage().entry_storage();
        let meta = RegionMeta::new(
            self.storage().region_state(),
            entry_storage.apply_state(),
            GroupState::Ordered,
            self.raft_group().status(),
        );
        ch.set_result(meta);
    }
}

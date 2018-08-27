// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use raftstore::store::peer_agent::StoreMeta;
use raftstore::store::msg::{PeerMsg, StoreMsg, Callback, SignificantMsg};
use kvproto::raft_cmdpb::RaftCmdRequest;
use kvproto::raft_serverpb::RaftMessage;
use util::mpsc::LooseBoundedSender;
use raft::SnapshotStatus;
use raftstore::Result;


pub trait RaftStoreRouter: Send + Clone {
    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> Result<()>;

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> Result<()>;

    // Report the peer of the region is unreachable.
    fn report_unreachable(&self, region_id: u64, to_peer_id: u64) -> Result<()>;

    // Report the sending snapshot status to the peer of the region.
    fn report_snapshot_status(
        &self,
        region_id: u64,
        to_peer_id: u64,
        status: SnapshotStatus,
    ) -> Result<()>;
}

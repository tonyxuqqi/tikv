// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::{RegionChangeEvent, RoleChange};

// to be removed later
pub trait LockManagerObserver: Send + Sync {
    fn on_role_change(&self, region: &Region, role_change: RoleChange);

    fn on_region_changed(&self, region: &Region, event: RegionChangeEvent, role: StateRole);
}

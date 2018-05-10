// Copyright 2016 PingCAP, Inc.
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

use super::{AdminObserver, Coprocessor, ObserverContext, Result as CopResult};
use coprocessor::codec::table;
use util::codec::bytes::{encode_bytes, BytesDecoder};
use util::escape;

use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
use std::result::Result as StdResult;

/// `SplitObserver` adjusts the split key so that it won't separate
/// the data of a row into two region. It adjusts the key according
/// to the key format of `TiDB`.
pub struct SplitObserver;

type Result<T> = StdResult<T, String>;

impl SplitObserver {
    fn on_split(&self, ctx: &mut ObserverContext, splits: &mut [SplitRequest]) -> Result<()> {
        for split in splits {
            if split.get_split_key().is_empty() {
                return Err("split key is expected!".to_owned());
            }

            let mut key = match split.get_split_key().decode_bytes(false) {
                Ok(x) => x,
                Err(_) => continue,
            };

            // format of a key is TABLE_PREFIX + table_id + RECORD_PREFIX_SEP + handle + column_id
            // + version or TABLE_PREFIX + table_id + INDEX_PREFIX_SEP + index_id + values + version
            // or meta_key + version
            // The length of TABLE_PREFIX + table_id is TABLE_PREFIX_KEY_LEN.
            if key.starts_with(table::TABLE_PREFIX) && key.len() > table::TABLE_PREFIX_KEY_LEN
                && key[table::TABLE_PREFIX_KEY_LEN..].starts_with(table::RECORD_PREFIX_SEP)
            {
                // row key, truncate to handle
                key.truncate(table::PREFIX_LEN + table::ID_LEN);
            }

            let region_start_key = ctx.region().get_start_key();

            let key = encode_bytes(&key);
            if *key <= *region_start_key {
                return Err(format!(
                    "no need to split: {} <= {}",
                    escape(&key),
                    escape(region_start_key)
                ));
            }

            split.set_split_key(key);
        }
        Ok(())
    }
}

impl Coprocessor for SplitObserver {}

impl AdminObserver for SplitObserver {
    fn pre_propose_admin(
        &self,
        ctx: &mut ObserverContext,
        req: &mut AdminRequest,
    ) -> CopResult<()> {
        match req.get_cmd_type() {
            AdminCmdType::Split => {
                return Err(box_err!(
                    "Command type Split is deprecated, use BatchSplit instead."
                ));
            }
            AdminCmdType::BatchSplit => {}
            _ => return Ok(()),
        }
        if !req.has_splits() {
            return Err(box_err!(
                "cmd_type is BatchSplit but it doesn't have splits request, message maybe \
                 corrupted!"
                    .to_owned()
            ));
        }
        if let Err(e) = self.on_split(ctx, req.mut_splits().mut_requests()) {
            error!("failed to handle split req: {:?}", e);
            return Err(box_err!(e));
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::{BigEndian, WriteBytesExt};
    use coprocessor::codec::{datum, table, Datum};
    use kvproto::metapb::Region;
    use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, SplitRequest};
    use raftstore::coprocessor::AdminObserver;
    use raftstore::coprocessor::ObserverContext;
    use util::codec::bytes::encode_bytes;
    use util::codec::number::NumberEncoder;

    fn new_split_request(key: &[u8]) -> AdminRequest {
        let mut req = AdminRequest::new();
        req.set_cmd_type(AdminCmdType::Split);
        let mut split_req = SplitRequest::new();
        split_req.set_split_key(key.to_vec());
        req.set_split(split_req);
        req
    }

    fn new_batch_split_request(keys: Vec<Vec<u8>>) -> AdminRequest {
        let mut req = AdminRequest::new();
        req.set_cmd_type(AdminCmdType::BatchSplit);
        for key in keys {
            let mut split_req = SplitRequest::new();
            split_req.set_split_key(key);
            req.mut_splits().mut_requests().push(split_req);
        }
        req
    }

    fn new_row_key(table_id: i64, row_id: i64, version_id: u64) -> Vec<u8> {
        let mut buf = Vec::with_capacity(table::ID_LEN);
        buf.encode_i64(row_id).unwrap();
        let mut key = table::encode_row_key(table_id, &buf);
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    fn new_index_key(table_id: i64, idx_id: i64, datums: &[Datum], version_id: u64) -> Vec<u8> {
        let mut key =
            table::encode_index_seek_key(table_id, idx_id, &datum::encode_key(datums).unwrap());
        key = encode_bytes(&key);
        key.write_u64::<BigEndian>(version_id).unwrap();
        key
    }

    #[test]
    fn test_forget_encode() {
        let region_start_key = new_row_key(256, 1, 0);
        let key = new_row_key(256, 2, 0);
        let mut r = Region::new();
        r.set_id(10);
        r.set_start_key(region_start_key);

        let mut ctx = ObserverContext::new(&r);
        let observer = SplitObserver;

        let mut req = new_batch_split_request(vec![key]);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        let expect_key = new_row_key(256, 2, 0);
        let len = expect_key.len();
        assert_eq!(req.get_splits().get_requests().len(), 1);
        assert_eq!(
            req.get_splits().get_requests()[0].get_split_key(),
            &expect_key[..len - 8]
        );
    }

    #[test]
    fn test_split() {
        let mut region = Region::new();
        let start_key = new_row_key(1, 1, 1);
        region.set_start_key(start_key.clone());
        let mut ctx = ObserverContext::new(&region);
        let mut req = AdminRequest::new();

        let observer = SplitObserver;

        let resp = observer.pre_propose_admin(&mut ctx, &mut req);
        // since no split is defined, actual coprocessor won't be invoke.
        assert!(resp.is_ok());
        assert!(!req.has_split(), "only split req should be handle.");

        req = new_split_request(b"test");
        // Only batch split is supported.
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_err());

        req = new_batch_split_request(vec![vec![]]);
        // Split key should not be empty.
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_err());

        req = new_batch_split_request(vec![start_key]);
        // Split key should not be the same as start_key.
        assert!(observer.pre_propose_admin(&mut ctx, &mut req).is_err());

        let mut split_keys = vec![b"xyz".to_vec()];
        let mut expected_keys = vec![b"xyz".to_vec()];

        let mut key = encode_bytes(b"xyz:1");
        key.write_u64::<BigEndian>(0).unwrap();
        split_keys.push(key);
        let mut expected_key = encode_bytes(b"xyz:1");
        expected_keys.push(expected_key);

        key = new_row_key(1, 2, 0);
        expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        key = new_row_key(1, 2, 1);
        split_keys.push(key);
        expected_keys.push(expected_key);

        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 0);
        expected_key = key[..key.len() - 8].to_vec();
        split_keys.push(key);
        expected_keys.push(expected_key.clone());

        key = new_index_key(2, 2, &[Datum::I64(1), Datum::Bytes(b"brgege".to_vec())], 5);
        split_keys.push(key);
        expected_keys.push(expected_key);

        expected_key =
            encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea_r\x80\x00\x00\x00\x00\x05\x82\x7f");
        key = expected_key.clone();
        key.extend_from_slice(b"\x80\x00\x00\x00\x00\x00\x00\xd3");
        split_keys.push(key);
        expected_keys.push(expected_key);

        // Split at table prefix.
        key = encode_bytes(b"t\x80\x00\x00\x00\x00\x00\x00\xea");
        split_keys.push(key.clone());
        expected_keys.push(key);

        req = new_batch_split_request(split_keys);
        req.mut_splits().set_right_derive(true);
        observer.pre_propose_admin(&mut ctx, &mut req).unwrap();
        assert!(req.get_splits().get_right_derive());
        assert_eq!(req.get_splits().get_requests().len(), expected_keys.len());
        for (i, (req, expected_key)) in req.get_splits()
            .get_requests()
            .iter()
            .zip(expected_keys)
            .enumerate()
        {
            assert_eq!(
                req.get_split_key(),
                expected_key.as_slice(),
                "case {}",
                i + 1
            );
        }
    }
}

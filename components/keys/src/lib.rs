// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath] Common key utitlies.

//! TiKV key building

#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::mem;

use byteorder::{BigEndian, ByteOrder};
use kvproto::metapb::Region;
use thiserror::Error;

pub mod rewrite;

pub const MIN_KEY: &[u8] = &[];
pub const MAX_KEY: &[u8] = &[0xFF];

pub const EMPTY_KEY: &[u8] = &[];

// local is in (0x01, 0x02);
pub const LOCAL_PREFIX: u8 = 0x01;
pub const LOCAL_MIN_KEY: &[u8] = &[LOCAL_PREFIX];
pub const LOCAL_MAX_KEY: &[u8] = &[LOCAL_PREFIX + 1];

pub const DATA_PREFIX: u8 = b'z';
pub const DATA_PREFIX_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MIN_KEY: &[u8] = &[DATA_PREFIX];
pub const DATA_MAX_KEY: &[u8] = &[DATA_PREFIX + 1];

// Following keys are all local keys, so the first byte must be 0x01.
pub const STORE_IDENT_KEY: &[u8] = &[LOCAL_PREFIX, 0x01];
pub const PREPARE_BOOTSTRAP_KEY: &[u8] = &[LOCAL_PREFIX, 0x02];
// We save two types region data in DB, for raft and other meta data.
// When the store starts, we should iterate all region meta data to
// construct peer, no need to travel large raft data, so we separate them
// with different prefixes.
pub const REGION_RAFT_PREFIX: u8 = 0x02;
pub const REGION_RAFT_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX];
pub const REGION_RAFT_MIN_KEY: &[u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX];
pub const REGION_RAFT_MAX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_RAFT_PREFIX + 1];
pub const REGION_META_PREFIX: u8 = 0x03;
pub const REGION_META_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MIN_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX];
pub const REGION_META_MAX_KEY: &[u8] = &[LOCAL_PREFIX, REGION_META_PREFIX + 1];

// Following are the suffix after the local prefix.
// For region id
pub const RAFT_LOG_SUFFIX: u8 = 0x01;
pub const RAFT_STATE_SUFFIX: u8 = 0x02;
pub const APPLY_STATE_SUFFIX: u8 = 0x03;
pub const SNAPSHOT_RAFT_STATE_SUFFIX: u8 = 0x04;
pub const SEQUENCE_NUMBER_RELATION_SUFFIX: u8 = 0x05;

// For region meta
pub const REGION_STATE_SUFFIX: u8 = 0x01;

#[inline]
fn make_region_prefix(region_id: u64, suffix: u8) -> [u8; 11] {
    let mut key = [0; 11];
    key[..2].copy_from_slice(REGION_RAFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key[10] = suffix;
    key
}

#[inline]
fn make_region_key(region_id: u64, suffix: u8, sub_id: u64) -> [u8; 19] {
    let mut key = [0; 19];
    key[..2].copy_from_slice(REGION_RAFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key[10] = suffix;
    BigEndian::write_u64(&mut key[11..19], sub_id);
    key
}

pub fn region_raft_prefix(region_id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[0..2].copy_from_slice(REGION_RAFT_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key
}

pub fn region_raft_prefix_len() -> usize {
    // REGION_RAFT_PREFIX_KEY + region_id + suffix
    REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>() + 1
}

pub fn raft_log_key(region_id: u64, log_index: u64) -> [u8; 19] {
    make_region_key(region_id, RAFT_LOG_SUFFIX, log_index)
}

pub fn raft_state_key(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, RAFT_STATE_SUFFIX)
}

pub fn sequence_number_relation_key(region_id: u64, seqno: u64) -> [u8; 19] {
    make_region_key(region_id, SEQUENCE_NUMBER_RELATION_SUFFIX, seqno)
}

pub fn snapshot_raft_state_key(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, SNAPSHOT_RAFT_STATE_SUFFIX)
}

pub fn apply_state_key(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, APPLY_STATE_SUFFIX)
}

/// Get the log index from raft log key generated by `raft_log_key`.
pub fn raft_log_index(key: &[u8]) -> Result<u64> {
    let expect_key_len = REGION_RAFT_PREFIX_KEY.len()
        + mem::size_of::<u64>()
        + mem::size_of::<u8>()
        + mem::size_of::<u64>();
    if key.len() != expect_key_len {
        return Err(Error::InvalidRaftLogKey(key.to_owned()));
    }
    Ok(BigEndian::read_u64(
        &key[expect_key_len - mem::size_of::<u64>()..],
    ))
}

/// Get the region id and index from raft log key generated by `raft_log_key`.
pub fn decode_raft_log_key(key: &[u8]) -> Result<(u64, u64)> {
    let suffix_idx = REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>();
    let expect_key_len = suffix_idx + mem::size_of::<u8>() + mem::size_of::<u64>();
    if key.len() != expect_key_len
        || !key.starts_with(REGION_RAFT_PREFIX_KEY)
        || key[suffix_idx] != RAFT_LOG_SUFFIX
    {
        return Err(Error::InvalidRaftLogKey(key.to_owned()));
    }
    let region_id = BigEndian::read_u64(&key[REGION_RAFT_PREFIX_KEY.len()..suffix_idx]);
    let index = BigEndian::read_u64(&key[suffix_idx + mem::size_of::<u8>()..]);
    Ok((region_id, index))
}

/// Get the region id and log type from raft key.
pub fn decode_raft_key(key: &[u8]) -> Result<(u64, u8)> {
    let suffix_idx = REGION_RAFT_PREFIX_KEY.len() + mem::size_of::<u64>();
    let expect_key_len = suffix_idx + mem::size_of::<u8>();
    if key.len() < expect_key_len || !key.starts_with(REGION_RAFT_PREFIX_KEY) {
        return Err(Error::InvalidRaftLogKey(key.to_owned()));
    }
    let region_id = BigEndian::read_u64(&key[REGION_RAFT_PREFIX_KEY.len()..suffix_idx]);
    Ok((region_id, key[suffix_idx]))
}

pub fn raft_log_prefix(region_id: u64) -> [u8; 11] {
    make_region_prefix(region_id, RAFT_LOG_SUFFIX)
}

/// Decode region raft key, return the region id and raft suffix type.
pub fn decode_region_raft_key(key: &[u8]) -> Result<(u64, u8)> {
    decode_region_key(REGION_RAFT_PREFIX_KEY, key, "raft")
}

#[inline]
fn make_region_meta_key(region_id: u64, suffix: u8) -> [u8; 11] {
    let mut key = [0; 11];
    key[0..2].copy_from_slice(REGION_META_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key[10] = suffix;
    key
}

/// Decode region key, return the region id and meta suffix type.
fn decode_region_key(prefix: &[u8], key: &[u8], category: &str) -> Result<(u64, u8)> {
    if prefix.len() + mem::size_of::<u64>() + mem::size_of::<u8>() != key.len() {
        return Err(Error::InvalidRegionKeyLength(
            category.to_owned(),
            key.to_owned(),
        ));
    }

    if !key.starts_with(prefix) {
        return Err(Error::InvalidRegionPrefix(
            category.to_owned(),
            key.to_owned(),
        ));
    }

    let region_id = BigEndian::read_u64(&key[prefix.len()..prefix.len() + mem::size_of::<u64>()]);

    Ok((region_id, key[key.len() - 1]))
}

/// Decode region meta key, return the region id and meta suffix type.
pub fn decode_region_meta_key(key: &[u8]) -> Result<(u64, u8)> {
    decode_region_key(REGION_META_PREFIX_KEY, key, "meta")
}

pub fn region_meta_prefix(region_id: u64) -> [u8; 10] {
    let mut key = [0; 10];
    key[0..2].copy_from_slice(REGION_META_PREFIX_KEY);
    BigEndian::write_u64(&mut key[2..10], region_id);
    key
}

pub fn region_state_key(region_id: u64) -> [u8; 11] {
    make_region_meta_key(region_id, REGION_STATE_SUFFIX)
}

pub fn region_state_key_with_index(region_id: u64, index: u64) -> [u8; 19] {
    let mut key = [0; 19];
    let meta_key = make_region_meta_key(region_id, REGION_STATE_SUFFIX);
    key[0..11].copy_from_slice(&meta_key);
    BigEndian::write_u64(&mut key[11..19], index);
    key
}

pub fn validate_data_key(key: &[u8]) -> bool {
    key.starts_with(DATA_PREFIX_KEY)
}

pub fn data_key(key: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(DATA_PREFIX_KEY.len() + key.len());
    v.extend_from_slice(DATA_PREFIX_KEY);
    v.extend_from_slice(key);
    v
}

pub fn data_key_with_buffer(key: &[u8], buffer: &mut Vec<u8>) {
    buffer.clear();
    buffer.extend_from_slice(DATA_PREFIX_KEY);
    buffer.extend_from_slice(key);
}

pub fn origin_key(key: &[u8]) -> &[u8] {
    assert!(
        validate_data_key(key),
        "invalid data key {}",
        &log_wrappers::Value::key(key)
    );
    &key[DATA_PREFIX_KEY.len()..]
}

/// Get the `start_key` of current region in encoded form.
pub fn enc_start_key(region: &Region) -> Vec<u8> {
    // only initialized region's start_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!region.get_peers().is_empty());
    data_key(region.get_start_key())
}

/// Get the `end_key` of current region in encoded form.
pub fn enc_end_key(region: &Region) -> Vec<u8> {
    // only initialized region's end_key can be encoded, otherwise there must be bugs
    // somewhere.
    assert!(!region.get_peers().is_empty());
    data_end_key(region.get_end_key())
}

#[inline]
pub fn data_end_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        DATA_MAX_KEY.to_vec()
    } else {
        data_key(key)
    }
}

pub fn origin_end_key(key: &[u8]) -> &[u8] {
    if key == DATA_MAX_KEY {
        b""
    } else {
        origin_key(key)
    }
}

pub(crate) fn next_key_no_alloc(key: &[u8]) -> Option<(&[u8], u8)> {
    let pos = key.iter().rposition(|b| *b != 0xff)?;
    Some((&key[..pos], key[pos] + 1))
}

/// Computes the next key of the given key.
///
/// If the key has no successor key (e.g. the input is "\xff\xff"), the result
/// would be an empty vector.
///
/// # Examples
///
/// ```
/// use keys::next_key;
/// assert_eq!(next_key(b"123"), b"124");
/// assert_eq!(next_key(b"12\xff"), b"13");
/// assert_eq!(next_key(b"\xff\xff"), b"");
/// assert_eq!(next_key(b"\xff\xfe"), b"\xff\xff");
/// assert_eq!(next_key(b"T"), b"U");
/// assert_eq!(next_key(b""), b"");
/// ```
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if let Some((s, e)) = next_key_no_alloc(key) {
        let mut res = Vec::with_capacity(s.len() + 1);
        res.extend_from_slice(s);
        res.push(e);
        res
    } else {
        Vec::new()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("{} is not a valid raft log key", log_wrappers::Value(.0))]
    InvalidRaftLogKey(Vec<u8>),
    #[error("invalid region {0} key length for key {}", log_wrappers::Value(.1))]
    InvalidRegionKeyLength(String, Vec<u8>),
    #[error("invalid region {0} prefix for key {}", log_wrappers::Value(.1))]
    InvalidRegionPrefix(String, Vec<u8>),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use byteorder::{BigEndian, WriteBytesExt};
    use kvproto::metapb::{Peer, Region};

    use super::*;

    #[test]
    fn test_region_id_key() {
        let region_ids = vec![0, 1, 1024, u64::MAX];
        for region_id in region_ids {
            let prefix = region_raft_prefix(region_id);

            assert!(raft_log_prefix(region_id).starts_with(&prefix));
            assert!(raft_log_key(region_id, 1).starts_with(&prefix));
            assert!(raft_state_key(region_id).starts_with(&prefix));
            assert!(apply_state_key(region_id).starts_with(&prefix));
        }

        // test sort.
        let tbls = vec![
            (1, 0, Ordering::Greater),
            (1, 1, Ordering::Equal),
            (1, 2, Ordering::Less),
        ];
        for (lid, rid, order) in tbls {
            let lhs = region_raft_prefix(lid);
            let rhs = region_raft_prefix(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));

            let lhs = raft_state_key(lid);
            let rhs = raft_state_key(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));

            let lhs = apply_state_key(lid);
            let rhs = apply_state_key(rid);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));
        }
    }

    #[test]
    fn test_raft_log_sort() {
        let tbls = vec![
            (1, 1, 1, 2, Ordering::Less),
            (2, 1, 1, 2, Ordering::Greater),
            (1, 1, 1, 1, Ordering::Equal),
        ];

        for (lid, l_log_id, rid, r_log_id, order) in tbls {
            let lhs = raft_log_key(lid, l_log_id);
            let rhs = raft_log_key(rid, r_log_id);
            assert_eq!(lhs.partial_cmp(&rhs), Some(order));
        }
    }

    #[test]
    fn test_region_key() {
        let ids = vec![1, 1024, u64::max_value()];
        for id in ids {
            // region meta key
            let meta_prefix = region_meta_prefix(id);
            let meta_info_key = region_state_key(id);
            assert!(meta_info_key.starts_with(&meta_prefix));

            assert_eq!(
                decode_region_meta_key(&meta_info_key).unwrap(),
                (id, REGION_STATE_SUFFIX)
            );

            // region raft key
            let raft_prefix = region_raft_prefix(id);
            let raft_apply_key = apply_state_key(id);
            assert!(raft_apply_key.starts_with(&raft_prefix));

            assert_eq!(
                decode_region_raft_key(&raft_apply_key).unwrap(),
                (id, APPLY_STATE_SUFFIX)
            );
        }

        // test region key sort.
        let tbls: Vec<(u64, u64, Ordering)> = vec![
            (1, 2, Ordering::Less),
            (1, 1, Ordering::Equal),
            (2, 1, Ordering::Greater),
        ];

        for (lkey, rkey, order) in tbls {
            // region meta key.
            let meta_lhs = region_state_key(lkey);
            let meta_rhs = region_state_key(rkey);
            assert_eq!(meta_lhs.partial_cmp(&meta_rhs), Some(order));

            // region meta key.
            let raft_lhs = apply_state_key(lkey);
            let raft_rhs = apply_state_key(rkey);
            assert_eq!(raft_lhs.partial_cmp(&raft_rhs), Some(order));
        }
    }

    #[test]
    fn test_raft_log_key() {
        for region_id in 1..10 {
            for idx_id in 1..10 {
                let key = raft_log_key(region_id, idx_id);
                assert_eq!(idx_id, raft_log_index(&key).unwrap());
                assert_eq!((region_id, idx_id), decode_raft_log_key(&key).unwrap());
            }
        }

        let state_key = raft_state_key(1);
        // invalid length
        assert!(decode_raft_log_key(&state_key).is_err());

        let mut state_key = state_key.to_vec();
        state_key.write_u64::<BigEndian>(2).unwrap();
        // invalid suffix
        assert!(decode_raft_log_key(&state_key).is_err());

        let mut region_state_key = region_state_key(1).to_vec();
        region_state_key.write_u64::<BigEndian>(2).unwrap();
        // invalid prefix
        assert!(decode_raft_log_key(&region_state_key).is_err());
    }

    #[test]
    fn test_data_key() {
        assert!(!validate_data_key(b"abc"));
        assert!(validate_data_key(&data_key(b"abc")));
        let mut buffer = vec![];
        data_key_with_buffer(b"abc", &mut buffer);
        assert_eq!(buffer, data_key(b"abc"));
        data_key_with_buffer(b"cde", &mut buffer);
        assert_eq!(buffer, data_key(b"cde"));

        let mut region = Region::default();
        // uninitialised region should not be passed in `enc_start_key` and `enc_end_key`.
        assert!(::panic_hook::recover_safe(|| enc_start_key(&region)).is_err());
        assert!(::panic_hook::recover_safe(|| enc_end_key(&region)).is_err());

        region.mut_peers().push(Peer::default());
        assert_eq!(enc_start_key(&region), vec![DATA_PREFIX]);
        assert_eq!(enc_end_key(&region), vec![DATA_PREFIX + 1]);

        region.set_start_key(vec![1]);
        region.set_end_key(vec![2]);
        assert_eq!(enc_start_key(&region), vec![DATA_PREFIX, 1]);
        assert_eq!(enc_end_key(&region), vec![DATA_PREFIX, 2]);
    }
}

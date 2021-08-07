// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Various metrics related to key ranges
//!
//! In RocksDB these are typically implemented with user collected properties,
//! which might require the database to be constructed with specific options.

use crate::errors::Result;
use crate::Range;

pub trait RangePropertiesExt {
    /// Gets the number of keys in a range.
    fn get_range_approximate_keys(&self, range: Range, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get the approximate size of the range
    fn get_range_approximate_size(&self, range: Range, large_threshold: u64) -> Result<u64>;

    fn get_range_approximate_size_cf(
        &self,
        cfname: &str,
        range: Range,
        large_threshold: u64,
    ) -> Result<u64>;

    /// Get range approximate split keys to split range evenly into key_count + 1 parts .
    fn get_range_approximate_split_keys(
        &self,
        range: Range,
        split_size: u64,
        max_size: u64,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>>;

    fn get_range_approximate_split_keys_cf(
        &self,
        cfname: &str,
        range: Range,
        split_size: u64,
        max_size: u64,
        key_count: usize,
    ) -> Result<Vec<Vec<u8>>>;
}

// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::cf_names::CFNamesExt;
use crate::errors::Result;
use crate::range::Range;
use std::path::PathBuf;
use std::collections::HashMap;
use std::string::String;

#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    DeleteFiles,
    DeleteBlobs,
    DeleteByKey,
    DeleteByRange,
    DeleteByWriter { sst_path: String },
}

#[derive(Clone, Debug)]
pub struct SSTFile {
    pub cf_name: String,
    pub file_name: String,
    pub file_size: usize,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub smallest_seqno: u64,
    pub largest_seqno: u64,
}

impl SSTFile {
    pub fn get_cf_name(&self) -> &str {
        self.cf_name.as_str()
    }

    pub fn get_file_name(&self) -> &str {
        self.file_name.as_str()
    }

    pub fn get_file_size(&self) -> usize {
        self.file_size
    }

    pub fn get_smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }
    pub fn get_largest_key(&self) -> &[u8] {
        &self.largest_key
    }
    pub fn get_smallest_seqno(&self) -> u64 {
        self.smallest_seqno
    }
    pub fn get_largest_seqno(&self) -> u64 {
        self.largest_seqno
    }
}

pub trait MiscExt: CFNamesExt {
    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()>;

    fn delete_all_in_range(&self, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        for cf in self.cf_names() {
            self.delete_ranges_cf(cf, strategy.clone(), ranges)?;
        }
        Ok(())
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()>;

    /// Return the approximate number of records and size in the range of memtables of the cf.
    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool>;

    /// Gets total used size of rocksdb engine, including:
    /// *  total size (bytes) of all SST files.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob files.
    ///
    fn get_engine_used_size(&self) -> Result<u64>;

    fn get_engine_total_keys(&self) -> Result<u64>;

    fn get_engine_memory_usage(&self) -> u64;

    /// Roughly deletes files in multiple ranges.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in the database.
    ///    - After this operation, some keys in the range might be removed from existing snapshot,
    ///      so you shouldn't expect to be able to read data from the range using existing snapshots
    ///      any more.
    ///
    /// Ref: <https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys>
    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;

    /// The path to the directory on the filesystem where the database is stored
    fn path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Check whether a database exists at a given path
    fn exists(path: &str) -> bool;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64>;

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>>;

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>>;

    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>>;

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>>;

    fn get_cf_compaction_pending_bytes(&self, cf: &str) -> Result<Option<u64>>;

    fn is_stalled_or_stopped(&self) -> bool;

    fn checkpoint_to(&self, path: &[PathBuf], size_to_flush: u64) -> Result<()>;

    fn filter_sst(&mut self, sst_folder: &str,  start_key: &[u8], end_key: &[u8]) -> HashMap<String, String>;

    fn get_cf_files(&self, cf: &str, level: usize) -> Result<Vec<SSTFile>>;

    fn get_cf_num_of_level(&self, cf: &str) -> usize;
}

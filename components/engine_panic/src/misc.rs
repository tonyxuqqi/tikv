// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::PanicEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, Result, SSTFile};
use std::path::PathBuf;
use std::collections::HashMap;
use std::string::String;
use kvproto::import_sstpb::SstMeta;

impl MiscExt for PanicEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        panic!()
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        panic!()
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        panic!()
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        panic!()
    }

    fn get_engine_total_keys(&self) -> Result<u64> {
        panic!()
    }

    fn get_engine_memory_usage(&self) -> u64 {
        panic!()
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        panic!()
    }

    fn path(&self) -> &str {
        panic!()
    }

    fn sync_wal(&self) -> Result<()> {
        panic!()
    }

    fn exists(path: &str) -> bool {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        panic!()
    }

    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>> {
        panic!()
    }

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn get_cf_compaction_pending_bytes(&self, cf: &str) -> Result<Option<u64>> {
        panic!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        panic!()
    }

    fn checkpoint_to(&self, path: &[PathBuf], size_to_flush: u64) -> Result<()> {
        panic!()
    }

    fn filter_sst(&mut self, sst_folder: &str, start_key: &[u8], end_key: &[u8]) -> HashMap<String, String> {
        panic!()
    }

    fn get_cf_files(&self, cf: &str, level: usize) -> Result<Vec<SSTFile>> {
        panic!()
    }

    fn get_cf_num_of_level(&self, cf: &str) -> usize {
        panic!()
    }
}

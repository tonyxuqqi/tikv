// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::import::RocksIngestExternalFileOptions;
use crate::sst::{RocksSstReader, RocksSstWriterBuilder};
use crate::{util, RocksSstWriter, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES, ROCKSDB_ESTIMATE_NUM_KEYS};
use engine_traits::{
    CFNamesExt, DeleteStrategy, Error, ImportExt, IngestExternalFileOptions, IterOptions, Iterable,
    Iterator, MiscExt, Mutable, Range, Result, SSTFile, SstWriter, SstWriterBuilder, WriteBatch,
    WriteBatchExt, ALL_CFS,
};
use rocksdb::Range as RocksRange;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::string::String;
use tikv_util::box_try;
use tikv_util::keybuilder::KeyBuilder;

pub const MAX_DELETE_COUNT_BY_KEY: usize = 2048;

impl RocksEngine {
    fn is_titan(&self) -> bool {
        self.as_inner().is_titan()
    }

    // We store all data which would be deleted in memory at first because the data of region will never be larger than
    // max-region-size.
    fn delete_all_in_range_cf_by_ingest(
        &self,
        cf: &str,
        sst_path: String,
        ranges: &[Range],
    ) -> Result<()> {
        let mut ranges = ranges.to_owned();
        ranges.sort_by(|a, b| a.start_key.cmp(b.start_key));
        let max_end_key = ranges
            .iter()
            .fold(ranges[0].end_key, |x, y| std::cmp::max(x, y.end_key));
        let start = KeyBuilder::from_slice(ranges[0].start_key, 0, 0);
        let end = KeyBuilder::from_slice(max_end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }

        let mut writer_wrapper: Option<RocksSstWriter> = None;
        let mut data: Vec<Vec<u8>> = vec![];
        let mut last_end_key: Option<Vec<u8>> = None;
        for r in ranges {
            // There may be a range overlap with next range
            if last_end_key
                .as_ref()
                .map_or(false, |key| key.as_slice() > r.start_key)
            {
                self.delete_all_in_range_cf_by_key(cf, &r)?;
                continue;
            }
            last_end_key = Some(r.end_key.to_owned());

            let mut it = self.iterator_cf_opt(cf, opts.clone())?;
            let mut it_valid = it.seek(r.start_key.into())?;
            while it_valid {
                if it.key() >= r.end_key {
                    break;
                }
                if let Some(writer) = writer_wrapper.as_mut() {
                    writer.delete(it.key())?;
                } else {
                    data.push(it.key().to_vec());
                }
                if data.len() > MAX_DELETE_COUNT_BY_KEY {
                    let builder = RocksSstWriterBuilder::new().set_db(self).set_cf(cf);
                    let mut writer = builder.build(sst_path.as_str())?;
                    for key in data.iter() {
                        writer.delete(key)?;
                    }
                    data.clear();
                    writer_wrapper = Some(writer);
                }
                it_valid = it.next()?;
            }
        }

        if let Some(writer) = writer_wrapper {
            writer.finish()?;
            let mut opt = RocksIngestExternalFileOptions::new();
            opt.move_files(true);
            self.ingest_external_file_cf(cf, &opt, &[sst_path.as_str()])?;
        } else {
            let mut wb = self.write_batch();
            for key in data.iter() {
                wb.delete_cf(cf, key)?;
                if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                    wb.write()?;
                    wb.clear();
                }
            }
            if wb.count() > 0 {
                wb.write()?;
            }
        }
        Ok(())
    }

    fn delete_all_in_range_cf_by_key(&self, cf: &str, range: &Range) -> Result<()> {
        let start = KeyBuilder::from_slice(range.start_key, 0, 0);
        let end = KeyBuilder::from_slice(range.end_key, 0, 0);
        let mut opts = IterOptions::new(Some(start), Some(end), false);
        if self.is_titan() {
            // Cause DeleteFilesInRange may expose old blob index keys, setting key only for Titan
            // to avoid referring to missing blob files.
            opts.set_key_only(true);
        }
        let mut it = self.iterator_cf_opt(cf, opts)?;
        let mut it_valid = it.seek(range.start_key.into())?;
        let mut wb = self.write_batch();
        while it_valid {
            wb.delete_cf(cf, it.key())?;
            if wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                wb.write()?;
                wb.clear();
            }
            it_valid = it.next()?;
        }
        if wb.count() > 0 {
            wb.write()?;
        }
        self.sync_wal()?;
        Ok(())
    }

    fn import_sst(
        &mut self,
        cf: &str,
        file: &str,
        folder: &str,
        temp_folder: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Result<String> {
        let src_file_path = folder.to_string() + file;
        let dst_file_path = temp_folder.to_string() + file;
        println!(
            "file:{}, folder:{}, temp_folder:{}, src file path {}, dst file path {}",
            file, folder, temp_folder, src_file_path, dst_file_path
        );
        let env = self.as_inner().env();
        let sst_reader = RocksSstReader::open_with_env(&src_file_path, env)?;
        fs::create_dir_all(temp_folder)?;
        let builder = RocksSstWriterBuilder::new()
            .set_db(self)
            .set_cf(cf);
        let mut writer = builder.build(&dst_file_path)?;
        sst_reader
            .scan(start_key, end_key, false, |key, value| {
                writer.put(key, value).unwrap();
                Ok(true)
            })
            .unwrap();
        writer.finish().unwrap();
        Ok(dst_file_path)
    }
}

impl MiscExt for RocksEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        let handles: Result<Vec<_>> = self
            .cf_names()
            .into_iter()
            .map(|name| util::get_cf_handle(self.as_inner(), &name))
            .collect();
        Ok(self.as_inner().flush_cfs(handles?.as_slice(), sync)?)
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self.as_inner().flush_cf(handle, sync)?)
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        if ranges.is_empty() {
            return Ok(());
        }
        match strategy {
            DeleteStrategy::DeleteFiles => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                for r in ranges {
                    if r.start_key >= r.end_key {
                        continue;
                    }
                    self.as_inner().delete_files_in_range_cf(
                        handle,
                        r.start_key,
                        r.end_key,
                        false,
                    )?;
                }
            }
            DeleteStrategy::DeleteBlobs => {
                let handle = util::get_cf_handle(self.as_inner(), cf)?;
                if self.is_titan() {
                    for r in ranges {
                        if r.start_key >= r.end_key {
                            continue;
                        }
                        self.as_inner().delete_blob_files_in_range_cf(
                            handle,
                            r.start_key,
                            r.end_key,
                            false,
                        )?;
                    }
                }
            }
            DeleteStrategy::DeleteByRange => {
                let mut wb = self.write_batch();
                for r in ranges.iter() {
                    wb.delete_range_cf(cf, r.start_key, r.end_key)?;
                }
                wb.write()?;
            }
            DeleteStrategy::DeleteByKey => {
                for r in ranges {
                    self.delete_all_in_range_cf_by_key(cf, &r)?;
                }
            }
            DeleteStrategy::DeleteByWriter { sst_path } => {
                self.delete_all_in_range_cf_by_ingest(cf, sst_path, ranges)?;
            }
        }
        Ok(())
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        let range = util::range_to_rocks_range(range);
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_approximate_memtable_stats_cf(handle, &range))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        if let Some(n) = util::get_cf_num_files_at_level(self.as_inner(), handle, 0) {
            let options = self.as_inner().get_options_cf(handle);
            let slowdown_trigger = options.get_level_zero_slowdown_writes_trigger();
            // Leave enough buffer to tolerate heavy write workload,
            // which may flush some memtables in a short time.
            if n > u64::from(slowdown_trigger) / 2 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        let mut used_size: u64 = 0;
        for cf in ALL_CFS {
            let handle = util::get_cf_handle(self.as_inner(), cf)?;
            used_size += util::get_engine_cf_used_size(self.as_inner(), handle);
        }
        Ok(used_size)
    }

    fn get_engine_total_keys(&self) -> Result<u64> {
        let mut total_keys: u64 = 0;
        for cf in ALL_CFS {
            let handle = util::get_cf_handle(self.as_inner(), cf)?;
            total_keys += self
                .as_inner()
                .get_property_int_cf(handle, ROCKSDB_ESTIMATE_NUM_KEYS)
                .unwrap_or(0);
        }
        Ok(total_keys)
    }

    fn get_engine_memory_usage(&self) -> u64 {
        let mut total_mem: u64 = 0;
        for cf in ALL_CFS {
            let handle = util::get_cf_handle(self.as_inner(), cf).unwrap();
            total_mem += self
                .as_inner()
                .get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES)
                .unwrap_or(0);
        }
        total_mem
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let db = self.as_inner();
        let mut delete_ranges = Vec::new();
        for &(ref start, ref end) in ranges {
            if start == end {
                continue;
            }
            assert!(start < end);
            delete_ranges.push(RocksRange::new(start, end));
        }
        if delete_ranges.is_empty() {
            return Ok(());
        }

        for cf in db.cf_names() {
            let handle = util::get_cf_handle(db, cf)?;
            db.delete_files_in_ranges_cf(handle, &delete_ranges, /* include_end */ false)?;
        }

        Ok(())
    }

    fn path(&self) -> &str {
        self.as_inner().path()
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(self.as_inner().sync_wal()?)
    }

    fn exists(path: &str) -> bool {
        crate::raw_util::db_exist(path)
    }

    fn dump_stats(&self) -> Result<String> {
        const ROCKSDB_DB_STATS_KEY: &str = "rocksdb.dbstats";
        const ROCKSDB_CF_STATS_KEY: &str = "rocksdb.cfstats";

        let mut s = Vec::with_capacity(1024);
        // common rocksdb stats.
        for name in self.cf_names() {
            let handler = util::get_cf_handle(self.as_inner(), name)?;
            if let Some(v) = self
                .as_inner()
                .get_property_value_cf(handler, ROCKSDB_CF_STATS_KEY)
            {
                s.extend_from_slice(v.as_bytes());
            }
        }

        if let Some(v) = self.as_inner().get_property_value(ROCKSDB_DB_STATS_KEY) {
            s.extend_from_slice(v.as_bytes());
        }

        // more stats if enable_statistics is true.
        if let Some(v) = self.as_inner().get_statistics() {
            s.extend_from_slice(v.as_bytes());
        }

        Ok(box_try!(String::from_utf8(s)))
    }

    fn get_latest_sequence_number(&self) -> u64 {
        self.as_inner().get_latest_sequence_number()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        match self
            .as_inner()
            .get_property_int(crate::ROCKSDB_OLDEST_SNAPSHOT_SEQUENCE)
        {
            // Some(0) indicates that no snapshot is in use
            Some(0) => None,
            s => s,
        }
    }

    fn get_total_sst_files_size_cf(&self, cf: &str) -> Result<Option<u64>> {
        const ROCKSDB_TOTAL_SST_FILES_SIZE: &str = "rocksdb.total-sst-files-size";
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(self
            .as_inner()
            .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE))
    }

    fn get_range_entries_and_versions(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>> {
        Ok(crate::properties::get_range_entries_and_versions(
            self, cf, start, end,
        ))
    }

    fn get_cf_num_files_at_level(&self, cf: &str, level: usize) -> Result<Option<u64>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(crate::util::get_cf_num_files_at_level(
            self.as_inner(),
            &handle,
            level,
        ))
    }

    fn get_cf_num_immutable_mem_table(&self, cf: &str) -> Result<Option<u64>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(crate::util::get_cf_num_immutable_mem_table(
            self.as_inner(),
            &handle,
        ))
    }

    fn get_cf_compaction_pending_bytes(&self, cf: &str) -> Result<Option<u64>> {
        let handle = util::get_cf_handle(self.as_inner(), cf)?;
        Ok(crate::util::get_cf_compaction_pending_bytes(
            self.as_inner(),
            &handle,
        ))
    }

    fn is_stalled_or_stopped(&self) -> bool {
        const ROCKSDB_IS_WRITE_STALLED: &str = "rocksdb.is-write-stalled";
        const ROCKSDB_IS_WRITE_STOPPED: &str = "rocksdb.is-write-stopped";
        self.as_inner()
            .get_property_int(ROCKSDB_IS_WRITE_STALLED)
            .unwrap_or_default()
            != 0
            || self
                .as_inner()
                .get_property_int(ROCKSDB_IS_WRITE_STOPPED)
                .unwrap_or_default()
                != 0
    }

    fn checkpoint_to(&self, path: &[PathBuf], size_to_flush: u64) -> Result<()> {
        if path.is_empty() {
            return Ok(());
        }
        let mut checkpoint = self.as_inner().checkpoint()?;
        checkpoint.create_at(&path[0], size_to_flush)?;
        for p in &path[1..] {
            checkpoint.create_at(p, u64::MAX)?;
        }
        Ok(())
    }

    // generate sst files from original sst files that have other region's data
    // return a HashMap of <original file name : new file path>
    fn filter_sst(
        &mut self,
        sst_folder: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> HashMap<String, String> {
        let db = self.as_inner().clone();
        let cfs = db.cf_names();
        let mut sst_file_map: HashMap<String, String> = HashMap::new();
        let mut temp_folder = sst_folder.to_string();
        if temp_folder.len() == 0 {
            temp_folder = db.path().to_string() + "/tmp";
        }
        for cf in cfs.iter() {
            let handle = db.cf_handle(cf).unwrap();
            let column_family_meta = db.get_column_family_meta_data(handle);
            let level_metas = column_family_meta.get_levels();
            for level_meta in level_metas.iter() {
                let sst_file_metas = level_meta.get_files();
                println!("sst files for cf {} {}", cf, sst_file_metas.len());
                for sst_file_meta in sst_file_metas.iter() {
                    let smallest_key = sst_file_meta.get_smallestkey();
                    let biggest_key = sst_file_meta.get_largestkey();
                    /*if smallest_key.cmp(start_key) != Ordering::Less
                        && biggest_key.cmp(end_key) != Ordering::Greater
                    {
                        println!("skipping {}", sst_file_meta.get_name());
                        continue;
                    } else*/ if (smallest_key.cmp(end_key) != Ordering::Less)
                        || (biggest_key.cmp(start_key) == Ordering::Less)
                    {
                        sst_file_map.insert(sst_file_meta.get_name(), "".to_string());
                        continue;
                    }
                    let file_name = sst_file_meta.get_name();
                    let result = self.import_sst(
                        cf,
                        &file_name,
                        db.path(),
                        &temp_folder,
                        start_key,
                        end_key,
                    );
                    match result {
                        Ok(new_sst_file) => {
                            println!("sst_file_map insert {}", &new_sst_file);
                            sst_file_map.insert(file_name, new_sst_file);
                        }
                        Err(err) => {
                            println!("import_sst failed {:?}", err);
                        }
                    }
                }
            }
        }
        sst_file_map
    }

    // TODO: add get_cf_files impl;
    fn get_cf_files(&self, cf: &str, level: usize) -> Result<Vec<SSTFile>> {
        let db = self.as_inner().clone();
        let handle = db.cf_handle(cf).unwrap();
        let column_family_meta = db.get_column_family_meta_data(handle);
        let level_metas = column_family_meta.get_levels();
        if level_metas.len() <= level {
            return Err(Error::Other(
                format!(
                    "Invalid level '{}'. Total level is '{}'.",
                    level,
                    level_metas.len()
                )
                .into(),
            ));
        }
        Ok(level_metas[level]
            .get_files()
            .iter()
            .map(|sst_meta| SSTFile {
                cf_name: cf.to_string(),
                file_name: sst_meta.get_name(),
                file_size: sst_meta.get_size(),
            })
            .collect::<Vec<SSTFile>>())
    }

    fn get_cf_num_of_level(&self, cf: &str) -> usize {
        let db = self.as_inner().clone();
        let handle = db.cf_handle(cf).unwrap();
        let column_family_meta = db.get_column_family_meta_data(handle);
        let level_metas = column_family_meta.get_levels();
        return level_metas.len();
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use crate::engine::RocksEngine;
    use crate::raw::DB;
    use crate::raw::{ColumnFamilyOptions, DBOptions};
    use crate::raw_util::{new_engine_opt, CFOptions};
    use std::sync::Arc;

    use super::*;
    use engine_traits::{DeleteStrategy, ALL_CFS};
    use engine_traits::{Iterable, Iterator, Mutable, SeekKey, SyncMutable, WriteBatchExt};

    fn check_data(db: &RocksEngine, cfs: &[&str], expected: &[(&[u8], &[u8])]) {
        for cf in cfs {
            let mut iter = db.iterator_cf(cf).unwrap();
            iter.seek(SeekKey::Start).unwrap();
            for &(k, v) in expected {
                assert_eq!(k, iter.key());
                assert_eq!(v, iter.value());
                iter.next().unwrap();
            }
            assert!(!iter.valid().unwrap());
        }
    }

    fn test_delete_all_in_range(
        strategy: DeleteStrategy,
        origin_keys: &[Vec<u8>],
        ranges: &[Range],
    ) {
        let path = Builder::new()
            .prefix("engine_delete_all_in_range")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);

        let mut wb = db.write_batch();
        let ts: u8 = 12;
        let keys: Vec<_> = origin_keys
            .iter()
            .map(|k| {
                let mut k2 = k.clone();
                k2.append(&mut vec![ts; 8]);
                k2
            })
            .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                wb.put_cf(cf, k, v).unwrap();
            }
        }
        wb.write().unwrap();
        check_data(&db, ALL_CFS, kvs.as_slice());

        // Delete all in ranges.
        db.delete_all_in_range(strategy, ranges).unwrap();

        let mut kvs_left: Vec<_> = kvs;
        for r in ranges {
            kvs_left = kvs_left
                .into_iter()
                .filter(|k| k.0 < r.start_key || k.0 >= r.end_key)
                .collect();
        }
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    fn prepare_db(path_str: &str, origin_keys: &[Vec<u8>]) -> Arc<DB> {
        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, ColumnFamilyOptions::new()))
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db_return = db.clone();
        write_data(&db, origin_keys, true);
        return db_return;
    }

    fn write_data(db: &Arc<DB>, origin_keys: &[Vec<u8>], run_check_data: bool) {
        let db = RocksEngine::from_db(db.clone());
        let mut wb = db.write_batch();
        let ts: u8 = 12;
        let keys: Vec<_> = origin_keys
            .iter()
            .map(|k| {
                let mut k2 = k.clone();
                k2.append(&mut vec![ts; 8]);
                k2
            })
            .collect();

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for (_, key) in keys.iter().enumerate() {
            kvs.push((key.as_slice(), b"value"));
        }
        for &(k, v) in kvs.as_slice() {
            for cf in ALL_CFS {
                wb.put_cf(cf, k, v).unwrap();
            }
        }
        wb.write().unwrap();
        if run_check_data {
            check_data(&db, ALL_CFS, kvs.as_slice());
        }
        db.flush(true).unwrap();
    }

    #[test]
    fn test_delete_all_in_range_use_delete_range() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_key() {
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        // Single range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4")],
        );
        // Two ranges without overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k0", b"k1"), Range::new(b"k3", b"k4")],
        );
        // Two ranges with overlap.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k3"), Range::new(b"k2", b"k4")],
        );
        // One range contains the other range.
        test_delete_all_in_range(
            DeleteStrategy::DeleteByKey,
            &data,
            &[Range::new(b"k1", b"k4"), Range::new(b"k2", b"k3")],
        );
    }

    #[test]
    fn test_delete_all_in_range_by_writer() {
        let path = Builder::new()
            .prefix("test_delete_all_in_range_by_writer")
            .tempdir()
            .unwrap();
        let path_str = path.path();
        let sst_path = path_str.join("tmp_file").to_str().unwrap().to_owned();
        let mut data = vec![];
        for i in 1000..5000 {
            data.push(i.to_string().as_bytes().to_vec());
        }
        test_delete_all_in_range(
            DeleteStrategy::DeleteByWriter { sst_path },
            &data,
            &[
                Range::new(&data[2], &data[499]),
                Range::new(&data[502], &data[999]),
                Range::new(&data[1002], &data[1999]),
                Range::new(&data[1499], &data[2499]),
                Range::new(&data[2502], &data[3999]),
                Range::new(&data[3002], &data[3499]),
            ],
        );
    }

    #[test]
    fn test_delete_all_files_in_range() {
        let path = Builder::new()
            .prefix("engine_delete_all_files_in_range")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let cfs_opts = ALL_CFS
            .iter()
            .map(|cf| {
                let mut cf_opts = ColumnFamilyOptions::new();
                cf_opts.set_level_zero_file_num_compaction_trigger(1);
                CFOptions::new(cf, cf_opts)
            })
            .collect();
        let db = new_engine_opt(path_str, DBOptions::new(), cfs_opts).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);

        let keys = vec![b"k1", b"k2", b"k3", b"k4"];

        let mut kvs: Vec<(&[u8], &[u8])> = vec![];
        for key in keys {
            kvs.push((key, b"value"));
        }
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(kvs[0].0, kvs[0].1), (kvs[3].0, kvs[3].1)];
        for cf in ALL_CFS {
            for &(k, v) in kvs.as_slice() {
                db.put_cf(cf, k, v).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }
        check_data(&db, ALL_CFS, kvs.as_slice());

        db.delete_all_in_range(DeleteStrategy::DeleteFiles, &[Range::new(b"k2", b"k4")])
            .unwrap();
        db.delete_all_in_range(DeleteStrategy::DeleteBlobs, &[Range::new(b"k2", b"k4")])
            .unwrap();
        check_data(&db, ALL_CFS, kvs_left.as_slice());
    }

    #[test]
    fn test_delete_range_prefix_bloom_case() {
        let path = Builder::new()
            .prefix("engine_delete_range_prefix_bloom")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut opts = DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = ColumnFamilyOptions::new();
        // Prefix extractor(trim the timestamp at tail) for write cf.
        cf_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                Box::new(crate::util::FixedSuffixSliceTransform::new(8)),
            )
            .unwrap_or_else(|err| panic!("{:?}", err));
        // Create prefix bloom filter for memtable.
        cf_opts.set_memtable_prefix_bloom_size_ratio(0.1_f64);
        let cf = "default";
        let db = DB::open_cf(opts, path_str, vec![(cf, cf_opts)]).unwrap();
        let db = Arc::new(db);
        let db = RocksEngine::from_db(db);
        let mut wb = db.write_batch();
        let kvs: Vec<(&[u8], &[u8])> = vec![
            (b"kabcdefg1", b"v1"),
            (b"kabcdefg2", b"v2"),
            (b"kabcdefg3", b"v3"),
            (b"kabcdefg4", b"v4"),
        ];
        let kvs_left: Vec<(&[u8], &[u8])> = vec![(b"kabcdefg1", b"v1"), (b"kabcdefg4", b"v4")];

        for &(k, v) in kvs.as_slice() {
            wb.put_cf(cf, k, v).unwrap();
        }
        wb.write().unwrap();
        check_data(&db, &[cf], kvs.as_slice());

        // Delete all in ["k2", "k4").
        db.delete_all_in_range(
            DeleteStrategy::DeleteByRange,
            &[Range::new(b"kabcdefg2", b"kabcdefg4")],
        )
        .unwrap();
        check_data(&db, &[cf], kvs_left.as_slice());
    }

    #[test]
    fn test_get_cf_files() {
        let path = Builder::new().prefix("engine_misc").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let data = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        let db = prepare_db(path_str, &data);
        let db = RocksEngine::from_db(db);
        for cf in db.cf_names() {
            let max_level = db.get_cf_num_of_level(cf);
            for i in 0..max_level {
                let level = max_level - i - 1;
                let result = db.get_cf_files(cf, level);
                println!("sts meta count {}", result.unwrap().len());
            }
        }
    }
    #[test]
    fn test_filter_sst() {
        let path = Builder::new().prefix("engine_misc").tempdir().unwrap();
        let path_str = path.path().to_str().unwrap();
        let all_range = vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ];
        let left_range = vec![b"k00".to_vec(), b"k11".to_vec()];
        let right_range = vec![b"k22".to_vec(), b"k33".to_vec(), b"k44".to_vec()];
        let db = prepare_db(path_str, &all_range);
        write_data(&db, &left_range, false);
        write_data(&db, &right_range, false);
        let mut db = RocksEngine::from_db(db);
        let files = db.filter_sst("", b"k2", b"k5");
        println!("result1 {}", files.len());
        assert!(files.len() == 8);
        files
            .into_iter()
            .map(|(file_name, new_path)| {
                println!("file_name:{}, new path: {}", file_name, new_path);
                if !new_path.is_empty() {
                    let mut key_count = 0;
                    let sst_reader = RocksSstReader::open_with_env(&new_path, None).unwrap();
                    sst_reader
                        .scan(b"k2", b"k5", false, |_key, _value| {
                            key_count += 1;
                            Ok(true)
                        })
                        .unwrap();
                    assert!(key_count == 3); // key2, key3, key4
                }
                file_name
            })
            .for_each(drop);

        let files = db.filter_sst("", b"k0", b"k3");
        println!("result2 {}", files.len());
        assert!(files.len() == 8);
        files
            .into_iter()
            .map(|(file_name, new_path)| {
                println!("file_name:{}, new path: {}", file_name, new_path);
                if !new_path.is_empty() {
                    let mut key_count = 0;
                    let sst_reader = RocksSstReader::open_with_env(&new_path, None).unwrap();
                    sst_reader
                        .scan(b"k0", b"k3", false, |_key, _value| {
                            key_count += 1;
                            Ok(true)
                        })
                        .unwrap();
                    assert!(key_count == 3 || key_count == 1); // key0, key1, key2 or key22
                }
                file_name
            })
            .for_each(drop);
    }
}

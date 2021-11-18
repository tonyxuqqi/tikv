// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::util;
use collections::HashMap;
use engine_traits::{CFNamesExt, CompactExt, Result};
use lazy_static::*;
use rocksdb::{
    new_compaction_filter_raw, CompactOptions, CompactionFilter, CompactionFilterContext,
    CompactionFilterDecision, CompactionFilterFactory, CompactionFilterValueType,
    CompactionOptions, DBCompactionFilter, DBCompressionType,
};
use std::ffi::CString;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use tikv_util::{info};

use std::cmp;

impl CompactExt for RocksEngine {
    type CompactedEvent = crate::compact_listener::RocksCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> Result<bool> {
        for cf_name in self.cf_names() {
            let cf = util::get_cf_handle(self.as_inner(), cf_name)?;
            if self
                .as_inner()
                .get_options_cf(cf)
                .get_disable_auto_compactions()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn compact_range(
        &self,
        cf: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        exclusive_manual: bool,
        max_subcompactions: u32,
    ) -> Result<()> {
        let db = self.as_inner();
        let handle = util::get_cf_handle(db, cf)?;
        let mut compact_opts = CompactOptions::new();
        // `exclusive_manual == false` means manual compaction can
        // concurrently run with other background compactions.
        compact_opts.set_exclusive_manual_compaction(exclusive_manual);
        compact_opts.set_max_subcompactions(max_subcompactions as i32);
        db.compact_range_cf_opt(handle, &compact_opts, start_key, end_key);
        Ok(())
    }

    fn compact_files_in_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        for cf_name in self.cf_names() {
            self.compact_files_in_range_cf(cf_name, start, end, output_level)?;
        }
        Ok(())
    }

    fn compact_files_in_range_cf(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        output_level: Option<i32>,
    ) -> Result<()> {
        let db = self.as_inner();
        let handle = util::get_cf_handle(db, cf)?;
        let cf_opts = db.get_options_cf(handle);
        let output_level = output_level.unwrap_or(cf_opts.get_num_levels() as i32 - 1);

        let mut input_files = Vec::new();
        let cf_meta = db.get_column_family_meta_data(handle);
        for (i, level) in cf_meta.get_levels().iter().enumerate() {
            if i as i32 >= output_level {
                break;
            }
            for f in level.get_files() {
                if end.is_some() && end.unwrap() <= f.get_smallestkey() {
                    continue;
                }
                if start.is_some() && start.unwrap() > f.get_largestkey() {
                    continue;
                }
                input_files.push(f.get_name());
            }
        }
        if input_files.is_empty() {
            return Ok(());
        }

        self.compact_files_cf(
            cf,
            input_files,
            Some(output_level),
            cmp::min(num_cpus::get(), 32) as u32,
            false,
        )
    }

    fn compact_files_cf(
        &self,
        cf: &str,
        mut files: Vec<String>,
        output_level: Option<i32>,
        max_subcompactions: u32,
        exclude_l0: bool,
    ) -> Result<()> {
        let db = self.as_inner();
        let handle = util::get_cf_handle(db, cf)?;
        let cf_opts = db.get_options_cf(handle);
        let output_level = output_level.unwrap_or(cf_opts.get_num_levels() as i32 - 1);
        let output_compression = cf_opts
            .get_compression_per_level()
            .get(output_level as usize)
            .cloned()
            .unwrap_or(DBCompressionType::No);
        let output_file_size_limit = cf_opts.get_target_file_size_base() as usize;

        if exclude_l0 {
            let cf_meta = db.get_column_family_meta_data(handle);
            let l0_files = cf_meta.get_levels()[0].get_files();
            files.retain(|f| !l0_files.iter().any(|n| f.ends_with(&n.get_name())));
        }

        if files.is_empty() {
            return Ok(());
        }

        let mut opts = CompactionOptions::new();
        opts.set_compression(output_compression);
        opts.set_max_subcompactions(max_subcompactions as i32);
        opts.set_output_file_size_limit(output_file_size_limit);

        db.compact_files_cf(handle, &opts, &files, output_level)?;
        Ok(())
    }

    fn set_compaction_filter_key_range(
        &self,
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    ) {
        CompactionKeyRangeFilterFactory::update_range(region_id, start_key, end_key);
    }

    fn clear_compaction_filter_key_range(&self, region_id: u64) {
        CompactionKeyRangeFilterFactory::delete_range(region_id);
    }
}

pub struct RegionKeyRange {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

pub struct CompactionKeyRangeFilterFactory {
    pub region_id: u64,
}

lazy_static! {
    static ref REGIONS_KEY_RANGE: Arc<Mutex<HashMap<u64, RegionKeyRange>>> =
        Arc::new(Mutex::new(HashMap::default()));
}

pub struct CompactionKeyRangeFilter {
    region_id: u64,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    enabled: bool,
}

impl CompactionFilterFactory for CompactionKeyRangeFilterFactory {
    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> *mut DBCompactionFilter {
        let filter = self.create_filter();
        let filter = Box::new(filter);
        let name = CString::new("key_range_compaction_filter").unwrap();
        unsafe { new_compaction_filter_raw(name, filter) }
    }
}

impl CompactionKeyRangeFilterFactory {
    pub fn create_filter(&self) -> CompactionKeyRangeFilter {
        let mut start_key = keys::DATA_MIN_KEY.to_vec();
        let mut end_key = keys::DATA_MAX_KEY.to_vec();
        let mut enabled = false;
        {
            let key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
            if key_range_map.contains_key(&self.region_id) {
                let key_range = &key_range_map[&self.region_id];
                if key_range.start_key.len() != 0 || key_range.end_key.len() != 0 { 
                    enabled = true;
                    if key_range.start_key.len() != 0 {
                        start_key = keys::data_key(&key_range.start_key);
                    }
                    if key_range.end_key.len() != 0 {
                        end_key = keys::data_key(&key_range.end_key);
                    }
                }
            }
        }

        let region_id = self.region_id;
        println!(
            "create_filter region_id {} start_key {:?} end_key {:?}",
            self.region_id, &start_key, &end_key
        );
        info!("create compaction filter. region_id {}, enable {}, start_key {:?} end_key {:?}", region_id, enabled, &start_key, &end_key);
        CompactionKeyRangeFilter {
            region_id,
            start_key,
            end_key,
            enabled,
        }
    }

    fn update_range(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
        if region_id == 0 {
            return;
        }
        let mut key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
        key_range_map.insert(region_id, RegionKeyRange { start_key, end_key });
    }

    fn delete_range(region_id: u64) {
        let mut key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
        key_range_map.remove(&region_id);
    }
}

impl CompactionFilter for CompactionKeyRangeFilter {
    fn featured_filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _sequence: u64,
        _value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if value_type != CompactionFilterValueType::Value {
            return CompactionFilterDecision::Keep;
        }

        // only consider data keys or compaction filter is disabled
        if !self.enabled || !key.starts_with(keys::DATA_PREFIX_KEY) {
            return CompactionFilterDecision::Keep;
        }

        if key < self.start_key.as_slice() || key >= self.end_key.as_slice() {
            return CompactionFilterDecision::Remove;
        }
        CompactionFilterDecision::Keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw_util::{new_engine, CFOptions};
    use crate::Compat;
    use engine_traits::CompactExt;
    use rocksdb::{ColumnFamilyOptions, Writable};
    use std::sync::Arc;
    use tempfile::Builder;

    #[test]
    fn test_compact_files_in_range() {
        let temp_dir = Builder::new()
            .prefix("test_compact_files_in_range")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_disable_auto_compactions(true);
        let cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("test", cf_opts),
        ];
        let db = new_engine(
            temp_dir.path().to_str().unwrap(),
            None,
            &["default", "test"],
            Some(cfs_opts),
        )
        .unwrap();
        let db = Arc::new(db);

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            for i in 0..5 {
                db.put_cf(cf, &[i], &[i]).unwrap();
                db.put_cf(cf, &[i + 1], &[i + 1]).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            assert_eq!(cf_levels.first().unwrap().get_files().len(), 5);
        }

        // # Before
        // Level-0: [4-5], [3-4], [2-3], [1-2], [0-1]
        // # After
        // Level-0: [4-5]
        // Level-1: [0-4]
        db.c()
            .compact_files_in_range(None, Some(&[4]), Some(1))
            .unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            let level_0 = cf_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_1 = cf_levels[1].get_files();
            assert_eq!(level_1.len(), 1);
            assert_eq!(level_1[0].get_smallestkey(), &[0]);
            assert_eq!(level_1[0].get_largestkey(), &[4]);
        }

        // # Before
        // Level-0: [4-5]
        // Level-1: [0-4]
        // # After
        // Level-0: [4-5]
        // Level-N: [0-4]
        db.c()
            .compact_files_in_range(Some(&[2]), Some(&[4]), None)
            .unwrap();

        for cf_name in db.cf_names() {
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_opts = db.get_options_cf(cf);
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            let level_0 = cf_levels[0].get_files();
            assert_eq!(level_0.len(), 1);
            assert_eq!(level_0[0].get_smallestkey(), &[4]);
            assert_eq!(level_0[0].get_largestkey(), &[5]);
            let level_n = cf_levels[cf_opts.get_num_levels() - 1].get_files();
            assert_eq!(level_n.len(), 1);
            assert_eq!(level_n[0].get_smallestkey(), &[0]);
            assert_eq!(level_n[0].get_largestkey(), &[4]);
        }

        for cf_name in db.cf_names() {
            let mut files = vec![];
            let cf = db.cf_handle(cf_name).unwrap();
            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();

            for level in cf_levels.into_iter().rev() {
                files.extend(level.get_files().iter().map(|f| f.get_name()));
            }

            assert_eq!(files.len(), 2);
            db.c()
                .compact_files_cf(cf_name, files.clone(), Some(3), 0, true)
                .unwrap();

            let cf_meta = db.get_column_family_meta_data(cf);
            let cf_levels = cf_meta.get_levels();
            assert_eq!(cf_levels[0].get_files().len(), 1);
            assert_eq!(cf_levels[3].get_files().len(), 1);
        }
    }

    #[test]
    fn test_set_compaction_factory_update_range() {
        let region_id = 1;
        CompactionKeyRangeFilterFactory::update_range(region_id, b"k0".to_vec(), b"k5".to_vec());
        {
            let key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
            assert!(key_range_map.contains_key(&region_id));
        }
        CompactionKeyRangeFilterFactory::delete_range(region_id);
        {
            let key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
            assert!(!key_range_map.contains_key(&region_id));
        }
        CompactionKeyRangeFilterFactory::delete_range(region_id);
        {
            let key_range_map = REGIONS_KEY_RANGE.lock().unwrap();
            assert!(!key_range_map.contains_key(&region_id));
        }
    }

    #[test]
    fn test_create_compaction_key_range_filter() {
        let region_id = 1;
        let factory = CompactionKeyRangeFilterFactory { region_id };
        CompactionKeyRangeFilterFactory::update_range(region_id, b"k0".to_vec(), b"k5".to_vec());
        let filter = factory.create_filter();
        assert_eq!(filter.region_id, region_id);
        assert_eq!(filter.start_key, keys::data_key(b"k0"));
        assert_eq!(filter.end_key, keys::data_key(b"k5"));
    }
}

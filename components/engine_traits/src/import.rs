// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use std::path::Path;

pub trait ImportExt {
    type IngestExternalFileOptions: IngestExternalFileOptions;

    fn ingest_external_file_cf(
        &self,
        cf: &str,
        opt: &Self::IngestExternalFileOptions,
        files: &[&str],
    ) -> Result<()>;

    fn ingest_external_file_cf_with_seqno(
        &self,
        cf: &str,
        opts: &Self::IngestExternalFileOptions,
        files: &[&str],
        smallest_seqnos: &[u64],
        largest_seqnos: &[u64],
    ) -> Result<()>;

    fn reset_global_seq<P: AsRef<Path>>(&self, cf: &str, path: P) -> Result<()>;
}

pub trait IngestExternalFileOptions {
    fn new() -> Self;

    fn move_files(&mut self, f: bool);
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::Path;

use crate::meta::{
    load_meta, resolve_wal_input_file, validate_restore_chain, Meta, MetaError, DEFAULT_META_FILE,
};

pub struct RestoreOptions {
    pub backup_dirs: Vec<String>,
    pub out_home: String,
    pub file: String,
    pub verify: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error(transparent)]
    Meta(#[from] MetaError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Invalid(String),
}

pub fn run_restore(opts: RestoreOptions) -> Result<Meta, RestoreError> {
    if opts.backup_dirs.is_empty() {
        return Err(RestoreError::Invalid(
            "no backup directories provided".into(),
        ));
    }
    if Path::new(&opts.out_home).exists() {
        return Err(RestoreError::Invalid(format!(
            "target home {} already exists",
            opts.out_home
        )));
    }
    let file = if opts.file.is_empty() {
        crate::meta::DEFAULT_WAL_FILE
    } else {
        opts.file.as_str()
    };

    let mut metas = Vec::new();
    for dir in &opts.backup_dirs {
        let meta = load_meta(format!("{dir}/{DEFAULT_META_FILE}"))?;
        metas.push(meta);
    }
    validate_restore_chain(&metas)?;

    let db_name = &metas[0].database;
    for meta in &metas[1..] {
        if &meta.database != db_name {
            return Err(RestoreError::Invalid(format!(
                "backup chain mixes databases: {db_name} vs {}",
                meta.database
            )));
        }
    }

    for (i, dir) in opts.backup_dirs.iter().enumerate() {
        let meta = &metas[i];
        let (path, _compressed) = resolve_wal_input_file(dir, file)?;
        if opts.verify {
            use sha2::{Digest, Sha256};
            let bytes = std::fs::read(&path)?;
            let sum = hex::encode(Sha256::digest(&bytes));
            if sum != meta.sha256 {
                return Err(RestoreError::Invalid("checksum mismatch".into()));
            }
        }
    }

    Err(RestoreError::Invalid(
        "full restore apply not implemented in Rust port yet".into(),
    ))
}

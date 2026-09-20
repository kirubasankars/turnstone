// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub const TYPE_FULL: &str = "full";
pub const TYPE_DIFFERENTIAL: &str = "differential";
pub const DEFAULT_WAL_FILE: &str = "wal.bin";
pub const DEFAULT_META_FILE: &str = "backup.meta";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub timestamp: OffsetDateTime,
    pub database: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub base_lsn: u64,
    pub end_lsn: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub parent_sha256: String,
    pub compressed: bool,
    pub sha256: String,
}

#[derive(Debug, Deserialize)]
struct MetaRaw {
    timestamp: OffsetDateTime,
    database: String,
    #[serde(rename = "type")]
    ty: String,
    base_lsn: u64,
    end_lsn: u64,
    #[serde(default)]
    base_opid: u64,
    #[serde(default)]
    end_opid: u64,
    #[serde(default)]
    parent_sha256: String,
    compressed: bool,
    sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("invalid backup meta: {0}")]
    Invalid(String),
}

pub fn load_meta(path: impl AsRef<Path>) -> Result<Meta, MetaError> {
    let data = std::fs::read(path.as_ref())?;
    let raw: MetaRaw = serde_json::from_slice(&data).map_err(|e| MetaError::Invalid(e.to_string()))?;
    let mut meta = Meta {
        timestamp: raw.timestamp,
        database: raw.database,
        ty: raw.ty,
        base_lsn: raw.base_lsn,
        end_lsn: raw.end_lsn,
        parent_sha256: raw.parent_sha256,
        compressed: raw.compressed,
        sha256: raw.sha256,
    };
    if meta.base_lsn == 0 && raw.base_opid != 0 {
        meta.base_lsn = raw.base_opid;
    }
    if meta.end_lsn == 0 && raw.end_opid != 0 {
        meta.end_lsn = raw.end_opid;
    }
    if meta.database.is_empty() {
        return Err(MetaError::Invalid("backup meta missing database".into()));
    }
    if meta.ty != TYPE_FULL && meta.ty != TYPE_DIFFERENTIAL {
        return Err(MetaError::Invalid(format!(
            "backup meta has unknown type {:?}",
            meta.ty
        )));
    }
    Ok(meta)
}

pub fn save_meta(dir: impl AsRef<Path>, meta: &Meta) -> Result<(), MetaError> {
    let data = serde_json::to_string_pretty(meta).map_err(|e| MetaError::Invalid(e.to_string()))?;
    std::fs::write(dir.as_ref().join(DEFAULT_META_FILE), data)?;
    Ok(())
}

pub fn resolve_wal_file(dir: impl AsRef<Path>, name: &str, compressed: bool) -> PathBuf {
    if compressed && !name.ends_with(".gz") {
        dir.as_ref().join(format!("{name}.gz"))
    } else {
        dir.as_ref().join(name)
    }
}

pub fn resolve_wal_input_file(dir: impl AsRef<Path>, name: &str) -> Result<(PathBuf, bool), MetaError> {
    let plain = dir.as_ref().join(name);
    if plain.is_file() {
        return Ok((plain, false));
    }
    let gz = dir.as_ref().join(format!("{name}.gz"));
    if gz.is_file() {
        return Ok((gz, true));
    }
    Err(MetaError::Invalid(format!(
        "backup file not found: {}",
        plain.display()
    )))
}

pub fn validate_restore_chain(metas: &[Meta]) -> Result<(), MetaError> {
    if metas.is_empty() {
        return Err(MetaError::Invalid("empty backup chain".into()));
    }
    if metas[0].ty != TYPE_FULL {
        return Err(MetaError::Invalid(format!(
            "first backup must be type full, got {:?}",
            metas[0].ty
        )));
    }
    let mut prev_end = metas[0].end_lsn;
    let mut prev_sha = metas[0].sha256.clone();
    for (i, meta) in metas.iter().enumerate().skip(1) {
        if meta.ty != TYPE_DIFFERENTIAL {
            return Err(MetaError::Invalid(format!(
                "backup {i} must be differential, got {:?}",
                meta.ty
            )));
        }
        if meta.base_lsn != prev_end {
            return Err(MetaError::Invalid(format!(
                "backup {i} base_lsn {} does not match previous end_lsn {prev_end}",
                meta.base_lsn
            )));
        }
        if !meta.parent_sha256.is_empty() && meta.parent_sha256 != prev_sha {
            return Err(MetaError::Invalid(format!(
                "backup {i} parent_sha256 does not match previous backup"
            )));
        }
        prev_end = meta.end_lsn;
        prev_sha = meta.sha256.clone();
    }
    Ok(())
}

pub fn resolve_restore_chain(in_dir: impl AsRef<Path>, chain: &str) -> Result<Vec<PathBuf>, MetaError> {
    if chain.is_empty() {
        return Ok(vec![in_dir.as_ref().to_path_buf()]);
    }
    let dirs: Vec<PathBuf> = chain
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect();
    if dirs.is_empty() {
        return Err(MetaError::Invalid("empty --chain".into()));
    }
    Ok(dirs)
}

// Re-export type aliases for API symmetry with Go names.
pub use TYPE_DIFFERENTIAL as TypeDifferential;
pub use TYPE_FULL as TypeFull;

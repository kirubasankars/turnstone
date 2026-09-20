// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::recycle::create_allocated_wal_file;
use super::sync::sync_dir;
use crate::types::{DIR_MODE, FILE_MODE};

pub const WAL_DIR_NAME: &str = "wal";
pub const WAL_MANIFEST_NAME: &str = "manifest.json";
const WAL_MANIFEST_TMP_NAME: &str = "manifest.json.tmp";
pub const DEFAULT_WAL_SEG_SIZE: i64 = 64 << 20;
pub const WAL_MANIFEST_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalManifestSegment {
    pub id: u32,
    pub file: String,
    pub base_lsn: i64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub end_lsn: i64,
}

fn is_zero(v: &i64) -> bool {
    *v == 0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalManifest {
    pub version: i32,
    pub segment_size: i64,
    #[serde(default)]
    pub scan_floor: i64,
    pub active_id: u32,
    pub segments: Vec<WalManifestSegment>,
}

pub fn wal_segment_file_name(id: u32) -> String {
    format!("seg-{id:06}.wal")
}

pub fn wal_manifest_tmp_path(path: &Path) -> PathBuf {
    path.parent()
        .map(|d| d.join(WAL_MANIFEST_TMP_NAME))
        .unwrap_or_else(|| PathBuf::from(WAL_MANIFEST_TMP_NAME))
}

pub fn load_wal_manifest(path: &Path) -> io::Result<WalManifest> {
    match read_wal_manifest_file(path) {
        Ok(m) => return Ok(m),
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
        Err(_) => {}
    }
    let tmp = wal_manifest_tmp_path(path);
    match read_wal_manifest_file(&tmp) {
        Ok(m) => {
            fs::rename(&tmp, path)?;
            if let Some(dir) = path.parent() {
                sync_dir(dir.to_str().unwrap())?;
            }
            Ok(m)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(e),
        Err(_) => {
            let _ = fs::remove_file(&tmp);
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "wal manifest missing",
            ))
        }
    }
}

fn read_wal_manifest_file(path: &Path) -> io::Result<WalManifest> {
    let data = fs::read(path)?;
    let m: WalManifest =
        serde_json::from_slice(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if m.version != WAL_MANIFEST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported wal manifest version {}", m.version),
        ));
    }
    if m.segments.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wal manifest has no segments",
        ));
    }
    Ok(m)
}

pub fn save_wal_manifest(path: &Path, m: &WalManifest) -> io::Result<()> {
    let data =
        serde_json::to_vec_pretty(m).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut data = data;
    data.push(b'\n');
    write_file_atomic(path, &data)
}

fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let dir = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "manifest path has no parent")
    })?;
    let tmp = wal_manifest_tmp_path(path);
    {
        use std::fs::OpenOptions;
        use std::io::Write;
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_dir(dir.to_str().unwrap())
}

pub fn create_fresh_wal_manifest(wal_dir: &Path, segment_size: i64) -> io::Result<WalManifest> {
    fs::create_dir_all(wal_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut b = fs::DirBuilder::new();
        b.mode(DIR_MODE);
        let _ = b.create(wal_dir);
    }
    let seg_name = wal_segment_file_name(1);
    let seg_path = wal_dir.join(&seg_name);
    let f = create_allocated_wal_file(&seg_path, segment_size)?;
    drop(f);
    let m = WalManifest {
        version: WAL_MANIFEST_VERSION,
        segment_size,
        scan_floor: 0,
        active_id: 1,
        segments: vec![WalManifestSegment {
            id: 1,
            file: seg_name,
            base_lsn: 0,
            end_lsn: 0,
        }],
    };
    save_wal_manifest(&wal_dir.join(WAL_MANIFEST_NAME), &m)?;
    Ok(m)
}

pub fn normalize_wal_segment_size(size: i64) -> i64 {
    if size <= 0 {
        DEFAULT_WAL_SEG_SIZE
    } else {
        size
    }
}

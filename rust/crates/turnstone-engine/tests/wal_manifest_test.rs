// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs;
use std::io;
use std::path::PathBuf;

use turnstone_engine::wal::{
    load_wal_manifest, save_wal_manifest, wal_manifest_tmp_path, WalManifest, WalManifestSegment,
    WAL_MANIFEST_VERSION,
};

fn test_manifest(id: u32, file: &str) -> WalManifest {
    WalManifest {
        version: WAL_MANIFEST_VERSION,
        segment_size: 4096,
        scan_floor: 0,
        active_id: id,
        segments: vec![WalManifestSegment {
            id,
            file: file.to_string(),
            base_lsn: 0,
            end_lsn: 0,
        }],
    }
}

#[test]
fn save_wal_manifest_atomic_replace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manifest.json");

    save_wal_manifest(&path, &test_manifest(1, "seg-000001.wal")).unwrap();
    assert!(fs::metadata(wal_manifest_tmp_path(&path)).is_err());

    save_wal_manifest(&path, &test_manifest(2, "seg-000002.wal")).unwrap();
    let got = load_wal_manifest(&path).unwrap();
    assert_eq!(got.active_id, 2);
    assert_eq!(got.segments[0].file, "seg-000002.wal");
}

#[test]
fn save_wal_manifest_torn_tmp_keeps_old() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manifest.json");
    save_wal_manifest(&path, &test_manifest(1, "seg-000001.wal")).unwrap();
    fs::write(wal_manifest_tmp_path(&path), b"{").unwrap();
    let got = load_wal_manifest(&path).unwrap();
    assert_eq!(got.active_id, 1);
}

#[test]
fn load_wal_manifest_promotes_complete_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manifest.json");
    let tmp = wal_manifest_tmp_path(&path);
    let data = serde_json::to_vec_pretty(&test_manifest(3, "seg-000003.wal")).unwrap();
    let mut data = data;
    data.push(b'\n');
    fs::write(&tmp, data).unwrap();

    let got = load_wal_manifest(&path).unwrap();
    assert_eq!(got.active_id, 3);
    assert!(path.exists());
    assert!(fs::metadata(&tmp).is_err());
}

#[test]
fn load_wal_manifest_torn_tmp_without_dest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("manifest.json");
    fs::write(wal_manifest_tmp_path(&path), b"{").unwrap();
    let err = load_wal_manifest(&path).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);
}

#[test]
fn open_data_log_recovers_manifest_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let log = turnstone_engine::DataLog::open(dir.path(), 4096).unwrap();
    let payload = turnstone_engine::encode_record(&turnstone_engine::Record {
        ty: turnstone_engine::RecordType::Set,
        xid: 1,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    });
    log.append_encoded(&payload, true).unwrap();
    log.close().unwrap();

    let wal_dir: PathBuf = dir.path().join("wal");
    let path = wal_dir.join("manifest.json");
    let tmp = wal_manifest_tmp_path(&path);
    fs::rename(&path, &tmp).unwrap();

    let log2 = turnstone_engine::DataLog::open(dir.path(), 4096).unwrap();
    assert!(path.exists());
    assert_eq!(log2.segment_count(), 1);
    log2.close().unwrap();
}

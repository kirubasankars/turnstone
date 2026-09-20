// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_engine::hashindex::IndexExt;
use turnstone_engine::{
    encode_record, frame_size, replay_into_mem, validate_frames, DataLog, Record, RecordType,
};
use turnstone_engine::wal::wal_segment_file_name;

#[test]
fn wal_creates_fresh_manifest_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let log = DataLog::open(dir.path(), 0).unwrap();
    let manifest_path = dir.path().join("wal").join("manifest.json");
    assert!(manifest_path.exists());
    let seg_path = dir.path().join("wal").join(wal_segment_file_name(1));
    assert!(seg_path.exists());
    log.close().unwrap();
}

#[test]
fn wal_global_lsn_across_segment_rotation() {
    let dir = tempfile::tempdir().unwrap();
    const SEG_SIZE: i64 = 256;
    let log = DataLog::open(dir.path(), SEG_SIZE).unwrap();
    let payload = encode_record(&Record {
        ty: RecordType::Set,
        xid: 1,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    });
    let frame_len = frame_size(payload.len()) as i64;
    let mut first_off = -1i64;
    for _ in 0..20 {
        let off = log.append_encoded(&payload, false).unwrap();
        if first_off < 0 {
            first_off = off;
        }
    }
    assert!(
        log.segment_count() >= 2,
        "expected rotation, got {} segments",
        log.segment_count()
    );
    let val = log.read_value_at(first_off, 1).unwrap();
    assert_eq!(val, b"v");
    let last = log.write_offset();
    assert!(last > first_off + frame_len);
    log.close().unwrap();
}

#[test]
fn wal_read_log_range_crosses_segment_boundary() {
    let dir = tempfile::tempdir().unwrap();
    const SEG_SIZE: i64 = 128;
    let log = DataLog::open(dir.path(), SEG_SIZE).unwrap();
    let payload = encode_record(&Record {
        ty: RecordType::Set,
        xid: 1,
        key: b"k".to_vec(),
        value: b"value".to_vec(),
    });
    for _ in 0..15 {
        log.append_encoded(&payload, false).unwrap();
    }
    let head = log.write_offset();
    let (data, end) = log.read_log_range(0, head).unwrap();
    assert!(!data.is_empty() && end > 0);
    let frames = validate_frames(&data).unwrap();
    assert!(!frames.is_empty());
    log.close().unwrap();

    let log2 = DataLog::open(dir.path(), SEG_SIZE).unwrap();
    assert!(log2.segment_count() >= 2);
    log2.close().unwrap();
}

#[test]
fn wal_replay_preserves_global_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let log = DataLog::open(dir.path(), 200).unwrap();
    let begin = encode_record(&Record {
        ty: RecordType::Begin,
        xid: 1,
        key: vec![],
        value: vec![],
    });
    let set = encode_record(&Record {
        ty: RecordType::Set,
        xid: 1,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    });
    let commit = encode_record(&Record {
        ty: RecordType::Commit,
        xid: 1,
        key: vec![],
        value: vec![],
    });
    log.append_encoded(&begin, false).unwrap();
    let set_off = log.append_encoded(&set, false).unwrap();
    log.append_encoded(&commit, true).unwrap();
    log.close().unwrap();

    let log2 = DataLog::open(dir.path(), 200).unwrap();
    let (index, _, _) = replay_into_mem(&log2, false).unwrap();
    let chain = index.get_chain(b"k");
    let stored = chain[0].offset;
    assert_eq!(stored, set_off);
    let val = log2.read_value_at(stored, 1).unwrap();
    assert_eq!(val, b"v");
    log2.close().unwrap();
}

#[test]
fn wal_is_frame_boundary_at_segment_base() {
    let dir = tempfile::tempdir().unwrap();
    let log = DataLog::open(dir.path(), 128).unwrap();
    let payload = encode_record(&Record {
        ty: RecordType::Set,
        xid: 1,
        key: b"a".to_vec(),
        value: b"1".to_vec(),
    });
    for _ in 0..10 {
        log.append_encoded(&payload, false).unwrap();
    }
    assert!(log.segment_count() >= 2);
    let segs = log.segments();
    let base = segs[1].1;
    assert!(log.is_frame_boundary(base), "expected boundary at {base}");
    log.close().unwrap();
}

#[test]
fn recovery_uncommitted_not_in_index() {
    let dir = tempfile::tempdir().unwrap();
    let log = DataLog::open(dir.path(), 4096).unwrap();
    let begin = encode_record(&Record {
        ty: RecordType::Begin,
        xid: 1,
        key: vec![],
        value: vec![],
    });
    let set = encode_record(&Record {
        ty: RecordType::Set,
        xid: 1,
        key: b"crash_key".to_vec(),
        value: b"before_commit".to_vec(),
    });
    log.append_encoded(&begin, false).unwrap();
    log.append_encoded(&set, false).unwrap();
    log.close_active_file_for_test();
    log.close().unwrap();

    let log2 = DataLog::open(dir.path(), 4096).unwrap();
    let (index, clog, _) = replay_into_mem(&log2, true).unwrap();
    assert!(index.get_chain(b"crash_key").is_empty());
    assert_eq!(clog.clog_len(), 0);
    log2.close().unwrap();
}

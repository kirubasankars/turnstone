// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::Arc;
use std::time::Duration;

use turnstone_database::{
    open, DatabaseError, OpenOptions, REPLICA_ROLE_ADMIN, REPLICA_ROLE_BACKUP, REPLICA_ROLE_SERVER,
};

fn put_kv(db: &turnstone_database::Database, key: &str, val: &str) {
    let mut tx = db.new_transaction(true);
    tx.put(key.as_bytes(), val.as_bytes()).unwrap();
    tx.commit().unwrap();
}

#[test]
fn database_recover_basic() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s1 = open(&dir, OpenOptions::default()).unwrap();
        for k in ["alpha", "beta", "gamma"] {
            put_kv(&s1, k, &format!("val-{k}"));
        }
        s1.close().unwrap();
    }
    let s2 = open(&dir, OpenOptions::default()).unwrap();
    for k in ["alpha", "beta", "gamma"] {
        let val = s2.get(k).unwrap();
        assert_eq!(String::from_utf8_lossy(&val), format!("val-{k}"));
    }
    s2.close().unwrap();
}

#[test]
fn database_reset_wipes_keys() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "keep-me", "until-reset");
    s.reset().unwrap();
    assert!(s.get("keep-me").is_err());
    s.close().unwrap();
}

#[test]
fn database_replication_quorum() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(
        &dir,
        OpenOptions {
            min_replicas: 1,
            ..Default::default()
        },
    )
    .unwrap();
    s.register_replica("replica-1", 0, REPLICA_ROLE_SERVER);

    let s2 = Arc::clone(&s);
    let handle = std::thread::spawn(move || {
        let mut tx = s2.new_transaction(true);
        tx.put(b"k", b"v").unwrap();
        tx.commit().unwrap();
        s2.wait_for_quorum(s2.last_log_offset(), Duration::ZERO, None)
    });

    std::thread::sleep(Duration::from_millis(100));
    assert!(handle.is_finished() == false);

    s.update_replica_offset("replica-1", s.last_log_offset());
    handle.join().unwrap().unwrap();
}

#[test]
fn database_commit_puts() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "k1", "v1");
    put_kv(&s, "k2", "v2");
    assert_eq!(String::from_utf8_lossy(&s.get("k1").unwrap()), "v1");
    assert_eq!(String::from_utf8_lossy(&s.get("k2").unwrap()), "v2");
}

#[test]
fn stats_conflicts_and_storage() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    assert_eq!(s.stats().conflicts, 0);

    let mut tx1 = s.new_transaction(true);
    tx1.put(b"key", b"val1").unwrap();
    let mut tx2 = s.new_transaction(true);
    let _ = tx2.put(b"key", b"val2");
    tx1.commit().unwrap();
    assert!(tx2.commit().is_err());
    assert_eq!(s.stats().conflicts, 1);

    put_kv(&s, "k1", "v1");
    put_kv(&s, "k2", "v2");
    let stats = s.stats();
    assert!(stats.log_size > 0);
    assert_eq!(stats.log_size, stats.offset);
    assert!(stats.log_allocated > 0);

    let detail = s.storage_detail();
    assert!(!detail.segments.is_empty());
    assert!(detail.wal_live_bytes > 0);
    assert!(detail.index_allocated_bytes > detail.index_live_bytes);
    assert!(detail.index_arena_bytes >= detail.index_live_bytes);
    assert!(detail.hash_shards >= 1);
    let hash = s.index_hash_metrics();
    assert_eq!(hash.shards_used, detail.hash_shards);
    assert_eq!(s.wal_segment_count(), detail.segments.len() as i32);
}

#[test]
fn database_replica_lag() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "k1", "v1");
    let head = s.last_log_offset();
    s.register_replica("r1", head, REPLICA_ROLE_SERVER);
    assert_eq!(s.stats().replica_lag, 0);
    put_kv(&s, "k1", "v1");
    let new_head = s.last_log_offset();
    let stats = s.stats();
    assert_eq!(stats.replica_lag, new_head - head);
    assert_eq!(stats.replicas.len(), 1);
    assert_eq!(stats.server_replicas, 1);
}

#[test]
fn is_valid_replication_cursor_head_and_zero() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "k", "v");
    let head = s.last_log_offset();
    assert!(s.is_valid_replication_cursor(0));
    assert!(s.is_valid_replication_cursor(head));
    assert!(!s.is_valid_replication_cursor(head + 1));
}

#[test]
fn is_valid_replication_cursor_rejects_mid_frame() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "k", "v");
    let head = s.last_log_offset();
    assert!(head > 1);
    assert!(!s.is_valid_replication_cursor(1));
    assert!(s.engine().is_valid_frame_offset(0));
}

#[test]
fn min_replica_offset_connected_backup_pins() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(&dir, OpenOptions::default()).unwrap();
    put_kv(&s, "k", "v");
    let head = s.last_log_offset();
    s.register_replica("backup", head / 2, REPLICA_ROLE_BACKUP);
    assert_eq!(s.min_replica_offset(), head / 2);
    s.unregister_replica("backup");
    assert_eq!(s.min_replica_offset(), u64::MAX);
    s.register_replica("server", head / 3, REPLICA_ROLE_SERVER);
    assert_eq!(s.min_replica_offset(), head / 3);
}

#[test]
fn wait_for_quorum_ignores_non_server_roles() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(
        &dir,
        OpenOptions {
            min_replicas: 1,
            ..Default::default()
        },
    )
    .unwrap();
    put_kv(&s, "k", "v");
    let target = s.last_log_offset();
    s.register_replica("admin-only", target, REPLICA_ROLE_ADMIN);
    s.register_replica("backup-only", target, REPLICA_ROLE_BACKUP);

    let s2 = Arc::clone(&s);
    let handle =
        std::thread::spawn(move || s2.wait_for_quorum(target, Duration::from_millis(200), None));
    assert!(matches!(
        handle.join().unwrap(),
        Err(DatabaseError::Other(_))
    ));

    s.register_replica("server", target, REPLICA_ROLE_SERVER);
    s.wait_for_quorum(target, Duration::from_secs(1), None)
        .unwrap();
}

#[test]
fn register_replica_reregister_preserves_connected() {
    let dir = tempfile::tempdir().unwrap();
    let s = open(
        &dir,
        OpenOptions {
            min_replicas: 1,
            ..Default::default()
        },
    )
    .unwrap();
    put_kv(&s, "k", "v");
    let head = s.last_log_offset();
    let (_, gen1) = s.register_replica_hello("r1", head, REPLICA_ROLE_SERVER);
    let (_, gen2) = s.register_replica_hello("r1", head, REPLICA_ROLE_SERVER);
    assert_ne!(gen1, gen2);
    s.unregister_replica_gen("r1", gen1);
    s.wait_for_quorum(head, Duration::from_secs(1), None)
        .unwrap();
}

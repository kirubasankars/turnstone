// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use turnstone_engine::{Db, EngineError, Options};

#[test]
fn shared_buffers_hit_on_second_get() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        value_cache_bytes: -1,
        shared_buffers_bytes: 128 << 10,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    let buffers = db.shared_buffers().expect("shared buffers enabled").clone();

    {
        let mut tx = db.new_transaction(true);
        tx.put(b"buf", b"shared-page").unwrap();
        tx.commit().unwrap();
    }

    let misses_before = buffers.misses();
    {
        let mut tx = db.new_transaction(false);
        assert_eq!(tx.get(b"buf").unwrap(), b"shared-page");
        tx.discard();
    }
    assert!(
        buffers.misses() > misses_before,
        "first Get should populate shared buffers"
    );

    let hits_before = buffers.hits();
    {
        let mut tx = db.new_transaction(false);
        assert_eq!(tx.get(b"buf").unwrap(), b"shared-page");
        tx.discard();
    }
    assert!(
        buffers.hits() > hits_before,
        "second Get should hit shared buffers"
    );
    db.close().unwrap();
}

#[test]
fn shared_buffers_disabled() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        value_cache_bytes: -1,
        shared_buffers_bytes: -1,
        ..Options::default()
    };
    let db = Db::open(dir.path(), opts).unwrap();
    assert!(db.shared_buffers().is_none());

    let mut tx = db.new_transaction(true);
    tx.put(b"k", b"v").unwrap();
    tx.commit().unwrap();
    let tx = db.new_transaction(false);
    assert_eq!(tx.get(b"k").unwrap(), b"v");
    db.close().unwrap();
}

#[test]
fn group_commit_multiple_disjoint_keys() {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        commit_delay: Duration::from_millis(20),
        commit_siblings: 4,
        ..Options::default()
    };
    let db = Arc::new(Db::open(dir.path(), opts).unwrap());

    let db_a = db.clone();
    let h_a = thread::spawn(move || {
        let mut tx = db_a.new_transaction(true);
        tx.put(b"k_a", b"val_a").unwrap();
        tx.commit().unwrap();
    });
    let db_b = db.clone();
    let h_b = thread::spawn(move || {
        let mut tx = db_b.new_transaction(true);
        tx.put(b"k_b", b"val_b").unwrap();
        tx.commit().unwrap();
    });
    h_a.join().unwrap();
    h_b.join().unwrap();

    let tx = db.new_transaction(false);
    assert_eq!(tx.get(b"k_a").unwrap(), b"val_a");
    assert_eq!(tx.get(b"k_b").unwrap(), b"val_b");
    db.close().unwrap();
}

#[test]
fn wal_maintenance_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    for i in 0..32 {
        let mut tx = db.new_transaction(true);
        tx.put(format!("k{i}").as_bytes(), b"x").unwrap();
        tx.commit().unwrap();
    }
    db.run_wal_maintenance().expect("maintenance");
    let tx = db.new_transaction(false);
    assert_eq!(tx.get(b"k0").unwrap(), b"x");
    db.close().unwrap();
}

#[test]
fn maybe_compact_index_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();
    let mut tx = db.new_transaction(true);
    tx.put(b"a", b"1").unwrap();
    tx.commit().unwrap();
    let mut tx = db.new_transaction(true);
    tx.put(b"a", b"2").unwrap();
    tx.commit().unwrap();
    let _ = db.maybe_compact_index().expect("compact");
    let tx = db.new_transaction(false);
    assert_eq!(tx.get(b"a").unwrap(), b"2");
    db.close().unwrap();
}

#[test]
fn read_conflict_does_not_leave_key_locked() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), Options::default()).unwrap();

    {
        let mut tx = db.new_transaction(true);
        tx.put(b"k_conflict", b"v1").unwrap();
        tx.commit().unwrap();
    }

    let mut tx_b = db.new_transaction(true);
    assert_eq!(tx_b.get(b"k_conflict").unwrap(), b"v1");

    {
        let mut tx_c = db.new_transaction(true);
        tx_c.put(b"k_conflict", b"v2").unwrap();
        tx_c.commit().unwrap();
    }

    tx_b.put(b"k_b", b"val_b").unwrap();
    assert!(matches!(tx_b.commit(), Err(EngineError::WriteConflict)));

    assert!(matches!(
        db.new_transaction(false).get(b"k_b"),
        Err(EngineError::KeyNotFound)
    ));
    let mut tx = db.new_transaction(true);
    tx.put(b"k_b", b"val_b_retry").unwrap();
    tx.commit().unwrap();
    assert_eq!(
        db.new_transaction(false).get(b"k_b").unwrap(),
        b"val_b_retry"
    );
    db.close().unwrap();
}

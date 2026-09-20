// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Smoke tests for benchmark workloads (run under `cargo test`, no Criterion).

use std::time::{Duration, Instant};

use turnstone_engine::{Db, Options};

fn smoke_opts() -> Options {
    Options {
        unsafe_disable_fsync: true,
        ..Options::default()
    }
}

#[test]
fn benchmark_insert_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), smoke_opts()).unwrap();
    let val = b"benchmark_value_data_1234567890";
    let start = Instant::now();
    const N: u64 = 2_000;
    for i in 0..N {
        let mut tx = db.new_transaction(true);
        tx.put(format!("insert-key-{i}").as_bytes(), val).unwrap();
        tx.commit().unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!("benchmark_insert_smoke: {N} commits in {elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(120),
        "insert smoke too slow: {elapsed:?}"
    );
    db.close().unwrap();
}

#[test]
fn benchmark_read_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), smoke_opts()).unwrap();
    let val = b"v";
    for i in 0..500 {
        let mut tx = db.new_transaction(true);
        tx.put(format!("read-key-{i}").as_bytes(), val).unwrap();
        tx.commit().unwrap();
    }
    let start = Instant::now();
    for i in 0..500 {
        let tx = db.new_transaction(false);
        assert_eq!(tx.get(format!("read-key-{i}").as_bytes()).unwrap(), val);
    }
    let elapsed = start.elapsed();
    eprintln!("benchmark_read_smoke: 500 gets in {elapsed:?}");
    assert!(elapsed < Duration::from_secs(60));
    db.close().unwrap();
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Engine microbenchmarks (mirrors Go `engine/benchmark_test.go`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use turnstone_engine::{Db, Options};

fn bench_opts() -> Options {
    Options {
        unsafe_disable_fsync: true,
        commit_delay: Duration::ZERO,
        commit_siblings: 1,
        value_cache_bytes: -1,
        shared_buffers_bytes: -1,
        ..Options::default()
    }
}

fn open_db(path: &std::path::Path) -> Db {
    Db::open(path, bench_opts()).expect("open")
}

fn insert(c: &mut Criterion) {
    let val = b"benchmark_value_data_1234567890";
    c.bench_function("db_insert", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_db(dir.path());
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("insert-key-{i}");
            i += 1;
            let mut tx = db.new_transaction(true);
            tx.put(key.as_bytes(), val).expect("put");
            tx.commit().expect("commit");
            black_box(());
        });
        db.close().expect("close");
    });
}

fn update(c: &mut Criterion) {
    let val = b"benchmark_value_data_1234567890";
    const NUM_KEYS: i32 = 10_000;
    c.bench_function("db_update", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_db(dir.path());
        for i in 0..NUM_KEYS {
            let mut tx = db.new_transaction(true);
            tx.put(format!("update-key-{i}").as_bytes(), val)
                .expect("put");
            tx.commit().expect("commit");
        }
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let k = rng.gen_range(0..NUM_KEYS);
            let key = format!("update-key-{k}");
            let mut tx = db.new_transaction(true);
            tx.put(key.as_bytes(), val).expect("put");
            tx.commit().expect("commit");
            black_box(());
        });
        db.close().expect("close");
    });
}

fn read(c: &mut Criterion) {
    let val = b"benchmark_value_data_1234567890";
    const NUM_KEYS: i32 = 10_000;
    c.bench_function("db_read", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_db(dir.path());
        const BATCH: i32 = 100;
        for i in (0..NUM_KEYS).step_by(BATCH as usize) {
            let mut tx = db.new_transaction(true);
            for j in 0..BATCH {
                let idx = i + j;
                if idx >= NUM_KEYS {
                    break;
                }
                tx.put(format!("read-key-{idx}").as_bytes(), val)
                    .expect("put");
            }
            tx.commit().expect("commit");
        }
        let mut rng = rand::thread_rng();
        b.iter(|| {
            let k = rng.gen_range(0..NUM_KEYS);
            let key = format!("read-key-{k}");
            let tx = db.new_transaction(false);
            let v = tx.get(key.as_bytes()).expect("get");
            black_box(v);
        });
        db.close().expect("close");
    });
}

fn mixed(c: &mut Criterion) {
    let val = b"benchmark_value_data_1234567890";
    const NUM_KEYS: i32 = 10_000;
    c.bench_function("db_mixed", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = open_db(dir.path());
        for i in 0..NUM_KEYS {
            let mut tx = db.new_transaction(true);
            tx.put(format!("key-{i}").as_bytes(), val).expect("put");
            tx.commit().expect("commit");
        }
        let mut rng = rand::thread_rng();
        let mut i = 0u64;
        b.iter(|| {
            let k = rng.gen_range(0..NUM_KEYS);
            let key = format!("key-{k}");
            if i % 2 == 0 {
                let tx = db.new_transaction(false);
                let v = tx.get(key.as_bytes()).expect("get");
                black_box(v);
            } else {
                let mut tx = db.new_transaction(true);
                tx.put(key.as_bytes(), val).expect("put");
                tx.commit().expect("commit");
            }
            i += 1;
            black_box(());
        });
        db.close().expect("close");
    });
}

fn insert_parallel(c: &mut Criterion) {
    let val = b"benchmark_value_data_1234567890";
    c.bench_function("db_insert_parallel", |b| {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Arc::new(open_db(dir.path()));
        let seq = AtomicU64::new(0);
        b.iter(|| {
            std::thread::scope(|scope| {
                for _ in 0..4 {
                    let db = Arc::clone(&db);
                    scope.spawn(|| {
                        let n = seq.fetch_add(1, Ordering::Relaxed);
                        let key = format!("p-ins-{n}");
                        let mut tx = db.new_transaction(true);
                        tx.put(key.as_bytes(), val).expect("put");
                        tx.commit().expect("commit");
                    });
                }
            });
            black_box(());
        });
        Arc::try_unwrap(db).unwrap_or_else(|_| panic!("db still shared")).close().expect("close");
    });
}

fn group(c: &mut Criterion) {
    insert(c);
    update(c);
    read(c);
    mixed(c);
    insert_parallel(c);
}

criterion_group! {
    name = engine_benches;
    config = Criterion::default().sample_size(50);
    targets = group
}
criterion_main!(engine_benches);

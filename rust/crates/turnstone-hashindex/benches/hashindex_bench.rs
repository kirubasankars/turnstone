// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Hash-index microbenchmarks (mirrors Go `engine/hashindex/benchmark_test.go`).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use turnstone_hashindex::{hash_key, Index, Version};

fn shard_keys_for_bench(n: usize, shard: u8) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(n);
    let mut i = 0;
    while keys.len() < n {
        let key = format!("bench-key-{i}");
        i += 1;
        if (hash_key(key.as_bytes()) & 255) as u8 == shard {
            keys.push(key.into_bytes());
        }
    }
    keys
}

fn index_put(c: &mut Criterion) {
    let keys = shard_keys_for_bench(512, 0);
    c.bench_function("index_put", |b| {
        let idx = Index::new();
        let mut i = 0u64;
        b.iter(|| {
            let key = &keys[(i as usize) % keys.len()];
            i += 1;
            idx.put(
                key,
                Version {
                    offset: i as i64,
                    value_len: 64,
                    xmin: i + 1,
                    tombstone: false,
                },
            )
            .expect("put");
            black_box(());
        });
        idx.close().expect("close");
    });
}

fn index_grow_shard(c: &mut Criterion) {
    let keys = shard_keys_for_bench(1500, 0);
    c.bench_function("index_grow_shard", |b| {
        let idx = Index::new();
        let mut i = 0u64;
        b.iter(|| {
            let key = &keys[(i as usize) % keys.len()];
            i += 1;
            idx.put(
                key,
                Version {
                    offset: i as i64,
                    value_len: 32,
                    xmin: i + 1,
                    tombstone: false,
                },
            )
            .expect("put");
            black_box(());
        });
        idx.close().expect("close");
    });
}

fn fragment_key_for_compact(idx: &Index, key: &[u8]) {
    idx.put(
        key,
        Version {
            offset: 100,
            value_len: 64,
            xmin: 1,
            tombstone: false,
        },
    )
    .expect("put");
    for xid in 2..=8u64 {
        idx.put(
            key,
            Version {
                offset: (xid * 10) as i64,
                value_len: 64,
                xmin: xid,
                tombstone: false,
            },
        )
        .expect("put");
        idx.drop_xid(xid).expect("drop_xid");
    }
}

fn compact_shard(c: &mut Criterion) {
    let key = b"compact-bench-key";
    let shard_idx = (hash_key(key) & 255) as i32;
    let filter = |_: &[u8], chain: &[Version]| -> Vec<Version> {
        if chain.is_empty() {
            return Vec::new();
        }
        chain[..1].to_vec()
    };

    c.bench_function("compact_shard", |b| {
        let idx = Index::new();
        b.iter(|| {
            fragment_key_for_compact(&idx, key);
            idx.compact_shard(shard_idx, Some(&filter))
                .expect("compact");
            black_box(());
        });
        idx.close().expect("close");
    });
}

fn group(c: &mut Criterion) {
    index_put(c);
    index_grow_shard(c);
    compact_shard(c);
}

criterion_group! {
    name = hashindex_benches;
    config = Criterion::default().sample_size(50);
    targets = group
}
criterion_main!(hashindex_benches);

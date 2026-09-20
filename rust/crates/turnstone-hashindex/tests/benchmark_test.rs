// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::time::{Duration, Instant};

use turnstone_hashindex::{hash_key, Index, Version};

#[test]
fn benchmark_index_put_smoke() {
    let idx = Index::new();
    let keys: Vec<Vec<u8>> = (0..512)
        .filter_map(|i| {
            let key = format!("bench-key-{i}");
            if (hash_key(key.as_bytes()) & 255) == 0 {
                Some(key.into_bytes())
            } else {
                None
            }
        })
        .collect();
    assert!(!keys.is_empty());

    let start = Instant::now();
    for i in 0..10_000u64 {
        let key = &keys[(i as usize) % keys.len()];
        idx.put(
            key,
            Version {
                offset: i as i64,
                value_len: 64,
                xmin: i + 1,
                tombstone: false,
            },
        )
        .unwrap();
    }
    let elapsed = start.elapsed();
    eprintln!("benchmark_index_put_smoke: 10000 puts in {elapsed:?}");
    assert!(elapsed < Duration::from_secs(30));
    idx.close().unwrap();
}

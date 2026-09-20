// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Sharded in-memory MVCC hash index (compatible with Go `engine/hashindex`).

mod arena;
mod arena_heap;
#[cfg(unix)]
mod arena_mmap;
mod buffer_size;
mod budget;
mod compact;
mod encoding;
mod errors;
mod index;
mod shard;

pub use budget::SharedBudget;
pub use compact::{IndexStats, ShardStats, VersionFilter};
pub use encoding::{hash_key, NUM_SHARDS};
pub use errors::ErrArenaLimit;
pub use index::Index;

/// One MVCC index entry pointing at a log record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Version {
    pub offset: i64,
    pub value_len: u32,
    pub xmin: u64,
    pub tombstone: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_walk_versions() {
        let idx = Index::new();
        let key = b"alpha";
        idx.put(key, Version {
            offset: 10,
            value_len: 3,
            xmin: 1,
            tombstone: false,
        })
        .unwrap();
        idx.put(key, Version {
            offset: 20,
            value_len: 3,
            xmin: 2,
            tombstone: false,
        })
        .unwrap();

        let mut chain = Vec::new();
        idx.walk_versions(key, |v| {
            chain.push(v);
            true
        });
        assert_eq!(chain.len(), 2);
        assert_eq!(chain[0].xmin, 2);
        assert_eq!(chain[1].xmin, 1);
    }

    #[test]
    fn for_each_key_and_drop_xid() {
        let idx = Index::new();
        idx.put(b"a", Version {
            offset: 1,
            value_len: 0,
            xmin: 1,
            tombstone: false,
        })
        .unwrap();
        idx.put(b"b", Version {
            offset: 2,
            value_len: 0,
            xmin: 2,
            tombstone: false,
        })
        .unwrap();
        idx.put(b"a", Version {
            offset: 3,
            value_len: 0,
            xmin: 3,
            tombstone: false,
        })
        .unwrap();

        let mut count = 0;
        idx.for_each_key(|_, chain| {
            count += 1;
            assert!(!chain.is_empty());
        });
        assert_eq!(count, 2);

        idx.drop_xid(2).unwrap();
        let mut found_b = false;
        idx.for_each_key(|key, chain| {
            if key == b"b" {
                found_b = true;
                assert!(chain.is_empty());
            }
        });
        assert!(!found_b);
    }

    #[test]
    fn hash_table_grow_same_shard() {
        let idx = Index::new();
        let mut keys = Vec::new();
        let mut i = 0;
        while keys.len() < 1500 {
            let k = format!("grow-key-{i}");
            i += 1;
            if hash_key(k.as_bytes()) & 255 == 0 {
                keys.push(k.into_bytes());
            }
        }

        for (i, key) in keys.iter().enumerate() {
            idx.put(key, Version {
                offset: i as i64,
                value_len: 0,
                xmin: (i + 1) as u64,
                tombstone: false,
            })
            .unwrap();
        }

        let mut seen = 0;
        idx.for_each_key(|_, _| seen += 1);
        assert_eq!(seen, keys.len());
    }

    #[test]
    fn hash_table_grow_many_keys() {
        let idx = Index::new();
        const N: usize = 5000;
        for i in 0..N {
            let key = format!("grow-key-{i}");
            idx.put(key.as_bytes(), Version {
                offset: i as i64,
                value_len: 0,
                xmin: (i + 1) as u64,
                tombstone: false,
            })
            .unwrap();
        }

        let mut seen = 0;
        idx.for_each_key(|_, chain| {
            seen += 1;
            assert!(!chain.is_empty());
        });
        assert_eq!(seen, N);
    }

    #[test]
    fn concurrent_puts_different_keys() {
        use std::sync::Arc;
        use std::thread;

        let idx = Arc::new(Index::new());
        const N: usize = 200;
        let mut handles = Vec::new();
        for i in 0..N {
            let idx = Arc::clone(&idx);
            handles.push(thread::spawn(move || {
                let key = format!("key-{i}");
                idx.put(key.as_bytes(), Version {
                    offset: i as i64,
                    value_len: 4,
                    xmin: (i + 1) as u64,
                    tombstone: false,
                })
                .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut seen = 0;
        idx.for_each_key(|_, chain| {
            seen += 1;
            assert_eq!(chain.len(), 1);
        });
        assert_eq!(seen, N);
    }

    #[test]
    fn filtered_live_bytes_respects_version_filter() {
        let idx = Index::new();
        let key = b"k";
        for i in 1..=4 {
            idx.put(key, Version {
                offset: (i * 10) as i64,
                value_len: 1,
                xmin: i as u64,
                tombstone: false,
            })
            .unwrap();
        }
        let shard_idx = (hash_key(key) & 255) as i32;
        let keep_newest = |_: &[u8], chain: &[Version]| -> Vec<Version> {
            if chain.is_empty() {
                return Vec::new();
            }
            chain[..1].to_vec()
        };
        let (arena, filtered, keys) = idx.filtered_live_bytes(shard_idx, Some(&keep_newest));
        let (_, linked, _) = idx.filtered_live_bytes(shard_idx, None);
        assert_eq!(keys, 1);
        assert!(arena > 0);
        assert!(linked > 0);
        assert!(filtered < linked);
        assert!(arena >= linked);

        let (a, l, k) = idx.filtered_live_bytes(-1, None);
        assert_eq!((a, l, k), (0, 0, 0));
    }

    #[test]
    fn compact_shard_drops_old_versions_and_shrinks_arena() {
        let idx = Index::new();
        let key = b"k";
        for i in 1..=5 {
            idx.put(key, Version {
                offset: (i * 10) as i64,
                value_len: 1,
                xmin: i as u64,
                tombstone: false,
            })
            .unwrap();
        }

        let shard_idx = (hash_key(key) & 255) as i32;
        let before = idx.stats().shards[shard_idx as usize].arena_used;
        assert!(before > 0);

        let filter = |_: &[u8], chain: &[Version]| -> Vec<Version> {
            if chain.is_empty() {
                return Vec::new();
            }
            chain[..1].to_vec()
        };
        let after_stats = idx.compact_shard(shard_idx, Some(&filter)).unwrap();
        assert!(after_stats.arena_used < before);

        let mut chain = Vec::new();
        idx.walk_versions(key, |v| {
            chain.push(v);
            true
        });
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].xmin, 5);
    }

    #[test]
    fn compact_shard_removes_key_when_filter_empty() {
        let idx = Index::new();
        let key = b"drop-me";
        idx.put(key, Version {
            offset: 1,
            value_len: 0,
            xmin: 1,
            tombstone: false,
        })
        .unwrap();

        let shard_idx = (hash_key(key) & 255) as i32;
        let filter = |_: &[u8], _: &[Version]| -> Vec<Version> { Vec::new() };
        idx.compact_shard(shard_idx, Some(&filter)).unwrap();

        let mut seen = false;
        idx.for_each_key(|k, _| {
            if k == key {
                seen = true;
            }
        });
        assert!(!seen);
    }
}

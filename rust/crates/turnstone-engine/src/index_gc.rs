// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use turnstone_hashindex::{Version, VersionFilter};

use crate::types::{IndexVersion, Snapshot, TxStatus};
use crate::Db;

pub(crate) trait IndexGcDb {
    fn gc_clog_status(&self, xid: u64) -> TxStatus;
    fn gc_is_visible(&self, xmin: u64, snap: &Snapshot) -> bool;
}

impl IndexGcDb for Db {
    fn gc_clog_status(&self, xid: u64) -> TxStatus {
        self.clog_status(xid)
    }

    fn gc_is_visible(&self, xmin: u64, snap: &Snapshot) -> bool {
        self.is_visible(xmin, snap)
    }
}

pub const DEFAULT_INDEX_FRAGMENTATION_RATIO: f64 = 3.0;

pub struct IndexGcReader {
    pub snapshot: Snapshot,
    pub my_xid: u64,
    pub update: bool,
}

pub struct IndexGcContext {
    pub readers: Vec<IndexGcReader>,
    pub active_xids: HashSet<u64>,
    pub log_floor: i64,
}

#[derive(Debug, Clone, Default)]
pub struct IndexCompactResult {
    pub shards_compacted: i32,
    pub arena_before: u64,
    pub arena_after: u64,
}

impl Db {
    pub fn build_index_gc_context(&self) -> IndexGcContext {
        let readers: Vec<IndexGcReader> = self
            .active_txns
            .lock()
            .values()
            .map(|reg| IndexGcReader {
                snapshot: reg.snapshot.clone(),
                my_xid: reg.my_xid,
                update: reg.update,
            })
            .collect();
        let active_xids: HashSet<u64> = self.active_xids.lock().keys().copied().collect();
        IndexGcContext {
            readers,
            active_xids,
            log_floor: self.scan_floor.load(Ordering::Acquire),
        }
    }

    pub fn maybe_compact_index(&self) -> Result<IndexCompactResult, crate::types::EngineError> {
        let _guard = self.wal_rewrite_mu.write();
        let ratio = if self.index_fragmentation_ratio > 0.0 {
            self.index_fragmentation_ratio
        } else {
            DEFAULT_INDEX_FRAGMENTATION_RATIO
        };
        let ctx = self.build_index_gc_context();
        let filter = self.index_wal_retain_filter(&ctx);
        let filter_ref: &VersionFilter<'_> = filter.as_ref();
        let mut before = 0u64;
        let mut after = 0u64;
        let mut compacted = 0i32;
        self.index.with_hash_index(|idx| {
            let n = idx.shard_count();
            for i in 0..n {
                let (arena, live, keys) = idx.filtered_live_bytes(i as i32, Some(filter_ref));
                before += arena;
                if keys == 0 {
                    after += arena;
                    continue;
                }
                if live > 0 && arena as f64 <= live as f64 * ratio {
                    after += arena;
                    continue;
                }
                if let Ok(st) = idx.compact_shard(i as i32, Some(filter_ref)) {
                    compacted += 1;
                    after += st.arena_used;
                }
            }
        });
        if compacted == 0 {
            return Ok(IndexCompactResult::default());
        }
        let res = IndexCompactResult {
            shards_compacted: compacted,
            arena_before: before,
            arena_after: after,
        };
        self.record_index_compact(&res);
        Ok(res)
    }

    /// Rewrites every shard, dropping versions below the log floor (Go `CompactIndex`).
    pub fn compact_index(
        &self,
        ctx: &IndexGcContext,
    ) -> Result<IndexCompactResult, crate::types::EngineError> {
        let _guard = self.wal_rewrite_mu.write();
        let mut before = 0u64;
        let mut shards_compacted = 0i32;
        self.index.with_hash_index(|idx| {
            let stats = idx.stats();
            for st in stats.shards {
                before += st.arena_used;
                if st.key_count > 0 {
                    shards_compacted += 1;
                }
            }
        });
        let filter = self.index_version_filter(ctx);
        let stats_after = self.index.compact_all_filtered(Some(filter.as_ref()))?;
        let mut after = 0u64;
        for st in stats_after.shards {
            after += st.arena_used;
        }
        let res = IndexCompactResult {
            shards_compacted,
            arena_before: before,
            arena_after: after,
        };
        self.record_index_compact(&res);
        Ok(res)
    }

    fn record_index_compact(&self, res: &IndexCompactResult) {
        if res.shards_compacted <= 0 {
            return;
        }
        self.metrics_hash_shards_compacted
            .fetch_add(res.shards_compacted as u64, Ordering::AcqRel);
        if res.arena_before > res.arena_after {
            self.metrics_hash_compact_reclaimed
                .fetch_add(res.arena_before - res.arena_after, Ordering::AcqRel);
        }
        self.metrics_hash_compact_unix.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            Ordering::Release,
        );
    }

    pub(crate) fn index_wal_retain_filter<'a>(
        &'a self,
        ctx: &'a IndexGcContext,
    ) -> Box<VersionFilter<'a>> {
        let db = self;
        Box::new(move |key: &[u8], chain: &[Version]| {
            let kept = ctx.filter_versions_for_wal_retain(key, chain, db);
            kept.into_iter().map(to_hash_version).collect()
        })
    }

    pub(crate) fn index_version_filter<'a>(
        &'a self,
        ctx: &'a IndexGcContext,
    ) -> Box<VersionFilter<'a>> {
        let db = self;
        Box::new(move |key: &[u8], chain: &[Version]| {
            let in_v: Vec<IndexVersion> = chain.iter().map(from_hash_version).collect();
            let kept = ctx.filter_versions(key, &in_v, db, true);
            kept.into_iter().map(to_hash_version).collect()
        })
    }
}

impl IndexGcContext {
    pub(crate) fn filter_versions(
        &self,
        _key: &[u8],
        chain: &[IndexVersion],
        db: &dyn IndexGcDb,
        apply_log_floor: bool,
    ) -> Vec<IndexVersion> {
        let mask = self.keep_mask(chain, db, apply_log_floor);
        chain
            .iter()
            .zip(mask.iter())
            .filter_map(|(v, k)| if *k { Some(*v) } else { None })
            .collect()
    }

    pub(crate) fn filter_versions_for_wal_retain(
        &self,
        key: &[u8],
        chain: &[Version],
        db: &dyn IndexGcDb,
    ) -> Vec<IndexVersion> {
        let in_v: Vec<IndexVersion> = chain.iter().map(from_hash_version).collect();
        self.filter_versions(key, &in_v, db, false)
    }

    fn keep_mask(
        &self,
        chain: &[IndexVersion],
        db: &dyn IndexGcDb,
        apply_log_floor: bool,
    ) -> Vec<bool> {
        let n = chain.len();
        let mut keep = vec![false; n];
        if self.readers.is_empty() {
            for (i, v) in chain.iter().enumerate() {
                keep[i] = true;
                if db.gc_clog_status(v.xmin) == TxStatus::Committed {
                    break;
                }
            }
        } else {
            for r in &self.readers {
                for (i, v) in chain.iter().enumerate() {
                    keep[i] = true;
                    if r.update && v.xmin == r.my_xid {
                        break;
                    }
                    if db.gc_is_visible(v.xmin, &r.snapshot) {
                        break;
                    }
                }
            }
        }
        for (i, v) in chain.iter().enumerate() {
            if self.active_xids.contains(&v.xmin) {
                keep[i] = true;
            }
            if apply_log_floor && self.log_floor > 0 && v.offset < self.log_floor {
                keep[i] = false;
            }
        }
        keep
    }
}

fn from_hash_version(v: &Version) -> IndexVersion {
    IndexVersion {
        offset: v.offset,
        value_len: v.value_len,
        xmin: v.xmin,
        tombstone: v.tombstone,
    }
}

fn to_hash_version(v: IndexVersion) -> Version {
    Version {
        offset: v.offset,
        value_len: v.value_len,
        xmin: v.xmin,
        tombstone: v.tombstone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    struct MockGcDb {
        clog: Box<dyn Fn(u64) -> TxStatus + Send + Sync>,
        visible: Box<dyn Fn(u64, &Snapshot) -> bool + Send + Sync>,
    }

    impl IndexGcDb for MockGcDb {
        fn gc_clog_status(&self, xid: u64) -> TxStatus {
            (self.clog)(xid)
        }

        fn gc_is_visible(&self, xmin: u64, snap: &Snapshot) -> bool {
            (self.visible)(xmin, snap)
        }
    }

    fn committed_clog() -> MockGcDb {
        MockGcDb {
            clog: Box::new(|_| TxStatus::Committed),
            visible: Box::new(|xmin, snap| xmin < snap.xmax && !snap.contains(xmin)),
        }
    }

    fn chain54321() -> Vec<IndexVersion> {
        vec![
            IndexVersion {
                offset: 500,
                value_len: 0,
                xmin: 5,
                tombstone: false,
            },
            IndexVersion {
                offset: 400,
                value_len: 0,
                xmin: 4,
                tombstone: false,
            },
            IndexVersion {
                offset: 300,
                value_len: 0,
                xmin: 3,
                tombstone: false,
            },
            IndexVersion {
                offset: 200,
                value_len: 0,
                xmin: 2,
                tombstone: false,
            },
            IndexVersion {
                offset: 100,
                value_len: 0,
                xmin: 1,
                tombstone: false,
            },
        ]
    }

    fn filtered_xmins(ctx: &IndexGcContext, chain: &[IndexVersion], db: &MockGcDb) -> Vec<u64> {
        ctx.filter_versions(b"k", chain, db, true)
            .into_iter()
            .map(|v| v.xmin)
            .collect()
    }

    #[test]
    fn keep_mask_regression_table() {
        let cases: Vec<(&str, Vec<IndexVersion>, IndexGcContext, Vec<u64>, MockGcDb)> = vec![
            (
                "single reader sees head only",
                chain54321(),
                IndexGcContext {
                    readers: vec![IndexGcReader {
                        snapshot: Snapshot {
                            xmax: 10,
                            xip: HashMap::new(),
                        },
                        my_xid: 0,
                        update: false,
                    }],
                    active_xids: HashSet::new(),
                    log_floor: 0,
                },
                vec![5],
                committed_clog(),
            ),
            (
                "reader skips invisible head keeps prefix",
                vec![
                    IndexVersion {
                        offset: 300,
                        value_len: 0,
                        xmin: 5,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 200,
                        value_len: 0,
                        xmin: 4,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 100,
                        value_len: 0,
                        xmin: 3,
                        tombstone: false,
                    },
                ],
                IndexGcContext {
                    readers: vec![IndexGcReader {
                        snapshot: Snapshot {
                            xmax: 5,
                            xip: HashMap::from([(4, true)]),
                        },
                        my_xid: 0,
                        update: false,
                    }],
                    active_xids: HashSet::new(),
                    log_floor: 0,
                },
                vec![5, 4, 3],
                committed_clog(),
            ),
            (
                "multi reader union keeps deepest skip prefix",
                chain54321(),
                IndexGcContext {
                    readers: vec![
                        IndexGcReader {
                            snapshot: Snapshot {
                                xmax: 10,
                                xip: HashMap::new(),
                            },
                            my_xid: 0,
                            update: false,
                        },
                        IndexGcReader {
                            snapshot: Snapshot {
                                xmax: 4,
                                xip: HashMap::new(),
                            },
                            my_xid: 0,
                            update: false,
                        },
                    ],
                    active_xids: HashSet::new(),
                    log_floor: 0,
                },
                vec![5, 4, 3],
                committed_clog(),
            ),
            (
                "no readers keeps through newest committed",
                chain54321(),
                IndexGcContext {
                    readers: vec![],
                    active_xids: HashSet::new(),
                    log_floor: 0,
                },
                vec![5],
                committed_clog(),
            ),
            (
                "no readers walks in progress head to committed",
                vec![
                    IndexVersion {
                        offset: 200,
                        value_len: 0,
                        xmin: 2,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 100,
                        value_len: 0,
                        xmin: 1,
                        tombstone: false,
                    },
                ],
                IndexGcContext {
                    readers: vec![],
                    active_xids: HashSet::new(),
                    log_floor: 0,
                },
                vec![2, 1],
                MockGcDb {
                    clog: Box::new(|xid| {
                        if xid == 1 {
                            TxStatus::Committed
                        } else {
                            TxStatus::InProgress
                        }
                    }),
                    visible: Box::new(|xmin, snap| xmin < snap.xmax && !snap.contains(xmin)),
                },
            ),
            (
                "active xid forces keep of in progress version",
                vec![
                    IndexVersion {
                        offset: 200,
                        value_len: 0,
                        xmin: 7,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 100,
                        value_len: 0,
                        xmin: 1,
                        tombstone: false,
                    },
                ],
                IndexGcContext {
                    readers: vec![],
                    active_xids: HashSet::from([7]),
                    log_floor: 0,
                },
                vec![7],
                committed_clog(),
            ),
            (
                "log floor drops below floor even in skip prefix",
                vec![
                    IndexVersion {
                        offset: 400,
                        value_len: 0,
                        xmin: 4,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 300,
                        value_len: 0,
                        xmin: 3,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 200,
                        value_len: 0,
                        xmin: 2,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 50,
                        value_len: 0,
                        xmin: 1,
                        tombstone: false,
                    },
                ],
                IndexGcContext {
                    readers: vec![IndexGcReader {
                        snapshot: Snapshot {
                            xmax: 2,
                            xip: HashMap::new(),
                        },
                        my_xid: 0,
                        update: false,
                    }],
                    active_xids: HashSet::new(),
                    log_floor: 100,
                },
                vec![4, 3, 2],
                committed_clog(),
            ),
            (
                "writable reader keeps own uncommitted write only in prefix",
                vec![
                    IndexVersion {
                        offset: 300,
                        value_len: 0,
                        xmin: 3,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 200,
                        value_len: 0,
                        xmin: 2,
                        tombstone: false,
                    },
                    IndexVersion {
                        offset: 100,
                        value_len: 0,
                        xmin: 1,
                        tombstone: false,
                    },
                ],
                IndexGcContext {
                    readers: vec![IndexGcReader {
                        snapshot: Snapshot {
                            xmax: 4,
                            xip: HashMap::from([(3, true)]),
                        },
                        my_xid: 3,
                        update: true,
                    }],
                    active_xids: HashSet::from([3]),
                    log_floor: 0,
                },
                vec![3],
                committed_clog(),
            ),
        ];

        for (name, chain, ctx, want, db) in cases {
            let got = filtered_xmins(&ctx, &chain, &db);
            assert_eq!(got, want, "case {name}");
        }
    }

    #[test]
    fn build_index_gc_context_includes_open_read_txn() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), crate::Options::default()).unwrap();
        {
            let mut tx = db.new_transaction(true);
            tx.put(b"k", b"v").unwrap();
            tx.commit().unwrap();
        }
        let mut read_tx = db.new_transaction(false);
        let ctx = db.build_index_gc_context();
        assert_eq!(ctx.readers.len(), 1);
        assert!(!ctx.readers[0].update);
        read_tx.discard();
        assert!(db.build_index_gc_context().readers.is_empty());
        db.close().unwrap();
    }
}

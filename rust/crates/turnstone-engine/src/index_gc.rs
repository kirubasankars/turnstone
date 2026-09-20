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
        let active_txns: Vec<u64> = self.active_txns.lock().values().copied().collect();
        let active_xids: HashSet<u64> = self
            .active_xids
            .lock()
            .keys()
            .copied()
            .collect();
        let readers = Vec::new(); // snapshot readers tracked via active txn tokens in full port
        let _ = active_txns;
        IndexGcContext {
            readers,
            active_xids,
            log_floor: self.scan_floor.load(Ordering::Acquire),
        }
    }

    pub fn maybe_compact_index(&self) -> Result<IndexCompactResult, crate::types::EngineError> {
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

    fn record_index_compact(&self, res: &IndexCompactResult) {
        if res.shards_compacted <= 0 {
            return;
        }
        self.metrics_hash_shards_compacted
            .fetch_add(res.shards_compacted as u64, Ordering::AcqRel);
        if res.arena_before > res.arena_after {
            self.metrics_hash_compact_reclaimed.fetch_add(
                res.arena_before - res.arena_after,
                Ordering::AcqRel,
            );
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
    pub fn filter_versions(
        &self,
        _key: &[u8],
        chain: &[IndexVersion],
        db: &Db,
        apply_log_floor: bool,
    ) -> Vec<IndexVersion> {
        let mask = self.keep_mask(chain, db, apply_log_floor);
        chain
            .iter()
            .zip(mask.iter())
            .filter_map(|(v, k)| if *k { Some(*v) } else { None })
            .collect()
    }

    pub fn filter_versions_for_wal_retain(
        &self,
        key: &[u8],
        chain: &[Version],
        db: &Db,
    ) -> Vec<IndexVersion> {
        let in_v: Vec<IndexVersion> = chain.iter().map(from_hash_version).collect();
        self.filter_versions(key, &in_v, db, false)
    }

    fn keep_mask(&self, chain: &[IndexVersion], db: &Db, apply_log_floor: bool) -> Vec<bool> {
        let n = chain.len();
        let mut keep = vec![false; n];
        if self.readers.is_empty() {
            for (i, v) in chain.iter().enumerate() {
                keep[i] = true;
                if db.clog_status(v.xmin) == TxStatus::Committed {
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
                    if db.is_visible(v.xmin, &r.snapshot) {
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

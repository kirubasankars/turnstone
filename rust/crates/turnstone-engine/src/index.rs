// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use parking_lot::RwLock;

use crate::hashindex::{Index, Version};
use crate::types::{EngineError, IndexVersion, Snapshot, TxStatus};

/// MVCC index wrapper (matches Go `engine.Index`).
pub struct MvccIndex {
    inner: RwLock<Option<Index>>,
}

impl Default for MvccIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccIndex {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Some(Index::new())),
        }
    }

    pub fn from_index(index: Index) -> Self {
        Self {
            inner: RwLock::new(Some(index)),
        }
    }

    pub fn close(&self) -> Result<(), EngineError> {
        let mut guard = self.inner.write();
        if let Some(idx) = guard.take() {
            idx.close().map_err(|e| EngineError::Other(e.to_string()))?;
        }
        Ok(())
    }

    pub fn put(&self, key: &[u8], v: IndexVersion) -> Result<(), EngineError> {
        let guard = self.inner.read();
        let Some(idx) = guard.as_ref() else {
            return Err(EngineError::DatabaseClosed);
        };
        idx.put(
            key,
            Version {
                offset: v.offset,
                value_len: v.value_len,
                xmin: v.xmin,
                tombstone: v.tombstone,
            },
        )
        .map_err(|e| EngineError::Other(e.to_string()))
    }

    pub fn drop_xid(&self, xid: u64) -> Result<(), EngineError> {
        let guard = self.inner.read();
        let Some(idx) = guard.as_ref() else {
            return Ok(());
        };
        idx.drop_xid(xid)
            .map_err(|e| EngineError::Other(e.to_string()))
    }

    pub(crate) fn with_hash_index<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Index) -> R,
        R: Default,
    {
        self.with_index(f)
    }

    pub(crate) fn compact_all_filtered(
        &self,
        filter: Option<&turnstone_hashindex::VersionFilter<'_>>,
    ) -> Result<turnstone_hashindex::IndexStats, EngineError> {
        let guard = self.inner.read();
        let Some(idx) = guard.as_ref() else {
            return Err(EngineError::DatabaseClosed);
        };
        idx.compact_all(filter)
            .map_err(|e| EngineError::Other(e.to_string()))
    }

    fn with_index<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Index) -> R,
        R: Default,
    {
        let guard = self.inner.read();
        match guard.as_ref() {
            Some(idx) => f(idx),
            None => R::default(),
        }
    }

    pub fn walk_key_versions<F>(&self, key: &[u8], mut f: F)
    where
        F: FnMut(IndexVersion) -> bool,
    {
        self.with_index(|idx| {
            idx.walk_versions(key, |v| {
                f(IndexVersion {
                    offset: v.offset,
                    value_len: v.value_len,
                    xmin: v.xmin,
                    tombstone: v.tombstone,
                })
            });
        });
    }

    pub fn latest_resolved(
        &self,
        key: &[u8],
        exclude_xid: u64,
        clog: impl Fn(u64) -> TxStatus,
    ) -> (Option<IndexVersion>, u64, bool) {
        let mut found = None;
        let mut found_xmin = 0u64;
        self.walk_key_versions(key, |v| {
            if v.xmin == exclude_xid {
                return true;
            }
            if clog(v.xmin) == TxStatus::Aborted {
                return true;
            }
            found = Some(v);
            found_xmin = v.xmin;
            false
        });
        match found {
            Some(v) => (Some(v), found_xmin, true),
            None => (None, 0, false),
        }
    }

    pub fn get_visible(
        &self,
        key: &[u8],
        snap: &Snapshot,
        my_xid: u64,
        update: bool,
        visible: impl Fn(u64, &Snapshot) -> bool,
    ) -> (Option<IndexVersion>, bool) {
        let mut found = None;
        self.walk_key_versions(key, |v| {
            if update && v.xmin == my_xid {
                found = Some(v);
                return false;
            }
            if visible(v.xmin, snap) {
                found = Some(v);
                return false;
            }
            true
        });
        match found {
            Some(v) => (Some(v), true),
            None => (None, false),
        }
    }

    pub fn has_newer_committed(
        &self,
        key: &[u8],
        exclude_xid: u64,
        snap: &Snapshot,
        clog: impl Fn(u64) -> TxStatus,
    ) -> bool {
        let mut hit = false;
        self.walk_key_versions(key, |v| {
            if v.xmin == exclude_xid {
                return true;
            }
            if clog(v.xmin) != TxStatus::Committed {
                return true;
            }
            hit = v.xmin >= snap.xmax || snap.contains(v.xmin);
            false
        });
        hit
    }

    pub fn for_each_key<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], Vec<IndexVersion>),
    {
        self.with_index(|idx| {
            idx.for_each_key(|key, chain| {
                let out: Vec<IndexVersion> = chain
                    .into_iter()
                    .map(|v| IndexVersion {
                        offset: v.offset,
                        value_len: v.value_len,
                        xmin: v.xmin,
                        tombstone: v.tombstone,
                    })
                    .collect();
                f(key, out);
            });
        });
    }

    pub fn live_key_count(&self, clog: impl Fn(u64) -> TxStatus) -> i64 {
        use crate::hashindex::IndexExt;
        self.with_index(|idx| idx.live_key_count(clog))
    }

    pub fn set_enforce_limit(&self, enforce: bool) {
        self.with_index(|idx| {
            idx.set_enforce_limit(enforce);
        });
    }

    pub fn recalc_used_bytes(&self) {
        self.with_index(|idx| idx.recalc_used_bytes());
    }

    pub fn set_max_arena_bytes(&self, n: i64) {
        self.with_index(|idx| idx.set_max_arena_bytes(n));
    }

    pub fn index_hash_metrics(&self) -> IndexHashMetrics {
        use turnstone_hashindex::IndexStats;
        let mut out = IndexHashMetrics::default();
        self.with_index(|idx| {
            let stats: IndexStats = idx.stats();
            for st in stats.shards {
                if st.key_count > 0 {
                    out.shards_used += 1;
                }
                out.arena_bytes += st.arena_used;
                out.allocated_bytes += st.allocated_bytes;
                out.live_bytes += st.live_bytes;
            }
        });
        out
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexHashMetrics {
    pub shards_used: i32,
    pub arena_bytes: u64,
    pub allocated_bytes: u64,
    pub live_bytes: u64,
}

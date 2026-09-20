// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::sync::Arc;

use crate::index::Index;
use crate::shard::{live_bytes_for_map, Shard};
use crate::Version;

/// Per-shard arena usage report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardStats {
    pub shard_index: u32,
    pub slot_count: u32,
    pub key_count: u32,
    pub arena_used: u64,
    pub allocated_bytes: u64,
    pub live_bytes: u64,
}

/// Aggregated per-shard arena usage.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub shards: [ShardStats; crate::encoding::NUM_SHARDS],
}

impl Default for IndexStats {
    fn default() -> Self {
        Self {
            shards: [ShardStats::default(); crate::encoding::NUM_SHARDS],
        }
    }
}

/// Returns the version chain to retain for a key (newest first).
pub type VersionFilter<'a> = dyn Fn(&[u8], &[Version]) -> Vec<Version> + 'a;

struct RestoreEnforce(Arc<crate::index::IndexInner>);

impl Drop for RestoreEnforce {
    fn drop(&mut self) {
        self.0.set_enforce_limit(true);
    }
}

impl Index {
    pub fn stats(&self) -> IndexStats {
        let mut out = IndexStats::default();
        for (i, seg) in self.shards.iter().enumerate() {
            out.shards[i] = seg.stats(i as u32);
        }
        out
    }

    pub fn filtered_live_bytes(
        &self,
        shard_index: i32,
        filter: Option<&VersionFilter<'_>>,
    ) -> (u64, u64, u32) {
        if shard_index < 0 || shard_index as usize >= crate::encoding::NUM_SHARDS {
            return (0, 0, 0);
        }
        self.shards[shard_index as usize].filtered_live_bytes(filter)
    }

    pub fn compact_shard(
        &self,
        shard_index: i32,
        filter: Option<&VersionFilter<'_>>,
    ) -> Result<ShardStats, Box<dyn std::error::Error + Send + Sync>> {
        if shard_index < 0 || shard_index as usize >= crate::encoding::NUM_SHARDS {
            return Err(format!("hashindex: invalid shard index {shard_index}").into());
        }
        let seg = &self.shards[shard_index as usize];
        seg.compact(filter)?;
        Ok(seg.stats(shard_index as u32))
    }

    pub fn compact_all(
        &self,
        filter: Option<&VersionFilter<'_>>,
    ) -> Result<IndexStats, Box<dyn std::error::Error + Send + Sync>> {
        let mut out = IndexStats::default();
        for (i, seg) in self.shards.iter().enumerate() {
            seg.compact(filter)?;
            out.shards[i] = seg.stats(i as u32);
        }
        Ok(out)
    }
}

pub(crate) fn compact_shard(
    shard: &Shard,
    filter: Option<&VersionFilter<'_>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    shard.parent().set_enforce_limit(false);
    let _restore = RestoreEnforce(Arc::clone(shard.parent()));

    let mut st = shard.state.write();
    if st.closed {
        return Err("hashindex: shard is closed".into());
    }

    let arena_before = live_bytes_for_map(&st.keys);
    let mut next: HashMap<Vec<u8>, Vec<Version>> = HashMap::new();
    for (key, chain) in st.keys.drain() {
        let versions = if let Some(f) = filter {
            f(&key, &chain)
        } else {
            chain
        };
        if !versions.is_empty() {
            next.insert(key, versions);
        }
    }

    let before_alloc = crate::encoding::HEADER_SIZE as i64 + arena_before as i64;
    st.keys = next;
    let arena_after = live_bytes_for_map(&st.keys);
    let after_alloc = crate::encoding::HEADER_SIZE as i64 + arena_after as i64;
    drop(st);

    let delta = after_alloc - before_alloc;
    if delta != 0 {
        shard
            .parent()
            .account_delta(delta)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
    }

    Ok(())
}

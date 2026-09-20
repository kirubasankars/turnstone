// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::Arc;

use crate::arena::new_shard_buffer_locked;
use crate::encoding::{
    read_u32, read_u64, write_u64, write_u32, write_version, HEADER_SIZE, INITIAL_SLOTS,
    VERSION_NODE_SZ, VERSION_SIZE, HDR_TABLE_OFF_OFF,
};
use crate::index::Index;
use crate::shard::Shard;
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

pub(crate) struct CompactResult {
    pub arena_before: u64,
    pub arena_after: u64,
    pub keys_before: u32,
    pub keys_after: u32,
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
        let res = seg.compact(filter)?;
        Ok(ShardStats {
            shard_index: shard_index as u32,
            slot_count: seg
                .with_shard_buffer(|b| Shard::slot_count_on_data(b.as_slice()))
                .unwrap_or(0),
            key_count: seg
                .with_shard_buffer(|b| Shard::key_count_on_data(b.as_slice()))
                .unwrap_or(0),
            arena_used: res.arena_after,
            allocated_bytes: seg.buffer_len() as u64,
            live_bytes: res.arena_after,
        })
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

impl Shard {
    pub fn filtered_live_bytes(&self, filter: Option<&VersionFilter<'_>>) -> (u64, u64, u32) {
        let st = self.state.read();
        let Some(ref buf) = st.buf else {
            return (0, 0, 0);
        };
        if buf.is_closed() {
            return (0, 0, 0);
        }
        let data = buf.as_slice();
        let arena_used = Shard::arena_used_on_data(data);
        let key_count = Shard::key_count_on_data(data);
        let live_bytes = if key_count > 0 {
            self.live_bytes_locked(data, filter)
        } else {
            0
        };
        (arena_used, live_bytes, key_count)
    }
}

pub(crate) fn compact_shard(
    shard: &Shard,
    filter: Option<&VersionFilter<'_>>,
) -> Result<CompactResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut st = shard.state.write();
    let buf = st.buf.as_mut().ok_or("hashindex: shard is closed")?;
    if buf.is_closed() {
        return Err("hashindex: shard is closed".into());
    }

    shard.parent().set_enforce_limit(false);
    let _restore = RestoreEnforce(Arc::clone(shard.parent()));

    let res_before = CompactResult {
        arena_before: Shard::arena_used_on_data(buf.as_slice()),
        keys_before: Shard::key_count_on_data(buf.as_slice()),
        arena_after: 0,
        keys_after: 0,
    };

    struct KeyEntry {
        key: Vec<u8>,
        versions: Vec<Version>,
    }

    let entries: Vec<KeyEntry> = {
        let data = buf.as_slice();
        let slots = Shard::slot_count_on_data(data);
        let table = read_u64(data, HDR_TABLE_OFF_OFF) as usize;
        let mut entries = Vec::new();
        for slot in 0..slots {
            let rec_off = read_u64(data, table + slot as usize * 8);
            if rec_off == 0 {
                continue;
            }
            let k_len = read_u32(data, rec_off as usize) as usize;
            let key = data[rec_off as usize + 12..rec_off as usize + 12 + k_len].to_vec();
            let chain = shard.read_chain_locked(data, rec_off);
            let versions = if let Some(f) = filter {
                f(&key, &chain)
            } else {
                chain
            };
            if versions.is_empty() {
                continue;
            }
            entries.push(KeyEntry { key, versions });
        }
        entries
    };

    let mut slot_count = Shard::slot_count_on_data(buf.as_slice());
    if slot_count == 0 {
        slot_count = INITIAL_SLOTS;
    }
    let table_bytes = u64::from(slot_count) * 8;
    let mut live_bytes = 0u64;
    for e in &entries {
        live_bytes += (12 + e.key.len()) as u64 + (e.versions.len() as u64) * VERSION_NODE_SZ as u64;
    }
    let mut new_size =
        HEADER_SIZE as i64 + table_bytes as i64 + live_bytes as i64 + HEADER_SIZE as i64;
    let min_floor = HEADER_SIZE as i64 + table_bytes as i64 + HEADER_SIZE as i64;
    if new_size < min_floor {
        new_size = min_floor;
    }

    let lock = shard.parent().mlock;
    let mut new_buf = new_shard_buffer_locked(new_size, lock)?;
    Shard::init_header_on_buf(&mut new_buf, slot_count);
    let table_off = HEADER_SIZE as u64;
    let arena_off = table_off + table_bytes;

    for e in &entries {
        let rec_size = 12 + e.key.len();
        let rec_off = arena_off + Shard::arena_used_on_data(new_buf.as_slice());
        if rec_off as i64 + rec_size as i64 > new_buf.len() as i64 {
            new_buf.close();
            return Err("compact: buffer too small".into());
        }
        {
            let data = new_buf.as_mut_slice();
            write_u32(data, rec_off as usize, e.key.len() as u32);
            write_u64(data, rec_off as usize + 4, 0);
            data[rec_off as usize + 12..rec_off as usize + 12 + e.key.len()].copy_from_slice(&e.key);
            Shard::set_arena_used_on_data(data, Shard::arena_used_on_data(data) + rec_size as u64);
        }

        let mut head = 0u64;
        for i in (0..e.versions.len()).rev() {
            let node_off = arena_off + Shard::arena_used_on_data(new_buf.as_slice());
            if node_off as usize + VERSION_NODE_SZ > new_buf.len() {
                new_buf.close();
                return Err("compact: buffer too small".into());
            }
            {
                let data = new_buf.as_mut_slice();
                write_version(data, node_off as usize, &e.versions[i]);
                write_u64(data, node_off as usize + VERSION_SIZE, head);
                head = node_off;
                Shard::set_arena_used_on_data(
                    data,
                    Shard::arena_used_on_data(data) + VERSION_NODE_SZ as u64,
                );
            }
        }
        {
            let data = new_buf.as_mut_slice();
            write_u64(data, rec_off as usize + 4, head);
        }

        Shard::insert_key_slot_on_data(new_buf.as_mut_slice(), &e.key, rec_off)?;
    }

    drop(st);
    shard.replace_buffer(new_buf)?;
    let st = shard.state.read();
    let buf = st.buf.as_ref().unwrap();
    Ok(CompactResult {
        arena_before: res_before.arena_before,
        keys_before: res_before.keys_before,
        arena_after: Shard::arena_used_on_data(buf.as_slice()),
        keys_after: Shard::key_count_on_data(buf.as_slice()),
    })
}

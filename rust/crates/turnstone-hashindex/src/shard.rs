// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::encoding::{HEADER_SIZE, INITIAL_SLOTS, VERSION_NODE_SZ};
use crate::index::IndexInner;
use crate::Version;

pub(crate) struct ShardState {
    pub keys: HashMap<Vec<u8>, Vec<Version>>,
    pub(crate) closed: bool,
}

pub struct Shard {
    parent: Arc<IndexInner>,
    pub(crate) state: RwLock<ShardState>,
}

pub(crate) fn entry_bytes(key: &[u8], chain: &[Version]) -> i64 {
    (12 + key.len()) as i64 + (chain.len() as i64) * VERSION_NODE_SZ as i64
}

pub(crate) fn live_bytes_for_map(keys: &HashMap<Vec<u8>, Vec<Version>>) -> u64 {
    keys.iter().map(|(k, c)| entry_bytes(k, c) as u64).sum()
}

fn slot_count_for(key_count: u32) -> u32 {
    if key_count == 0 {
        return INITIAL_SLOTS;
    }
    let mut n = INITIAL_SLOTS;
    while n < key_count {
        n = n.saturating_mul(2);
    }
    n
}

impl Shard {
    pub fn new(parent: Arc<IndexInner>) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        parent
            .account_delta(HEADER_SIZE as i64)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        Ok(Self {
            parent,
            state: RwLock::new(ShardState {
                keys: HashMap::new(),
                closed: false,
            }),
        })
    }

    pub fn buffer_len(&self) -> i64 {
        self.allocated_len()
    }

    fn allocated_len(&self) -> i64 {
        let st = self.state.read();
        if st.closed {
            return 0;
        }
        HEADER_SIZE as i64 + live_bytes_for_map(&st.keys) as i64
    }

    pub fn close(&self) {
        let mut st = self.state.write();
        if st.closed {
            return;
        }
        let bytes = HEADER_SIZE as i64 + live_bytes_for_map(&st.keys) as i64;
        let _ = self.parent.account_delta(-bytes);
        st.keys.clear();
        st.closed = true;
    }

    fn is_closed(st: &ShardState) -> bool {
        st.closed
    }

    pub(crate) fn parent(&self) -> &Arc<IndexInner> {
        &self.parent
    }

    fn account_mutation<F, R>(&self, f: F) -> Result<R, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(&mut ShardState) -> Result<R, Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut st = self.state.write();
        if Self::is_closed(&st) {
            return Err("hashindex: shard is closed".into());
        }
        let before = HEADER_SIZE as i64 + live_bytes_for_map(&st.keys) as i64;
        let out = f(&mut st)?;
        let after = HEADER_SIZE as i64 + live_bytes_for_map(&st.keys) as i64;
        let delta = after - before;
        if delta != 0 {
            self.parent
                .account_delta(delta)
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
        }
        Ok(out)
    }

    pub fn put(
        &self,
        key: &[u8],
        ver: Version,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.account_mutation(|st| {
            st.keys.entry(key.to_vec()).or_default().insert(0, ver);
            Ok(())
        })
    }

    pub fn walk_versions<F>(&self, key: &[u8], f: &mut F)
    where
        F: FnMut(Version) -> bool,
    {
        let st = self.state.read();
        if st.closed {
            return;
        }
        let Some(chain) = st.keys.get(key) else {
            return;
        };
        for &v in chain {
            if !f(v) {
                break;
            }
        }
    }

    pub fn for_each_key<F>(&self, f: &mut F)
    where
        F: FnMut(&[u8], &[Version]),
    {
        let st = self.state.read();
        if st.closed {
            return;
        }
        for (key, chain) in &st.keys {
            if !chain.is_empty() {
                f(key, chain);
            }
        }
    }

    pub fn drop_xid(&self, xid: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.account_mutation(|st| {
            st.keys.retain(|_, chain| {
                chain.retain(|v| v.xmin != xid);
                !chain.is_empty()
            });
            Ok(())
        })
    }

    pub(crate) fn stats(&self, shard_index: u32) -> crate::compact::ShardStats {
        let st = self.state.read();
        if st.closed {
            return crate::compact::ShardStats {
                shard_index,
                ..Default::default()
            };
        }
        let key_count = st.keys.len() as u32;
        let live = live_bytes_for_map(&st.keys);
        crate::compact::ShardStats {
            shard_index,
            slot_count: slot_count_for(key_count),
            key_count,
            arena_used: live,
            allocated_bytes: HEADER_SIZE as u64 + live,
            live_bytes: live,
        }
    }

    pub(crate) fn live_bytes_locked(
        &self,
        filter: Option<&crate::compact::VersionFilter<'_>>,
    ) -> u64 {
        let st = self.state.read();
        if st.closed {
            return 0;
        }
        let mut live = 0u64;
        for (key, chain) in &st.keys {
            let versions = if let Some(f) = filter {
                f(key, chain)
            } else {
                chain.clone()
            };
            if versions.is_empty() {
                continue;
            }
            live += entry_bytes(key, &versions) as u64;
        }
        live
    }

    pub fn filtered_live_bytes(
        &self,
        filter: Option<&crate::compact::VersionFilter<'_>>,
    ) -> (u64, u64, u32) {
        let st = self.state.read();
        if st.closed {
            return (0, 0, 0);
        }
        let key_count = st.keys.len() as u32;
        let arena_used = live_bytes_for_map(&st.keys);
        let live_bytes = if key_count > 0 {
            self.live_bytes_locked(filter)
        } else {
            0
        };
        (arena_used, live_bytes, key_count)
    }

    pub(crate) fn compact(
        &self,
        filter: Option<&crate::compact::VersionFilter<'_>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        crate::compact::compact_shard(self, filter)
    }
}

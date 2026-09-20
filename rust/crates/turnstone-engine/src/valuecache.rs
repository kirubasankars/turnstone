// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;

use parking_lot::Mutex;

const SHARDS: usize = 64;
const MAX_ENTRY: usize = 1 << 20;

struct Shard {
    items: HashMap<i64, Vec<u8>>,
    bytes: i64,
    max: i64,
}

pub struct ValueCache {
    shards: [Mutex<Shard>; SHARDS],
}

impl ValueCache {
    pub fn new(max_bytes: i64) -> Self {
        let per = (max_bytes.max(SHARDS as i64)) / SHARDS as i64;
        Self {
            shards: std::array::from_fn(|_| {
                Mutex::new(Shard {
                    items: HashMap::new(),
                    bytes: 0,
                    max: per,
                })
            }),
        }
    }

    pub fn get(&self, offset: i64) -> Option<Vec<u8>> {
        self.shards[(offset as u64 as usize) % SHARDS]
            .lock()
            .items
            .get(&offset)
            .cloned()
    }

    pub fn put(&self, offset: i64, val: &[u8]) {
        if val.is_empty() || val.len() > MAX_ENTRY {
            return;
        }
        let mut s = self.shards[(offset as u64 as usize) % SHARDS].lock();
        if let Some(old) = s.items.remove(&offset) {
            s.bytes -= old.len() as i64;
        }
        let need = val.len() as i64;
        while s.bytes + need > s.max && !s.items.is_empty() {
            if let Some(k) = s.items.keys().next().copied() {
                if let Some(v) = s.items.remove(&k) {
                    s.bytes -= v.len() as i64;
                }
            }
        }
        if s.bytes + need <= s.max {
            s.items.insert(offset, val.to_vec());
            s.bytes += need;
        }
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            let mut s = shard.lock();
            s.items.clear();
            s.bytes = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_put_and_eviction() {
        let cache = ValueCache::new(4096);
        cache.put(100, b"hello");
        assert_eq!(cache.get(100).as_deref(), Some(b"hello".as_ref()));
        cache.put(100, b"world");
        assert_eq!(cache.get(100).as_deref(), Some(b"world".as_ref()));
        cache.clear();
        assert!(cache.get(100).is_none());
    }

    #[test]
    fn ignores_oversized_entries() {
        let cache = ValueCache::new(1 << 20);
        let huge = vec![0u8; MAX_ENTRY + 1];
        cache.put(1, &huge);
        assert!(cache.get(1).is_none());
    }
}

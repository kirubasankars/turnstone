// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::budget::SharedBudget;
use crate::encoding::{hash_key, NUM_SHARDS};
use crate::errors::ErrArenaLimit;
use crate::shard::Shard;

pub(crate) struct IndexInner {
    pub max_arena_bytes: AtomicI64,
    pub used_bytes: AtomicI64,
    pub enforce_limit: AtomicI32,
    pub shared: Mutex<Option<Arc<SharedBudget>>>,
    pub mlock: bool,
}

impl IndexInner {
    pub fn set_enforce_limit(&self, enforce: bool) {
        if enforce {
            self.enforce_limit.store(1, Ordering::Release);
        } else {
            self.enforce_limit.store(0, Ordering::Release);
        }
    }

    pub fn account_delta(&self, delta: i64) -> Result<(), ErrArenaLimit> {
        if delta == 0 {
            return Ok(());
        }
        if delta < 0 {
            if let Some(ref b) = *self.shared.lock() {
                let _ = b.reserve(delta);
            }
            self.used_bytes.fetch_add(delta, Ordering::AcqRel);
            return Ok(());
        }
        if let Some(ref b) = *self.shared.lock() {
            b.reserve(delta)?;
        }
        if self.enforce_limit.load(Ordering::Acquire) == 0 {
            self.used_bytes.fetch_add(delta, Ordering::AcqRel);
            return Ok(());
        }
        let limit = self.max_arena_bytes.load(Ordering::Acquire);
        if limit <= 0 {
            self.used_bytes.fetch_add(delta, Ordering::AcqRel);
            return Ok(());
        }
        loop {
            let used = self.used_bytes.load(Ordering::Acquire);
            let next = used + delta;
            if next > limit {
                if let Some(ref b) = *self.shared.lock() {
                    let _ = b.reserve(-delta);
                }
                return Err(ErrArenaLimit);
            }
            if self
                .used_bytes
                .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(());
            }
        }
    }
}

/// Sharded in-memory hash index with per-shard locking.
pub struct Index {
    pub(crate) inner: Arc<IndexInner>,
    pub(crate) shards: Box<[Arc<Shard>; NUM_SHARDS]>,
}

impl Index {
    /// Creates mmap-backed index shards (heap fallback on non-Unix).
    pub fn new() -> Self {
        Self::open(false).unwrap_or_else(|e| panic!("hashindex: {e}"))
    }

    /// Like [`Self::new`] with optional mlock of shard arenas.
    pub fn open(lock: bool) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let inner = Arc::new(IndexInner {
            max_arena_bytes: AtomicI64::new(0),
            used_bytes: AtomicI64::new(0),
            enforce_limit: AtomicI32::new(1),
            shared: Mutex::new(None),
            mlock: lock,
        });
        let mut built: Vec<Arc<Shard>> = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            match Shard::new(Arc::clone(&inner)) {
                Ok(s) => built.push(Arc::new(s)),
                Err(e) => {
                    for seg in built.iter() {
                        seg.close();
                    }
                    return Err(e);
                }
            }
        }
        let shards: Box<[Arc<Shard>; NUM_SHARDS]> =
            built.try_into().map_err(|_| "shard count")?;
        let idx = Self { inner, shards };
        idx.recalc_used_bytes();
        Ok(idx)
    }

    pub fn close(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for seg in self.shards.iter() {
            seg.close();
        }
        Ok(())
    }

    fn shard_for(&self, key: &[u8]) -> &Arc<Shard> {
        &self.shards[(hash_key(key) & 255) as usize]
    }

    pub fn put(&self, key: &[u8], ver: crate::Version) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.shard_for(key).put(key, ver)
    }

    pub fn walk_versions<F>(&self, key: &[u8], mut f: F)
    where
        F: FnMut(crate::Version) -> bool,
    {
        self.shard_for(key).walk_versions(key, &mut f);
    }

    pub fn drop_xid(&self, xid: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.set_enforce_limit(false);
        let result = self.drop_xid_shards(xid);
        self.set_enforce_limit(true);
        result
    }

    fn drop_xid_shards(&self, xid: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for seg in self.shards.iter() {
            seg.drop_xid(xid)?;
        }
        Ok(())
    }

    pub fn for_each_key<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[crate::Version]),
    {
        for seg in self.shards.iter() {
            seg.for_each_key(&mut f);
        }
    }

    pub fn set_max_arena_bytes(&self, n: i64) {
        self.inner
            .max_arena_bytes
            .store(n, Ordering::Release);
    }

    pub fn set_shared_budget(&self, b: Option<Arc<SharedBudget>>) -> Result<(), ErrArenaLimit> {
        *self.inner.shared.lock() = b.clone();
        if let Some(ref budget) = b {
            budget.reserve(self.inner.used_bytes.load(Ordering::Acquire))?;
        }
        Ok(())
    }

    pub fn max_arena_bytes(&self) -> i64 {
        self.inner.max_arena_bytes.load(Ordering::Acquire)
    }

    pub fn set_enforce_limit(&self, enforce: bool) {
        self.inner.set_enforce_limit(enforce);
    }

    pub fn used_bytes(&self) -> i64 {
        self.inner.used_bytes.load(Ordering::Acquire)
    }

    pub fn recalc_used_bytes(&self) {
        let mut total = 0i64;
        for s in self.shards.iter() {
            total += s.buffer_len();
        }
        self.inner.used_bytes.store(total, Ordering::Release);
    }

    pub fn shard_count(&self) -> usize {
        NUM_SHARDS
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

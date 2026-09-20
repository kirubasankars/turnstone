// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

/// Shard backing store: heap-allocated bytes (per-shard lock protects access).
pub(crate) struct ShardBuffer {
    pub data: Option<Vec<u8>>,
    pub locked: bool,
}

impl ShardBuffer {
    pub fn is_closed(&self) -> bool {
        self.data.is_none()
    }

    pub fn len(&self) -> usize {
        self.data.as_ref().map(|v| v.len()).unwrap_or(0)
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_deref().unwrap_or(&[])
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.data.as_deref_mut().unwrap_or(&mut [])
    }

    pub fn close(&mut self) {
        release_shard_buffer(self);
        self.data = None;
        self.locked = false;
    }
}

pub(crate) fn new_shard_buffer_locked(
    size: i64,
    lock: bool,
) -> Result<ShardBuffer, Box<dyn std::error::Error + Send + Sync>> {
    crate::arena_heap::heap_new_shard_buffer(size, lock)
}

pub(crate) fn grow_shard_buffer(
    b: &mut ShardBuffer,
    min_size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    crate::arena_heap::heap_grow_shard_buffer(b, min_size)
}

pub(crate) fn release_shard_buffer(b: &mut ShardBuffer) {
    crate::arena_heap::heap_release_shard_buffer(b);
}

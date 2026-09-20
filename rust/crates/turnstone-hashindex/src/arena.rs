// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

pub(crate) struct MmapBacking {
    pub ptr: *mut u8,
    pub len: usize,
}

// Mmap-backed shard bytes are only accessed under per-shard locks.
unsafe impl Send for MmapBacking {}
unsafe impl Sync for MmapBacking {}

/// Shard backing store: anonymous mmap on Unix, heap fallback elsewhere.
pub(crate) struct ShardBuffer {
    pub data: Option<Vec<u8>>,
    pub mmap: Option<MmapBacking>,
    pub locked: bool,
}

unsafe impl Send for ShardBuffer {}
unsafe impl Sync for ShardBuffer {}

impl ShardBuffer {
    pub fn is_closed(&self) -> bool {
        self.data.is_none() && self.mmap.is_none()
    }

    pub fn len(&self) -> usize {
        if let Some(ref v) = self.data {
            return v.len();
        }
        if let Some(ref m) = self.mmap {
            return m.len;
        }
        0
    }

    pub fn as_slice(&self) -> &[u8] {
        if let Some(ref v) = self.data {
            return v;
        }
        if let Some(ref m) = self.mmap {
            return unsafe { std::slice::from_raw_parts(m.ptr, m.len) };
        }
        &[]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        if let Some(ref mut v) = self.data {
            return v;
        }
        if let Some(ref m) = self.mmap {
            return unsafe { std::slice::from_raw_parts_mut(m.ptr, m.len) };
        }
        &mut []
    }

    pub fn close(&mut self) {
        release_shard_buffer(self);
        self.data = None;
        self.mmap = None;
        self.locked = false;
    }
}

#[cfg(unix)]
pub(crate) fn align_shard_buffer_size(n: i64) -> i64 {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as i64;
    if n <= 0 {
        return page;
    }
    (n + page - 1) & !(page - 1)
}

pub(crate) fn new_shard_buffer_locked(
    size: i64,
    lock: bool,
) -> Result<ShardBuffer, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    {
        crate::arena_mmap::mmap_new_shard_buffer(size, lock)
    }
    #[cfg(not(unix))]
    {
        crate::arena_heap::heap_new_shard_buffer(size, lock)
    }
}

pub(crate) fn grow_shard_buffer(
    b: &mut ShardBuffer,
    min_size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(unix)]
    {
        if b.mmap.is_some() || !b.is_closed() && b.data.is_none() {
            return crate::arena_mmap::mmap_grow_shard_buffer(b, min_size);
        }
    }
    crate::arena_heap::heap_grow_shard_buffer(b, min_size)
}

pub(crate) fn release_shard_buffer(b: &mut ShardBuffer) {
    #[cfg(unix)]
    {
        if b.mmap.is_some() {
            crate::arena_mmap::mmap_release_shard_buffer(b);
            return;
        }
    }
    crate::arena_heap::heap_release_shard_buffer(b);
}

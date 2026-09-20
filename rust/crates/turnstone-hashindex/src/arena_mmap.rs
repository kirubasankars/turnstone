// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::ptr;

use turnstone_mlock as mlock;

use crate::arena::{self, MmapBacking, ShardBuffer};

fn align_shard_buffer_size(n: i64) -> i64 {
    arena::align_shard_buffer_size(n)
}

pub(crate) fn mmap_new_shard_buffer(
    size: i64,
    lock: bool,
) -> Result<ShardBuffer, Box<dyn std::error::Error + Send + Sync>> {
    let mapped_size = align_shard_buffer_size(size) as usize;
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            mapped_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANON,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(format!("mmap shard buffer: {}", std::io::Error::last_os_error()).into());
    }
    let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, mapped_size) };
    if lock {
        if let Err(e) = mlock::lock(slice) {
            unsafe {
                libc::munmap(ptr, mapped_size);
            }
            return Err(format!("mlock shard buffer: {e}").into());
        }
    }
    Ok(ShardBuffer {
        data: None,
        mmap: Some(MmapBacking {
            ptr: ptr as *mut u8,
            len: mapped_size,
        }),
        locked: lock,
    })
}

pub(crate) fn mmap_grow_shard_buffer(
    b: &mut ShardBuffer,
    min_size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if b.is_closed() {
        return Err("hashindex: buffer is closed".into());
    }
    if min_size <= b.len() as i64 {
        return Ok(());
    }
    let mut n = b.len();
    if n == 0 {
        n = min_size as usize;
    }
    while (n as i64) < min_size {
        n *= 2;
    }
    let mut next = mmap_new_shard_buffer(n as i64, b.locked)?;
    next.as_mut_slice()[..b.len()].copy_from_slice(b.as_slice());
    mmap_release_shard_buffer(b);
    b.data = next.data.take();
    b.mmap = next.mmap.take();
    b.locked = next.locked;
    Ok(())
}

pub(crate) fn mmap_release_shard_buffer(b: &mut ShardBuffer) {
    let Some(ref mmap) = b.mmap else {
        return;
    };
    if b.locked {
        let slice = unsafe { std::slice::from_raw_parts(mmap.ptr, mmap.len) };
        mlock::unlock(slice);
        b.locked = false;
    }
    unsafe {
        libc::munmap(mmap.ptr as *mut _, mmap.len);
    }
    b.mmap = None;
    b.data = None;
}

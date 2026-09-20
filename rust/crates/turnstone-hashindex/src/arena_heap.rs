// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_mlock as mlock;

use crate::arena::ShardBuffer;

#[cfg_attr(unix, allow(dead_code))]
pub(crate) fn heap_new_shard_buffer(
    size: i64,
    lock: bool,
) -> Result<ShardBuffer, Box<dyn std::error::Error + Send + Sync>> {
    if size <= 0 {
        return Err(format!("hashindex: invalid buffer size {size}").into());
    }
    let data = vec![0u8; size as usize];
    if lock {
        mlock::lock(&data).map_err(|e| format!("mlock shard buffer: {e}"))?;
    }
    Ok(ShardBuffer {
        data: Some(data),
        mmap: None,
        locked: lock,
    })
}

pub(crate) fn heap_grow_shard_buffer(
    b: &mut ShardBuffer,
    min_size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if b.is_closed() {
        return Err("hashindex: buffer is closed".into());
    }
    let cur = b.len() as i64;
    if min_size <= cur {
        return Ok(());
    }
    let mut n = b.len();
    if n == 0 {
        n = min_size as usize;
    }
    while (n as i64) < min_size {
        n *= 2;
    }
    let mut out = vec![0u8; n];
    if let Some(ref old) = b.data {
        out[..old.len()].copy_from_slice(old);
    }
    if b.locked {
        mlock::lock(&out).map_err(|e| format!("mlock shard buffer: {e}"))?;
        if let Some(ref old) = b.data {
            mlock::unlock(old);
        }
    }
    b.data = Some(out);
    Ok(())
}

pub(crate) fn heap_release_shard_buffer(b: &mut ShardBuffer) {
    if let Some(ref data) = b.data {
        if b.locked {
            mlock::unlock(data);
            b.locked = false;
        }
    }
    b.data = None;
}

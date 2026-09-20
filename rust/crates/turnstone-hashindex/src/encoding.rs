// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use crate::Version;

pub const NUM_SHARDS: usize = 256;
pub(crate) const INITIAL_SLOTS: u32 = 1024;
pub(crate) const MAX_LOAD_FACTOR_NUM: u64 = 3;
pub(crate) const MAX_LOAD_FACTOR_DEN: u64 = 4;
pub const HEADER_SIZE: usize = 4096;
pub(crate) const VERSION_SIZE: usize = 21;
pub(crate) const VERSION_NODE_SZ: usize = VERSION_SIZE + 8;
pub(crate) const MAGIC: u64 = 0x5447_4853_48; // "TGHSH"
pub(crate) const FORMAT_VERSION: u32 = 1;

pub(crate) const HDR_MAGIC_OFF: usize = 0;
pub(crate) const HDR_VERSION_OFF: usize = 8;
pub(crate) const HDR_SLOT_COUNT_OFF: usize = 12;
pub(crate) const HDR_KEY_COUNT_OFF: usize = 16;
pub(crate) const HDR_TABLE_OFF_OFF: usize = 24;
pub(crate) const HDR_ARENA_OFF_OFF: usize = 32;
pub(crate) const HDR_ARENA_USED_OFF: usize = 40;

pub fn hash_key(key: &[u8]) -> u64 {
    const OFFSET64: u64 = 14695981039346656037;
    const PRIME64: u64 = 1099511628211;
    let mut h = OFFSET64;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME64);
    }
    h
}

pub(crate) fn write_version(buf: &mut [u8], off: usize, v: &Version) {
    buf[off..off + 8].copy_from_slice(&v.offset.to_be_bytes());
    buf[off + 8..off + 12].copy_from_slice(&v.value_len.to_be_bytes());
    buf[off + 12..off + 20].copy_from_slice(&v.xmin.to_be_bytes());
    buf[off + 20] = u8::from(v.tombstone);
}

pub(crate) fn read_version(buf: &[u8], off: usize) -> Version {
    Version {
        offset: i64::from_be_bytes(buf[off..off + 8].try_into().unwrap()),
        value_len: u32::from_be_bytes(buf[off + 8..off + 12].try_into().unwrap()),
        xmin: u64::from_be_bytes(buf[off + 12..off + 20].try_into().unwrap()),
        tombstone: buf[off + 20] == 1,
    }
}

pub(crate) fn read_u64(buf: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
}

pub(crate) fn write_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_be_bytes());
}

pub(crate) fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
}

pub(crate) fn write_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_be_bytes());
}

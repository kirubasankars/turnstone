// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

pub const NUM_SHARDS: usize = 256;
pub(crate) const INITIAL_SLOTS: u32 = 1024;
pub const HEADER_SIZE: usize = 4096;
pub(crate) const VERSION_SIZE: usize = 21;
pub(crate) const VERSION_NODE_SZ: usize = VERSION_SIZE + 8;

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

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Core WAL record types and encoding (compatible with the Go engine).

mod encode;
mod types;

pub use encode::*;
pub use types::*;

/// Castagnoli CRC32 checksum for WAL frames (matches Go `crc32.Checksum(..., Crc32Table)`).
pub fn castagnoli_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

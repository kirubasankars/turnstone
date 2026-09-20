// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! TurnstoneDB storage engine: WAL types, encoding, and segmented data log.

mod clog;
mod encode;
pub mod hashindex;
mod recovery;
mod types;
pub mod wal;

pub use clog::ClogState;
pub use encode::{
    decode_record, decode_value_at, encode_record, EncodeError,
};
pub use recovery::{replay_into_mem, replay_log};
pub use types::*;
pub use wal::{validate_frames, DataLog};

/// Castagnoli CRC32 checksum for WAL frames (matches Go `crc32.Checksum(..., Crc32Table)`).
pub fn castagnoli_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

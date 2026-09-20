// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! TurnstoneDB storage engine: WAL types, encoding, MVCC index, and `Db`.

mod clog;
mod committer;
mod db;
mod index_gc;
mod shared_buffers;
mod wal_maintenance;
#[cfg(unix)]
mod disk_unix;
mod encode;
pub mod hashindex;
mod index;
mod recovery;
mod types;
mod valuecache;
pub mod wal;

pub use clog::ClogState;
pub use db::{Db, Options, Transaction, TxnHandle, WalSegmentInfo, WalSegmentMetrics};
pub use index_gc::{IndexCompactResult, IndexGcContext, DEFAULT_INDEX_FRAGMENTATION_RATIO};
pub use shared_buffers::{SharedBuffers, DEFAULT_SHARED_BUFFERS_BYTES, SHARED_BUFFER_PAGE_SIZE};
pub use wal_maintenance::{WalRetentionResult, DEFAULT_WAL_COPY_FORWARD_RATIO};
pub use encode::{
    decode_record, decode_value_at, encode_record, EncodeError,
};
pub use index::IndexHashMetrics;
pub use recovery::{replay_into_mem, replay_log};
pub use types::*;
pub use wal::{validate_frames, DataLog};

/// Castagnoli CRC32 checksum for WAL frames (matches Go `crc32.Checksum(..., Crc32Table)`).
pub fn castagnoli_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

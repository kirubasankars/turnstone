// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;

pub const DIR_MODE: u32 = 0o755;
pub const FILE_MODE: u32 = 0o644;

pub const LOG_RECORD_HEADER_SIZE: usize = 9;
pub const LOG_FRAME_HEADER_SIZE: usize = 8;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Begin = 1,
    Set = 2,
    Delete = 3,
    Commit = 4,
    Abort = 5,
}

impl TryFrom<u8> for RecordType {
    type Error = EngineError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Begin),
            2 => Ok(RecordType::Set),
            3 => Ok(RecordType::Delete),
            4 => Ok(RecordType::Commit),
            5 => Ok(RecordType::Abort),
            _ => Err(EngineError::CorruptData),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxStatus {
    InProgress = 0,
    Committed = 1,
    Aborted = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub ty: RecordType,
    pub xid: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// One MVCC version in the append-only log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexVersion {
    pub offset: i64,
    pub value_len: u32,
    pub xmin: u64,
    pub tombstone: bool,
}

/// On-disk byte range of one log frame (header + payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordSpan {
    pub offset: i64,
    pub length: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub xmax: u64,
    pub xip: HashMap<u64, bool>,
}

impl Snapshot {
    pub fn contains(&self, xid: u64) -> bool {
        self.xip.get(&xid).copied().unwrap_or(false)
    }
}

pub fn frame_size(payload_len: usize) -> i64 {
    (LOG_FRAME_HEADER_SIZE + payload_len) as i64
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EngineError {
    #[error("transaction is already finished")]
    TxnFinished,
    #[error("write conflict detected")]
    WriteConflict,
    #[error("key not found")]
    KeyNotFound,
    #[error("checksum mismatch")]
    Checksum,
    #[error("data corruption detected")]
    CorruptData,
    #[error("log truncated due to corruption")]
    Truncated,
    #[error("log unavailable for requested byte offset")]
    LogUnavailable,
    #[error("disk usage exceeds threshold")]
    DiskFull,
    #[error("index arena size exceeds limit")]
    IndexArenaLimit,
    #[error("database is closed")]
    DatabaseClosed,
    #[error("database is corrupt")]
    DatabaseCorrupt,
    #[error("invalid log offset")]
    InvalidLogOffset,
    #[error("{0}")]
    Other(String),
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::NotFound {
            EngineError::LogUnavailable
        } else {
            EngineError::Other(e.to_string())
        }
    }
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

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
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RecordType::Begin),
            2 => Ok(RecordType::Set),
            3 => Ok(RecordType::Delete),
            4 => Ok(RecordType::Commit),
            5 => Ok(RecordType::Abort),
            _ => Err(()),
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

pub fn frame_size(payload_len: usize) -> i64 {
    (LOG_FRAME_HEADER_SIZE + payload_len) as i64
}

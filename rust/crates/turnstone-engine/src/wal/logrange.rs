// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use crate::castagnoli_checksum;
use crate::decode_record;
use crate::types::{EngineError, Record, LOG_FRAME_HEADER_SIZE};
use crate::frame_size;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFrame {
    pub rec: Record,
    pub length: i64,
}

/// Ensures `data` is a concatenation of complete log frames.
pub fn validate_frames(data: &[u8]) -> Result<Vec<LogFrame>, EngineError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let mut frames = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        if pos + LOG_FRAME_HEADER_SIZE > data.len() {
            return Err(EngineError::CorruptData);
        }
        let length = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let checksum = u32::from_be_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        let total = frame_size(length) as usize;
        if pos + total > data.len() {
            return Err(EngineError::CorruptData);
        }
        let payload = &data[pos + LOG_FRAME_HEADER_SIZE..pos + LOG_FRAME_HEADER_SIZE + length];
        if castagnoli_checksum(payload) != checksum {
            return Err(EngineError::Checksum);
        }
        let rec = decode_record(payload)?;
        frames.push(LogFrame {
            rec,
            length: total as i64,
        });
        pos += total;
    }
    Ok(frames)
}

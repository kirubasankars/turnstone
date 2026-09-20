// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use crate::{Record, RecordType, LOG_RECORD_HEADER_SIZE};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EncodeError {
    #[error("data corruption detected")]
    CorruptData,
}

pub fn encode_record(rec: &Record) -> Vec<u8> {
    let body_len = match rec.ty {
        RecordType::Set => 4 + rec.key.len() + 4 + rec.value.len(),
        RecordType::Delete => 4 + rec.key.len(),
        RecordType::Begin | RecordType::Commit | RecordType::Abort => 0,
    };
    let mut buf = vec![0u8; LOG_RECORD_HEADER_SIZE + body_len];
    buf[0] = rec.ty as u8;
    buf[1..9].copy_from_slice(&rec.xid.to_be_bytes());

    match rec.ty {
        RecordType::Set => {
            let mut off = LOG_RECORD_HEADER_SIZE;
            buf[off..off + 4].copy_from_slice(&(rec.key.len() as u32).to_be_bytes());
            off += 4;
            buf[off..off + rec.key.len()].copy_from_slice(&rec.key);
            off += rec.key.len();
            buf[off..off + 4].copy_from_slice(&(rec.value.len() as u32).to_be_bytes());
            off += 4;
            buf[off..off + rec.value.len()].copy_from_slice(&rec.value);
        }
        RecordType::Delete => {
            let mut off = LOG_RECORD_HEADER_SIZE;
            buf[off..off + 4].copy_from_slice(&(rec.key.len() as u32).to_be_bytes());
            off += 4;
            buf[off..off + rec.key.len()].copy_from_slice(&rec.key);
        }
        RecordType::Begin | RecordType::Commit | RecordType::Abort => {}
    }
    buf
}

pub fn decode_record(payload: &[u8]) -> Result<Record, EncodeError> {
    if payload.len() < LOG_RECORD_HEADER_SIZE {
        return Err(EncodeError::CorruptData);
    }
    let ty = RecordType::try_from(payload[0]).map_err(|_| EncodeError::CorruptData)?;
    let xid = u64::from_be_bytes(payload[1..9].try_into().unwrap());
    let body = &payload[LOG_RECORD_HEADER_SIZE..];
    let mut rec = Record {
        ty,
        xid,
        key: Vec::new(),
        value: Vec::new(),
    };
    match ty {
        RecordType::Set => {
            if body.len() < 4 {
                return Err(EncodeError::CorruptData);
            }
            let klen = u32::from_be_bytes(body[0..4].try_into().unwrap()) as usize;
            let mut off = 4;
            if off + klen + 4 > body.len() {
                return Err(EncodeError::CorruptData);
            }
            rec.key = body[off..off + klen].to_vec();
            off += klen;
            let vlen = u32::from_be_bytes(body[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            if off + vlen > body.len() {
                return Err(EncodeError::CorruptData);
            }
            rec.value = body[off..off + vlen].to_vec();
        }
        RecordType::Delete => {
            if body.len() < 4 {
                return Err(EncodeError::CorruptData);
            }
            let klen = u32::from_be_bytes(body[0..4].try_into().unwrap()) as usize;
            let off = 4;
            if off + klen > body.len() {
                return Err(EncodeError::CorruptData);
            }
            rec.key = body[off..off + klen].to_vec();
        }
        RecordType::Begin | RecordType::Commit | RecordType::Abort => {}
    }
    Ok(rec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_set_record() {
        let rec = Record {
            ty: RecordType::Set,
            xid: 42,
            key: b"k".to_vec(),
            value: b"value".to_vec(),
        };
        let encoded = encode_record(&rec);
        let decoded = decode_record(&encoded).unwrap();
        assert_eq!(rec, decoded);
    }
}

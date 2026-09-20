// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io::{Cursor, Read};

/// Sentinel length in MGET responses when a key is missing.
pub const MGET_NOT_FOUND: u32 = 0xFFFF_FFFF;

#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error("malformed payload")]
    Malformed,
    #[error("invalid key: must be ASCII")]
    InvalidKey,
}

pub fn encode_set(key: &str, value: &[u8]) -> Result<Vec<u8>, PayloadError> {
    if !crate::is_ascii(key) {
        return Err(PayloadError::InvalidKey);
    }
    let k = key.as_bytes();
    let mut out = Vec::with_capacity(4 + k.len() + value.len());
    out.extend_from_slice(&(k.len() as u32).to_be_bytes());
    out.extend_from_slice(k);
    out.extend_from_slice(value);
    Ok(out)
}

pub fn encode_promote(min_replicas: u32) -> Vec<u8> {
    min_replicas.to_be_bytes().to_vec()
}

pub fn encode_replica_of(source_addr: &str, source_db: &str) -> Vec<u8> {
    let addr = source_addr.as_bytes();
    let db = source_db.as_bytes();
    let mut out = Vec::with_capacity(4 + addr.len() + db.len());
    out.extend_from_slice(&(addr.len() as u32).to_be_bytes());
    out.extend_from_slice(addr);
    out.extend_from_slice(db);
    out
}

pub fn encode_mget_keys(keys: &[&str]) -> Result<Vec<u8>, PayloadError> {
    for k in keys {
        if !crate::is_ascii(k) {
            return Err(PayloadError::InvalidKey);
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(keys.len() as u32).to_be_bytes());
    for k in keys {
        let kb = k.as_bytes();
        out.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        out.extend_from_slice(kb);
    }
    Ok(out)
}

pub fn decode_mget_response(
    payload: &[u8],
    expected_keys: usize,
) -> Result<Vec<Option<Vec<u8>>>, PayloadError> {
    if payload.len() < 4 {
        return Err(PayloadError::Malformed);
    }
    let count = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as usize;
    if count != expected_keys {
        return Err(PayloadError::Malformed);
    }
    let mut results = Vec::with_capacity(count);
    let mut offset = 4usize;
    for _ in 0..count {
        if offset + 4 > payload.len() {
            return Err(PayloadError::Malformed);
        }
        let val_len = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if val_len == MGET_NOT_FOUND {
            results.push(None);
            continue;
        }
        let len = val_len as usize;
        if offset + len > payload.len() {
            return Err(PayloadError::Malformed);
        }
        results.push(Some(payload[offset..offset + len].to_vec()));
        offset += len;
    }
    Ok(results)
}

pub fn encode_mset(entries: &[(&str, &[u8])]) -> Result<Vec<u8>, PayloadError> {
    for (k, _) in entries {
        if !crate::is_ascii(k) {
            return Err(PayloadError::InvalidKey);
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for (k, v) in entries {
        let kb = k.as_bytes();
        out.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        out.extend_from_slice(kb);
        out.extend_from_slice(&(v.len() as u32).to_be_bytes());
        out.extend_from_slice(v);
    }
    Ok(out)
}

pub fn encode_mdel_keys(keys: &[&str]) -> Result<Vec<u8>, PayloadError> {
    encode_mget_keys(keys)
}

pub fn decode_mdel_count(payload: &[u8]) -> Result<u32, PayloadError> {
    if payload.len() < 4 {
        return Err(PayloadError::Malformed);
    }
    Ok(u32::from_be_bytes(payload[0..4].try_into().unwrap()))
}

/// Best-effort reader for count-prefixed string lists (used in tests and fuzzing).
pub fn read_u32_be(cursor: &mut Cursor<&[u8]>) -> Result<u32, PayloadError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| PayloadError::Malformed)?;
    Ok(u32::from_be_bytes(buf))
}

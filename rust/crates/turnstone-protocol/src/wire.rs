// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use crate::PROTO_HEADER_SIZE;

pub fn append_header(buf: &mut Vec<u8>, op: u8, payload_len: usize) {
    buf.push(op);
    buf.extend_from_slice(&(payload_len as u32).to_be_bytes());
}

pub fn encode_frame(op: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PROTO_HEADER_SIZE + payload.len());
    append_header(&mut buf, op, payload.len());
    buf.extend_from_slice(payload);
    buf
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    pub status_or_op: u8,
    pub payload_len: u32,
}

pub fn decode_header(header: &[u8]) -> Option<FrameHeader> {
    if header.len() != PROTO_HEADER_SIZE {
        return None;
    }
    Some(FrameHeader {
        status_or_op: header[0],
        payload_len: u32::from_be_bytes(header[1..5].try_into().ok()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OP_SET;

    #[test]
    fn encode_frame_matches_go_layout() {
        let payload = b"mykey";
        let frame = encode_frame(OP_SET, payload);
        assert_eq!(frame[0], OP_SET);
        assert_eq!(u32::from_be_bytes(frame[1..5].try_into().unwrap()), 5);
        assert_eq!(&frame[5..], payload);
    }
}

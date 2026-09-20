// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

pub const OP_PING: u8 = 0x01;
pub const OP_GET: u8 = 0x02;
pub const OP_SET: u8 = 0x03;
pub const OP_DEL: u8 = 0x04;
pub const OP_SELECT: u8 = 0x05;
pub const OP_MGET: u8 = 0x06;
pub const OP_MSET: u8 = 0x07;
pub const OP_MDEL: u8 = 0x08;
pub const OP_BEGIN: u8 = 0x10;
pub const OP_COMMIT: u8 = 0x11;
pub const OP_ABORT: u8 = 0x12;
pub const OP_STAT: u8 = 0x20;
pub const OP_REPLICA_OF: u8 = 0x32;
pub const OP_PROMOTE: u8 = 0x34;
pub const OP_STEP_DOWN: u8 = 0x35;
pub const OP_FLUSH_DB: u8 = 0x37;
pub const OP_REPL_HELLO: u8 = 0x50;
pub const OP_REPL_ACK: u8 = 0x52;
pub const OP_REPL_SAFE_POINT: u8 = 0x55;
pub const OP_REPL_LOG_RANGE: u8 = 0x57;
pub const OP_QUIT: u8 = 0xFF;

/// Begin payload flag: one byte of this value opens a read-only snapshot transaction.
pub const BEGIN_READ_ONLY: u8 = 0;

pub const RES_OK: u8 = 0x00;
pub const RES_ERR: u8 = 0x01;
pub const RES_NOT_FOUND: u8 = 0x02;
pub const RES_TX_REQUIRED: u8 = 0x03;
pub const RES_TX_TIMEOUT: u8 = 0x04;
pub const RES_TX_CONFLICT: u8 = 0x05;
pub const RES_TX_IN_PROGRESS: u8 = 0x06;
pub const RES_SERVER_BUSY: u8 = 0x07;
pub const RES_ENTITY_TOO_LARGE: u8 = 0x08;
pub const RES_MEMORY_LIMIT: u8 = 0x09;

pub fn status_name(status: u8) -> &'static str {
    match status {
        RES_OK => "OK",
        RES_ERR => "ERR",
        RES_NOT_FOUND => "NOT_FOUND",
        RES_TX_REQUIRED => "TX_REQUIRED",
        RES_TX_TIMEOUT => "TX_TIMEOUT",
        RES_TX_CONFLICT => "TX_CONFLICT",
        RES_TX_IN_PROGRESS => "TX_IN_PROGRESS",
        RES_SERVER_BUSY => "SERVER_BUSY",
        RES_ENTITY_TOO_LARGE => "ENTITY_TOO_LARGE",
        RES_MEMORY_LIMIT => "MEMORY_LIMIT",
        _ => "UNKNOWN",
    }
}

pub fn is_ascii(s: &str) -> bool {
    s.bytes().all(|b| b <= 127)
}

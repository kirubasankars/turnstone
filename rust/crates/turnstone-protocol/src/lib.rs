// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! TurnstoneDB wire protocol (compatible with the Go implementation).

mod limits;
mod opcode;
mod payload;
mod wire;

pub use limits::*;
pub use opcode::*;
pub use payload::{PayloadError, *};
pub use wire::{decode_header, encode_frame, FrameHeader};

/// Key missing on GET (matches Go `protocol.ErrKeyNotFound`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("key does not exist")]
pub struct KeyNotFound;

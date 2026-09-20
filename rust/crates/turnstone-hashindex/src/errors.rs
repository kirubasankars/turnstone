// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use thiserror::Error;

/// Returned when a shard buffer grow would exceed [`crate::Index::max_arena_bytes`].
#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("index arena size exceeds limit")]
pub struct ErrArenaLimit;

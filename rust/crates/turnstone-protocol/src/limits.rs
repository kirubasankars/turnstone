// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::time::Duration;

pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(3 * 60);
pub const MAX_TX_DURATION: Duration = Duration::from_secs(30);
pub const MAX_TX_SIZE: u64 = 200 * 1024 * 1024;
pub const MAX_VALUE_SIZE: u64 = 4 * 1024 * 1024;
pub const MAX_COMMAND_SIZE: u64 = 512 * 1024 * 1024;

pub const PROTO_HEADER_SIZE: usize = 5;

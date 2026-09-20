// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::io;
use std::path::Path;

use nix::sys::statfs::statfs;

pub fn get_disk_usage(path: &Path) -> io::Result<f64> {
    let stat = statfs(path).map_err(|e| io::Error::from_raw_os_error(e as i32))?;
    let total = stat.blocks() as u64 * stat.block_size() as u64;
    let free = stat.blocks_available() as u64 * stat.block_size() as u64;
    if total == 0 {
        return Ok(0.0);
    }
    Ok(((total - free) as f64 / total as f64) * 100.0)
}

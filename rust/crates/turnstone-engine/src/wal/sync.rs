// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;

use nix::unistd::fdatasync;

/// Durability-flush file data (fdatasync on Unix).
pub fn sync_file(f: &File) -> io::Result<()> {
    loop {
        match fdatasync(f.as_raw_fd()) {
            Ok(()) => return Ok(()),
            Err(nix::errno::Errno::EINTR) => continue,
            Err(e) => return Err(io::Error::from_raw_os_error(e as i32)),
        }
    }
}

/// Persist a rename into `dir` (fsync parent directory).
pub fn sync_dir(dir: &str) -> io::Result<()> {
    let d = File::open(dir)?;
    d.sync_all()
}

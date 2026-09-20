// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicPtr, Ordering};

use nix::errno::Errno;
use nix::fcntl::{fallocate, FallocateFlags};

static TESTING_PREALLOC_ERR: AtomicPtr<io::ErrorKind> = AtomicPtr::new(std::ptr::null_mut());

/// Test hook: force preallocate to fail with the given error kind.
#[cfg(test)]
pub fn set_testing_prealloc_err(kind: Option<io::ErrorKind>) {
    use std::sync::Mutex;
    static SLOT: Mutex<Option<io::ErrorKind>> = Mutex::new(None);
    *SLOT.lock().unwrap() = kind;
}

pub(crate) fn testing_prealloc_err() -> Option<io::Error> {
    #[cfg(test)]
    {
        use std::sync::Mutex;
        static SLOT: Mutex<Option<io::ErrorKind>> = Mutex::new(None);
        return SLOT
            .lock()
            .unwrap()
            .map(|k| io::Error::from(k));
    }
    #[cfg(not(test))]
    {
        let _ = TESTING_PREALLOC_ERR.load(Ordering::Relaxed);
        None
    }
}

pub fn is_no_space(err: &io::Error) -> bool {
    err.raw_os_error() == Some(Errno::ENOSPC as i32)
        || err.raw_os_error() == Some(Errno::EDQUOT as i32)
}

pub fn preallocate_file(f: &File, size: i64) -> io::Result<()> {
    if size <= 0 {
        return Ok(());
    }
    if let Some(e) = testing_prealloc_err() {
        return Err(e);
    }
    preallocate_file_os(f, size)
}

fn preallocate_file_os(f: &File, size: i64) -> io::Result<()> {
    match fallocate(
        f.as_raw_fd(),
        FallocateFlags::empty(),
        0,
        size as i64,
    ) {
        Ok(()) => Ok(()),
        Err(Errno::EOPNOTSUPP) | Err(Errno::ENOSYS) => {
            f.set_len(size as u64)?;
            Ok(())
        }
        Err(e) => Err(io::Error::from_raw_os_error(e as i32)),
    }
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//! Pin process memory so the kernel will not swap those pages (Unix).

mod platform;

use std::error::Error;
use std::fmt;

/// Returned when [`lock`] is requested on a platform without mlock support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErrUnsupported;

impl fmt::Display for ErrUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("mlock is not supported on this platform")
    }
}

impl Error for ErrUnsupported {}

/// Pins `b` in RAM so the kernel will not swap those pages.
pub fn lock(b: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
    platform::lock(b)
}

/// Releases a previous [`lock`]. Munmap also unlocks on Unix.
pub fn unlock(b: &[u8]) {
    platform::unlock(b);
}

pub fn supported() -> bool {
    platform::supported()
}

pub fn is_denied(err: &(dyn Error + 'static)) -> bool {
    platform::is_denied(err)
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs::File;
use std::io;

use memmap2::{Advice, Mmap, MmapOptions};

pub struct WalMapping {
    pub mmap: Mmap,
}

pub fn mmap_wal_file(f: &File, size: i64) -> io::Result<Option<WalMapping>> {
    if size <= 0 {
        return Ok(None);
    }
    let mmap = unsafe { MmapOptions::new().len(size as usize).map(f)? };
    let _ = mmap.advise(Advice::Random);
    Ok(Some(WalMapping { mmap }))
}

pub fn unmap_wal(_mapping: WalMapping) {}

pub fn advise_wal_range(mapping: &[u8], off: i64, n: i64, advice: Advice) {
    if mapping.is_empty() || n <= 0 {
        return;
    }
    let page = 4096i64;
    let mut start = off - off % page;
    if start < 0 {
        start = 0;
    }
    let mut end = off + n;
    end = (end + page - 1) & !(page - 1);
    if end > mapping.len() as i64 {
        end = mapping.len() as i64;
    }
    if start >= end {
        return;
    }
    // Best-effort whole-map advice when range is large; memmap2 has no range API.
    let _ = (start, end, advice);
}

pub fn wal_advise_sequential() -> Advice {
    Advice::Sequential
}

pub fn wal_advise_random() -> Advice {
    Advice::Random
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use super::alloc::{is_no_space, preallocate_file};
use super::sync::sync_file;
use crate::castagnoli_checksum;
use crate::types::FILE_MODE;

pub const WAL_SEG_FOOTER_MAGIC: u32 = 0x5453_4631; // "TSF1"
pub const WAL_SEG_FOOTER_SIZE: i64 = 16;
const WAL_RECYCLE_DIR_NAME: &str = "recycle";
pub const MAX_RECYCLED_SEGMENTS: usize = 8;

pub fn write_segment_footer(f: &File, segment_size: i64, used: i64) -> io::Result<()> {
    if segment_size < WAL_SEG_FOOTER_SIZE {
        return Ok(());
    }
    let mut buf = [0u8; WAL_SEG_FOOTER_SIZE as usize];
    buf[0..4].copy_from_slice(&WAL_SEG_FOOTER_MAGIC.to_be_bytes());
    buf[4..12].copy_from_slice(&(used as u64).to_be_bytes());
    let csum = castagnoli_checksum(&buf[..12]);
    buf[12..16].copy_from_slice(&csum.to_be_bytes());
    f.write_all_at(&buf, (segment_size - WAL_SEG_FOOTER_SIZE) as u64)
}

pub fn write_segment_footer_if_allocated(f: &File, segment_size: i64, used: i64) -> io::Result<()> {
    let meta = f.metadata()?;
    if meta.len() < segment_size as u64 {
        return Ok(());
    }
    write_segment_footer(f, segment_size, used)
}

pub fn read_segment_footer(f: &File, segment_size: i64) -> Option<i64> {
    if segment_size < WAL_SEG_FOOTER_SIZE {
        return None;
    }
    let mut buf = [0u8; WAL_SEG_FOOTER_SIZE as usize];
    f.read_exact_at(&mut buf, (segment_size - WAL_SEG_FOOTER_SIZE) as u64)
        .ok()?;
    if u32::from_be_bytes(buf[0..4].try_into().ok()?) != WAL_SEG_FOOTER_MAGIC {
        return None;
    }
    let expect = u32::from_be_bytes(buf[12..16].try_into().ok()?);
    if castagnoli_checksum(&buf[..12]) != expect {
        return None;
    }
    let used = u64::from_be_bytes(buf[4..12].try_into().ok()?) as i64;
    let max_used = segment_size - WAL_SEG_FOOTER_SIZE;
    if used < 0 || used > max_used {
        return None;
    }
    Some(used)
}

pub fn create_allocated_wal_file(path: &Path, segment_size: i64) -> io::Result<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(FILE_MODE)
        .open(path)?;
    if let Err(e) = preallocate_file(&f, segment_size) {
        if is_no_space(&e) {
            return Ok(f);
        }
        let _ = fs::remove_file(path);
        return Err(e);
    }
    if let Err(e) = write_segment_footer(&f, segment_size, 0) {
        if is_no_space(&e) {
            f.set_len(0)?;
            return Ok(f);
        }
        let _ = fs::remove_file(path);
        return Err(e);
    }
    if let Err(e) = sync_file(&f) {
        let _ = fs::remove_file(path);
        return Err(e);
    }
    Ok(f)
}

pub fn reset_allocated_wal_file(path: &Path, segment_size: i64) -> io::Result<()> {
    let f = OpenOptions::new().read(true).write(true).open(path)?;
    if let Err(e) = preallocate_file(&f, segment_size) {
        if is_no_space(&e) {
            return f.set_len(0);
        }
        return Err(e);
    }
    if let Err(e) = write_segment_footer(&f, segment_size, 0) {
        if is_no_space(&e) {
            return f.set_len(0);
        }
        return Err(e);
    }
    sync_file(&f)
}

pub(crate) fn recycle_dir(wal_dir: &Path) -> PathBuf {
    wal_dir.join(WAL_RECYCLE_DIR_NAME)
}

pub(crate) fn parse_recycle_seq(name: &str) -> Option<u32> {
    let rest = name.strip_prefix("r-")?;
    rest.parse().ok()
}

trait FileExt {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;
}

impl FileExt for File {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let mut off = 0;
        while off < buf.len() {
            let n = self.write_at(&buf[off..], offset + off as u64)?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            off += n;
        }
        Ok(())
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        let mut off = 0;
        while off < buf.len() {
            let n = self.read_at(&mut buf[off..], offset + off as u64)?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            off += n;
        }
        Ok(())
    }
}

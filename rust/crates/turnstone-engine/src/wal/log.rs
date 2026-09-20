// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::cell::RefCell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicI32, Ordering};
use std::sync::{Condvar, Mutex, RwLock};

use super::alloc::{is_no_space, preallocate_file};
use super::logrange::validate_frames;
use super::manifest::{
    create_fresh_wal_manifest, load_wal_manifest, normalize_wal_segment_size, save_wal_manifest,
    wal_segment_file_name, WalManifest, WalManifestSegment, WAL_DIR_NAME, WAL_MANIFEST_NAME,
};
use super::mmap::{advise_wal_range, mmap_wal_file, unmap_wal, wal_advise_random, wal_advise_sequential, WalMapping};
use super::recycle::{
    create_allocated_wal_file, parse_recycle_seq, read_segment_footer, reset_allocated_wal_file,
    recycle_dir, write_segment_footer, write_segment_footer_if_allocated, WAL_SEG_FOOTER_SIZE,
};
use super::sync::sync_file;
use std::sync::Arc;

use crate::castagnoli_checksum;
use crate::shared_buffers::{BufferTag, SharedBuffers, SHARED_BUFFER_PAGE_SIZE};
use crate::decode_record;
use crate::decode_value_into;
use crate::types::{
    EngineError, Record, RecordSpan, LOG_FRAME_HEADER_SIZE, FILE_MODE,
};
use crate::{encode_record, frame_size, RecordType};

const REPLAY_CANCEL_CHECK_INTERVAL: u64 = 1024;

thread_local! {
    static FRAME_SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::new());
}

/// Result of copying live WAL frames into a fresh segment.
pub struct CopyForwardOutcome {
    pub remap: std::collections::HashMap<i64, i64>,
    pub head_before: i64,
    pub bytes_before: i64,
    pub bytes_after: i64,
}

static TESTING_BEFORE_SYNC: Mutex<Option<Box<dyn Fn() + Send>>> = Mutex::new(None);

/// Test hook: runs after WAL insert lock release and before fdatasync.
pub fn set_testing_before_sync(hook: Option<Box<dyn Fn() + Send>>) {
    *TESTING_BEFORE_SYNC.lock().unwrap() = hook;
}

struct WaitGroup {
    count: Mutex<u32>,
    cvar: Condvar,
}

impl WaitGroup {
    fn new() -> Self {
        Self {
            count: Mutex::new(0),
            cvar: Condvar::new(),
        }
    }

    fn add(&self) {
        *self.count.lock().unwrap() += 1;
    }

    fn done(&self) {
        let mut c = self.count.lock().unwrap();
        *c = c.saturating_sub(1);
        self.cvar.notify_all();
    }

    fn wait(&self) {
        let mut c = self.count.lock().unwrap();
        while *c > 0 {
            c = self.cvar.wait(c).unwrap();
        }
    }
}

struct WalSegment {
    id: u32,
    file: String,
    path: String,
    base_lsn: i64,
    end_lsn: i64,
    writer: Option<File>,
    reader: Option<File>,
    mapping: Option<WalMapping>,
    allocated: bool,
}

struct Inner {
    segments: Vec<WalSegment>,
    active_index: usize,
    write_offset: i64,
    recycle: Vec<String>,
    recycle_seq: u32,
}

/// Segmented append-only WAL addressed by a global byte LSN.
pub struct DataLog {
    dir: PathBuf,
    wal_dir: PathBuf,
    manifest_path: PathBuf,
    segment_size: i64,
    inner: RwLock<Inner>,
    durable_offset: AtomicI64,
    inflight_syncs: WaitGroup,
    syncing: AtomicI32,
    buffers: Option<Arc<SharedBuffers>>,
}

impl DataLog {
    pub fn open(dir: impl AsRef<Path>, segment_size: i64) -> io::Result<Self> {
        Self::open_with_buffers(dir, segment_size, None)
    }

    pub fn open_with_buffers(
        dir: impl AsRef<Path>,
        segment_size: i64,
        buffers: Option<Arc<SharedBuffers>>,
    ) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let segment_size = normalize_wal_segment_size(segment_size);
        let wal_dir = dir.join(WAL_DIR_NAME);
        let manifest_path = wal_dir.join(WAL_MANIFEST_NAME);

        let manifest = match load_wal_manifest(&manifest_path) {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                create_fresh_wal_manifest(&wal_dir, segment_size)?
            }
            Err(e) => return Err(e),
        };

        let mut segment_size = manifest.segment_size;
        if segment_size <= 0 {
            segment_size = normalize_wal_segment_size(segment_size);
        }

        let log = Self {
            dir,
            wal_dir: wal_dir.clone(),
            manifest_path,
            segment_size,
            inner: RwLock::new(Inner {
                segments: Vec::new(),
                active_index: 0,
                write_offset: 0,
                recycle: Vec::new(),
                recycle_seq: 0,
            }),
            durable_offset: AtomicI64::new(0),
            inflight_syncs: WaitGroup::new(),
            syncing: AtomicI32::new(0),
            buffers,
        };
        {
            let mut inner = log.inner.write().unwrap();
            log.load_segments(&mut inner, &manifest)?;
            log.load_recycle_pool(&mut inner)?;
        }
        log.durable_offset
            .store(log.inner.read().unwrap().write_offset, Ordering::Release);
        Ok(log)
    }

    pub fn write_offset(&self) -> i64 {
        self.inner.read().unwrap().write_offset
    }

    pub fn durable_offset(&self) -> i64 {
        self.durable_offset.load(Ordering::Acquire)
    }

    pub fn segment_count(&self) -> usize {
        self.inner.read().unwrap().segments.len()
    }

    /// Retained WAL LSN span (write head minus oldest segment base).
    pub fn logical_size(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        if inner.segments.is_empty() {
            return 0;
        }
        let size = inner.write_offset - inner.segments[0].base_lsn;
        if size < 0 {
            0
        } else {
            size
        }
    }

    /// On-disk allocated bytes for all WAL segment files.
    pub fn allocated_size(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        let mut total = 0i64;
        for seg in &inner.segments {
            if let Ok(meta) = std::fs::metadata(&seg.path) {
                total += meta.len() as i64;
            }
        }
        total
    }

    /// Base LSN of the earliest retained segment.
    pub fn oldest_segment_base_lsn(&self) -> i64 {
        let inner = self.inner.read().unwrap();
        inner
            .segments
            .first()
            .map(|s| s.base_lsn)
            .unwrap_or(0)
    }

    pub fn segments(&self) -> Vec<(u32, i64, i64)> {
        let inner = self.inner.read().unwrap();
        inner
            .segments
            .iter()
            .map(|s| (s.id, s.base_lsn, s.end_lsn))
            .collect()
    }

    pub fn append_encoded(&self, payload: &[u8], sync: bool) -> Result<i64, EngineError> {
        let mut inner = self.inner.write().unwrap();
        let off = inner.write_offset;
        self.write_frame_locked(&mut inner, payload)?;
        let sync_handle = if sync {
            Some(self.begin_sync_locked(&inner)?)
        } else {
            self.publish_append_locked(&inner);
            None
        };
        drop(inner);
        if let Some((sync_f, flushed)) = sync_handle {
            self.complete_sync(sync_f, flushed);
        }
        Ok(off)
    }

    pub fn append_records<F>(&self, builders: &[F], sync: bool) -> Result<Vec<i64>, EngineError>
    where
        F: Fn() -> Vec<u8>,
    {
        if builders.is_empty() {
            return Ok(Vec::new());
        }
        let mut inner = self.inner.write().unwrap();
        let mut offsets = Vec::with_capacity(builders.len());
        for build in builders {
            let off = inner.write_offset;
            self.write_frame_locked(&mut inner, &build())?;
            offsets.push(off);
        }
        let sync_handle = if sync {
            Some(self.begin_sync_locked(&inner)?)
        } else {
            self.publish_append_locked(&inner);
            None
        };
        drop(inner);
        if let Some((sync_f, flushed)) = sync_handle {
            self.complete_sync(sync_f, flushed);
        }
        Ok(offsets)
    }

    pub fn append_encoded_batch(
        &self,
        payloads: &[Vec<u8>],
        sync: bool,
    ) -> Result<Vec<i64>, EngineError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        let mut inner = self.inner.write().unwrap();
        let mut offsets = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let off = inner.write_offset;
            self.write_frame_locked(&mut inner, payload)?;
            offsets.push(off);
        }
        let sync_handle = if sync {
            Some(self.begin_sync_locked(&inner)?)
        } else {
            self.publish_append_locked(&inner);
            None
        };
        drop(inner);
        if let Some((sync_f, flushed)) = sync_handle {
            self.complete_sync(sync_f, flushed);
        }
        Ok(offsets)
    }

    pub fn read_value_at(&self, offset: i64, val_len: u32) -> Result<Vec<u8>, EngineError> {
        let mut out = Vec::new();
        self.read_value_at_into(offset, val_len, &mut out)?;
        Ok(out)
    }

    /// Read a SET value into `out`, reusing capacity across calls on the same buffer.
    pub fn read_value_at_into(
        &self,
        offset: i64,
        val_len: u32,
        out: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        if self.buffers.is_some() {
            if self
                .read_value_via_buffers_into(offset, val_len, out)
                .is_ok()
            {
                return Ok(());
            }
        }
        let inner = self.inner.read().unwrap();
        inner.read_value_at_locked_into(offset, val_len, out)
    }

    fn read_value_via_buffers_into(
        &self,
        offset: i64,
        val_len: u32,
        out: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        let buffers = self.buffers.as_ref().ok_or(EngineError::LogUnavailable)?;
        let inner = self.inner.read().unwrap();
        let (seg, local) = inner
            .resolve_lsn(offset)
            .ok_or(EngineError::InvalidLogOffset)?;
        let mut pins = Vec::new();
        let header = gather_from_buffers(
            buffers,
            seg,
            local,
            LOG_FRAME_HEADER_SIZE as i64,
            &inner,
            &mut pins,
        )?;
        let payload_len = u32::from_be_bytes(header[0..4].try_into().unwrap());
        if payload_len > 1 << 30 {
            return Err(EngineError::CorruptData);
        }
        let checksum = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let payload = gather_from_buffers(
            buffers,
            seg,
            local + LOG_FRAME_HEADER_SIZE as i64,
            payload_len as i64,
            &inner,
            &mut pins,
        )?;
        for i in pins {
            buffers.unpin(i);
        }
        if castagnoli_checksum(&payload) != checksum {
            return Err(EngineError::Checksum);
        }
        decode_value_into(&payload, val_len, out)
    }

    /// On-disk frame bytes (header + payload) at global LSN.
    pub fn read_frame_bytes_at(&self, lsn: i64) -> Result<Vec<u8>, EngineError> {
        let inner = self.inner.read().unwrap();
        let seg_idx = inner.segment_index_for_lsn(lsn);
        if seg_idx < 0 {
            return Err(EngineError::InvalidLogOffset);
        }
        let seg = &inner.segments[seg_idx as usize];
        let local = lsn - seg.base_lsn;
        let limit = inner.segment_scan_limit(seg, file_size_of(&seg.path), self.segment_size);
        let (_valid_end, _rec, span, rerr) = inner.read_frame_at_seg(seg, local, limit);
        if rerr.is_err() {
            return Err(EngineError::InvalidLogOffset);
        }
        let mut frame = vec![0u8; span.length as usize];
        inner
            .read_segment_at(seg, local, &mut frame)
            .map_err(EngineError::from)?;
        Ok(frame)
    }

    pub fn append_copy_forward_frames(
        &self,
        old_offsets: &[i64],
        frames: &[Vec<u8>],
    ) -> Result<CopyForwardOutcome, EngineError> {
        if old_offsets.len() != frames.len() {
            return Err(EngineError::Other(
                "wal copy-forward: offset/frame count mismatch".into(),
            ));
        }
        let mut inner = self.inner.write().unwrap();
        let mut out = CopyForwardOutcome {
            remap: std::collections::HashMap::with_capacity(old_offsets.len()),
            head_before: inner.write_offset,
            bytes_before: 0,
            bytes_after: 0,
        };
        for seg in &inner.segments {
            if let Ok(meta) = fs::metadata(&seg.path) {
                out.bytes_before += meta.len() as i64;
            }
        }
        inner.rotate_segment_locked(self)?;
        for (i, frame) in frames.iter().enumerate() {
            let off = inner.write_offset;
            let seg_idx = inner.active_index;
            let local_off = off - inner.segments[seg_idx].base_lsn;
            inner.write_segment_at(seg_idx, local_off, frame)?;
            inner.write_offset += frame.len() as i64;
            out.remap.insert(old_offsets[i], off);
            if inner.write_offset - inner.segments[seg_idx].base_lsn
                >= inner.usable_segment_size(self.segment_size)
            {
                inner.rotate_segment_locked(self)?;
            }
        }
        for seg in &inner.segments {
            if let Ok(meta) = fs::metadata(&seg.path) {
                out.bytes_after += meta.len() as i64;
            }
        }
        self.persist_manifest_locked(&mut inner)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        Ok(out)
    }

    pub fn read_log_range(
        &self,
        start_offset: i64,
        max_bytes: i64,
    ) -> Result<(Vec<u8>, i64), EngineError> {
        if max_bytes <= 0 {
            return Ok((Vec::new(), start_offset));
        }
        let inner = self.inner.read().unwrap();
        let mut durable = self.durable_offset.load(Ordering::Acquire);
        if durable > inner.write_offset {
            durable = inner.write_offset;
        }
        if start_offset >= durable {
            return Ok((Vec::new(), start_offset));
        }
        let start_idx = inner.segment_index_for_lsn(start_offset);
        if start_idx < 0 {
            return Err(EngineError::LogUnavailable);
        }
        let mut out = Vec::new();
        let mut pos = start_offset;
        for i in start_idx as usize..inner.segments.len() {
            let seg = &inner.segments[i];
            let mut seg_end = seg.end_lsn;
            if seg_end == 0 {
                seg_end = inner.write_offset;
            }
            if seg_end > durable {
                seg_end = durable;
            }
            if pos >= seg_end {
                continue;
            }
            if !Path::new(&seg.path).exists() {
                return Err(EngineError::LogUnavailable);
            }
            let file_size =
                inner.segment_scan_limit(seg, file_size_of(&seg.path), self.segment_size);
            let mut local_pos = pos - seg.base_lsn;
            while local_pos < file_size && pos < seg_end {
                let (valid_end, _rec, span, rerr) =
                    inner.read_frame_at_seg(seg, local_pos, file_size);
                match rerr {
                    Ok(()) => {}
                    Err(e) if e == ErrorKind::UnexpectedEof => break,
                    Err(e) if e == ErrorKind::NotFound => return Err(EngineError::LogUnavailable),
                    Err(_) => return Err(EngineError::CorruptData),
                }
                let frame_len = span.length;
                if !out.is_empty() && out.len() as i64 + frame_len > max_bytes {
                    return Ok((out, pos));
                }
                let mut frame = vec![0u8; frame_len as usize];
                inner
                    .read_segment_at(seg, local_pos, &mut frame)
                    .map_err(|e| {
                        if e.kind() == ErrorKind::NotFound {
                            EngineError::LogUnavailable
                        } else {
                            EngineError::Other(e.to_string())
                        }
                    })?;
                out.extend_from_slice(&frame);
                local_pos = valid_end;
                pos = seg.base_lsn + local_pos;
            }
        }
        Ok((out, pos))
    }

    pub fn append_raw_frames(&self, data: &[u8], fsync: bool) -> Result<i64, EngineError> {
        let frames = validate_frames(data)?;
        if frames.is_empty() {
            return Ok(self.write_offset());
        }
        let mut inner = self.inner.write().unwrap();
        let start_off = inner.write_offset;
        let mut pos = 0usize;
        for fr in &frames {
            let raw = &data[pos..pos + fr.length as usize];
            pos += fr.length as usize;
            inner.rotate_if_needed_locked(self, fr.length)?;
            let seg_idx = inner.active_index;
            let local_off = inner.write_offset - inner.segments[seg_idx].base_lsn;
            inner.write_segment_at(seg_idx, local_off, raw)?;
            inner.write_offset += fr.length;
            if inner.write_offset - inner.segments[seg_idx].base_lsn
                >= inner.usable_segment_size(self.segment_size)
            {
                inner.rotate_segment_locked(self)?;
            }
        }
        let sync_handle = if fsync {
            Some(self.begin_sync_locked(&inner)?)
        } else {
            self.publish_append_locked(&inner);
            None
        };
        drop(inner);
        if let Some((sync_f, flushed)) = sync_handle {
            self.complete_sync(sync_f, flushed);
        }
        Ok(start_off)
    }

    pub fn is_frame_boundary(&self, offset: i64) -> bool {
        let inner = self.inner.read().unwrap();
        if offset == inner.write_offset {
            return true;
        }
        if offset < 0 {
            return false;
        }
        let Some((seg, local)) = inner.resolve_lsn(offset) else {
            return false;
        };
        let limit = inner.segment_scan_limit(seg, file_size_of(&seg.path), self.segment_size);
        inner.read_frame_at_seg(seg, local, limit).3.is_ok()
    }

    pub fn replay<F>(
        &self,
        truncate_corrupt: bool,
        mut on_record: F,
    ) -> Result<(), EngineError>
    where
        F: FnMut(Record, RecordSpan),
    {
        let mut inner = self.inner.write().unwrap();
        let mut records = 0u64;
        let active_index = inner.active_index;
        let segment_size = self.segment_size;
        let seg_count = inner.segments.len();
        for i in 0..seg_count {
            let is_active = i == active_index;
            match inner.replay_segment_at_index(
                i,
                is_active,
                truncate_corrupt,
                &mut records,
                &mut on_record,
                segment_size,
            ) {
                Ok(()) => {}
                Err(EngineError::Truncated) if is_active => {
                    return self.persist_manifest_locked(&mut inner);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn close(&self) -> io::Result<()> {
        let mut inner = self.inner.write().unwrap();
        self.inflight_syncs.wait();
        let mut first_err: Option<io::Error> = None;
        for seg in &mut inner.segments {
            if let Some(m) = seg.mapping.take() {
                unmap_wal(m);
            }
            if let Some(f) = seg.writer.take() {
                if let Err(e) = f.sync_all() {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
            if let Some(f) = seg.reader.take() {
                if let Err(e) = f.sync_all() {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    pub fn close_active_file_for_test(&self) {
        let mut inner = self.inner.write().unwrap();
        let idx = inner.active_index;
        inner.segments[idx].writer.take();
    }

    pub fn active_segment_path(&self) -> String {
        let inner = self.inner.read().unwrap();
        inner.segments[inner.active_index].path.clone()
    }

    pub fn delete_segments_through(&self, max_end_lsn: i64) -> Result<(i32, i64), EngineError> {
        let mut inner = self.inner.write().unwrap();
        inner.delete_segments_through_locked(self, max_end_lsn)
    }

    pub fn init_log_at_lsn(&self, lsn: i64) -> Result<(), EngineError> {
        if lsn < 0 {
            return Err(EngineError::Other(format!("init log at lsn: negative lsn {lsn}")));
        }
        let mut inner = self.inner.write().unwrap();
        if inner.write_offset == lsn
            && !inner.segments.is_empty()
            && inner.segments[inner.active_index].base_lsn == lsn
        {
            return Ok(());
        }
        if inner.write_offset != 0 {
            return Err(EngineError::Other(format!(
                "init log at lsn: log is not empty (writeOffset={})",
                inner.write_offset
            )));
        }
        if lsn == 0 {
            return Ok(());
        }
        if inner.segments.len() != 1 {
            return Err(EngineError::Other(
                "init log at lsn: expected a single empty segment".into(),
            ));
        }
        let active = inner.active_index;
        if inner.segments[active].base_lsn != 0 {
            return Err(EngineError::Other(format!(
                "init log at lsn: unexpected base lsn {}",
                inner.segments[active].base_lsn
            )));
        }
        let old_base = inner.segments[active].base_lsn;
        let old_durable = self.durable_offset.load(Ordering::Acquire);
        inner.segments[active].base_lsn = lsn;
        inner.write_offset = lsn;
        self.durable_offset.store(lsn, Ordering::Release);
        if let Err(e) = self.persist_manifest_locked(&mut inner) {
            inner.segments[active].base_lsn = old_base;
            inner.write_offset = 0;
            self.durable_offset.store(old_durable, Ordering::Release);
            return Err(EngineError::Other(e.to_string()));
        }
        Ok(())
    }

    fn load_segments(&self, inner: &mut Inner, m: &WalManifest) -> io::Result<()> {
        inner.segments = Vec::with_capacity(m.segments.len());
        let mut active_idx = None;
        for seg in &m.segments {
            let path = self.wal_dir.join(&seg.file);
            let info = fs::metadata(&path)?;
            let mut end_lsn = seg.end_lsn;
            if seg.id == m.active_id {
                active_idx = Some(inner.segments.len());
                end_lsn = 0;
            } else if end_lsn == 0 {
                end_lsn = seg.base_lsn
                    + self.logical_size_from_file(&path, info.len() as i64);
            }
            inner.segments.push(WalSegment {
                id: seg.id,
                file: seg.file.clone(),
                path: path.to_string_lossy().into_owned(),
                base_lsn: seg.base_lsn,
                end_lsn,
                writer: None,
                reader: None,
                mapping: None,
                allocated: info.len() as i64 >= self.segment_size,
            });
            if seg.id != m.active_id {
                if let Ok(rf) = File::open(&path) {
                    let idx = inner.segments.len() - 1;
                    inner.segments[idx].reader = Some(rf);
                    self.map_segment(&mut inner.segments[idx]);
                }
            }
        }
        let active_idx = active_idx.ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("wal manifest active segment {} not found", m.active_id),
            )
        })?;
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&inner.segments[active_idx].path)?;
        self.ensure_allocated(&f)?;
        let used = read_segment_footer(&f, self.segment_size).unwrap_or_else(|| {
            self.logical_size_from_file(
                Path::new(&inner.segments[active_idx].path),
                file_size_of(&inner.segments[active_idx].path),
            )
        });
        inner.segments[active_idx].writer = Some(f);
        inner.segments[active_idx].end_lsn = 0;
        inner.segments[active_idx].allocated =
            file_allocated(&inner.segments[active_idx].writer, self.segment_size);
        self.map_segment(&mut inner.segments[active_idx]);
        inner.active_index = active_idx;
        inner.write_offset = inner.segments[active_idx].base_lsn + used;
        Ok(())
    }

    fn load_recycle_pool(&self, inner: &mut Inner) -> io::Result<()> {
        let dir = recycle_dir(&self.wal_dir);
        fs::create_dir_all(&dir)?;
        let entries = fs::read_dir(&dir)?;
        let mut max_seq = 0u32;
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                continue;
            }
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.metadata()?.len() < self.segment_size as u64 {
                if reset_allocated_wal_file(&path, self.segment_size).is_err() {
                    let _ = fs::remove_file(&path);
                    continue;
                }
            }
            inner.recycle.push(path.to_string_lossy().into_owned());
            if let Some(seq) = parse_recycle_seq(&name) {
                max_seq = max_seq.max(seq);
            }
        }
        inner.recycle_seq = max_seq;
        Ok(())
    }

    fn write_frame_locked(&self, inner: &mut Inner, payload: &[u8]) -> Result<(), EngineError> {
        let length = payload.len() as u32;
        let checksum = castagnoli_checksum(payload);
        let total_len = LOG_FRAME_HEADER_SIZE + payload.len();
        FRAME_SCRATCH.with(|scratch| {
            let mut buf = scratch.borrow_mut();
            if buf.len() < total_len {
                buf.resize(total_len, 0);
            }
            buf[0..4].copy_from_slice(&length.to_be_bytes());
            buf[4..8].copy_from_slice(&checksum.to_be_bytes());
            buf[8..total_len].copy_from_slice(payload);

            inner.rotate_if_needed_locked(self, total_len as i64)?;
            let seg_idx = inner.active_index;
            let local_off = inner.write_offset - inner.segments[seg_idx].base_lsn;
            inner.write_segment_at(seg_idx, local_off, &buf[..total_len])?;
            if let Some(b) = &self.buffers {
                b.apply_write(inner.segments[seg_idx].id, local_off, &buf[..total_len]);
            }
            inner.write_offset += total_len as i64;
            if inner.write_offset - inner.segments[seg_idx].base_lsn
                >= inner.usable_segment_size(self.segment_size)
            {
                inner.rotate_segment_locked(self)?;
            }
            Ok(())
        })
    }

    /// Snapshots the write head and returns a writer clone for fdatasync after the WAL lock is released.
    fn begin_sync_locked(&self, inner: &Inner) -> Result<(Option<File>, i64), EngineError> {
        self.persist_head_locked(inner)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        let flushed = inner.write_offset;
        let sync_f = inner.segments[inner.active_index]
            .writer
            .as_ref()
            .and_then(|f| f.try_clone().ok());
        if sync_f.is_some() {
            self.syncing.fetch_add(1, Ordering::AcqRel);
            self.inflight_syncs.add();
        }
        Ok((sync_f, flushed))
    }

    /// Must not take the WAL lock: concurrent appends may proceed during fdatasync.
    fn complete_sync(&self, sync_f: Option<File>, flushed: i64) {
        if let Some(f) = sync_f {
            if let Some(hook) = TESTING_BEFORE_SYNC.lock().unwrap().as_ref() {
                hook();
            }
            self.sync_writer(&f);
            self.syncing.fetch_sub(1, Ordering::AcqRel);
            self.inflight_syncs.done();
        }
        self.publish_durable(flushed);
    }

    fn sync_writer(&self, f: &File) {
        if let Err(e) = sync_file(f) {
            if e.kind() == ErrorKind::Interrupted {
                return self.sync_writer(f);
            }
            panic!("CRITICAL STORAGE FAILURE: {e}");
        }
    }

    fn publish_append_locked(&self, inner: &Inner) {
        if self.syncing.load(Ordering::Acquire) != 0 {
            return;
        }
        self.publish_durable(inner.write_offset);
    }

    fn publish_durable(&self, off: i64) {
        loop {
            let cur = self.durable_offset.load(Ordering::Acquire);
            if off <= cur {
                return;
            }
            if self
                .durable_offset
                .compare_exchange(cur, off, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    fn persist_head_locked(&self, inner: &Inner) -> io::Result<()> {
        let seg = &inner.segments[inner.active_index];
        if seg.writer.is_none() || !seg.allocated {
            return Ok(());
        }
        write_segment_footer(
            seg.writer.as_ref().unwrap(),
            self.segment_size,
            inner.write_offset - seg.base_lsn,
        )
    }

    fn persist_manifest_locked(&self, inner: &mut Inner) -> Result<(), EngineError> {
        save_wal_manifest(&self.manifest_path, &inner.manifest_snapshot(self.segment_size))
            .map_err(|e| EngineError::Other(e.to_string()))
    }

    fn ensure_allocated(&self, f: &File) -> io::Result<()> {
        let info = f.metadata()?;
        if info.len() as i64 >= self.segment_size {
            return Ok(());
        }
        if let Err(e) = preallocate_file(f, self.segment_size) {
            if is_no_space(&e) {
                return Ok(());
            }
            return Err(e);
        }
        write_segment_footer(f, self.segment_size, info.len() as i64)
    }

    fn map_segment(&self, seg: &mut WalSegment) {
        if seg.mapping.is_some() {
            return;
        }
        let f = seg.writer.as_ref().or(seg.reader.as_ref());
        let Some(f) = f else { return };
        if f.metadata().map(|m| m.len()).unwrap_or(0) < self.segment_size as u64 {
            return;
        }
        match mmap_wal_file(f, self.segment_size) {
            Ok(Some(m)) => seg.mapping = Some(m),
            Ok(None) => {}
            Err(e) => log::warn!("wal mmap failed segment={} err={e}", seg.file),
        }
    }

    fn logical_size_from_file(&self, path: &Path, size: i64) -> i64 {
        if size >= self.segment_size {
            if let Ok(f) = File::open(path) {
                if let Some(used) = read_segment_footer(&f, self.segment_size) {
                    return used;
                }
            }
        }
        let usable = self.segment_size - WAL_SEG_FOOTER_SIZE;
        if size > usable {
            usable
        } else {
            size
        }
    }
}

impl Inner {
    fn manifest_snapshot(&self, segment_size: i64) -> WalManifest {
        WalManifest {
            version: super::manifest::WAL_MANIFEST_VERSION,
            segment_size,
            scan_floor: 0,
            active_id: self.segments[self.active_index].id,
            segments: self
                .segments
                .iter()
                .enumerate()
                .map(|(i, seg)| WalManifestSegment {
                    id: seg.id,
                    file: seg.file.clone(),
                    base_lsn: seg.base_lsn,
                    end_lsn: if i != self.active_index && seg.end_lsn > seg.base_lsn {
                        seg.end_lsn
                    } else {
                        0
                    },
                })
                .collect(),
        }
    }

    fn usable_segment_size(&self, segment_size: i64) -> i64 {
        if segment_size <= WAL_SEG_FOOTER_SIZE + 64 {
            segment_size
        } else {
            segment_size - WAL_SEG_FOOTER_SIZE
        }
    }

    fn segment_used_bytes(&self, seg: &WalSegment, write_offset: i64) -> i64 {
        if seg.end_lsn > 0 {
            (seg.end_lsn - seg.base_lsn).max(0)
        } else {
            (write_offset - seg.base_lsn).max(0)
        }
    }

    fn segment_scan_limit(&self, seg: &WalSegment, file_size: i64, segment_size: i64) -> i64 {
        let mut used = self.segment_used_bytes(seg, self.write_offset);
        let usable = if segment_size <= WAL_SEG_FOOTER_SIZE + 64 {
            segment_size
        } else {
            segment_size - WAL_SEG_FOOTER_SIZE
        };
        if used > usable {
            used = usable;
        }
        if file_size > 0 && used > file_size {
            return file_size;
        }
        used.max(0)
    }

    fn resolve_lsn(&self, lsn: i64) -> Option<(&WalSegment, i64)> {
        if lsn < 0 || lsn >= self.write_offset {
            return None;
        }
        for seg in self.segments.iter().rev() {
            if lsn < seg.base_lsn {
                continue;
            }
            let mut end = seg.end_lsn;
            if end == 0 {
                end = self.write_offset;
            }
            if lsn >= end {
                return None;
            }
            return Some((seg, lsn - seg.base_lsn));
        }
        None
    }

    fn segment_index_for_lsn(&self, lsn: i64) -> i32 {
        for (i, seg) in self.segments.iter().enumerate() {
            let mut end = seg.end_lsn;
            if end == 0 {
                end = self.write_offset;
            }
            if lsn >= seg.base_lsn && lsn < end {
                return i as i32;
            }
        }
        -1
    }

    fn read_value_at_locked_into(
        &self,
        offset: i64,
        val_len: u32,
        out: &mut Vec<u8>,
    ) -> Result<(), EngineError> {
        let (seg, local) = self
            .resolve_lsn(offset)
            .ok_or(EngineError::InvalidLogOffset)?;
        let mut header = [0u8; LOG_FRAME_HEADER_SIZE];
        self.read_segment_at(seg, local, &mut header)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        let payload_len = u32::from_be_bytes(header[0..4].try_into().unwrap());
        if payload_len > 1 << 30 {
            return Err(EngineError::CorruptData);
        }
        let checksum = u32::from_be_bytes(header[4..8].try_into().unwrap());
        let payload_start = local + LOG_FRAME_HEADER_SIZE as i64;

        if let Some(m) = &seg.mapping {
            let mmap = &m.mmap;
            let start = payload_start as usize;
            let end = start + payload_len as usize;
            if end > mmap.len() {
                return Err(EngineError::CorruptData);
            }
            let payload = &mmap[start..end];
            if castagnoli_checksum(payload) != checksum {
                return Err(EngineError::Checksum);
            }
            return decode_value_into(payload, val_len, out);
        }

        let mut payload = Vec::with_capacity(payload_len as usize);
        payload.resize(payload_len as usize, 0);
        self.read_segment_at(seg, payload_start, &mut payload)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        if castagnoli_checksum(&payload) != checksum {
            return Err(EngineError::Checksum);
        }
        decode_value_into(&payload, val_len, out)
    }

    fn read_segment_at(&self, seg: &WalSegment, local: i64, dst: &mut [u8]) -> io::Result<()> {
        if dst.is_empty() {
            return Ok(());
        }
        if let Some(m) = &seg.mapping {
            let m = &m.mmap;
            if local < 0 || local + dst.len() as i64 > m.len() as i64 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            dst.copy_from_slice(&m[local as usize..local as usize + dst.len()]);
            return Ok(());
        }
        let owned;
        let f: &File = if let Some(w) = seg.writer.as_ref() {
            w
        } else if let Some(r) = seg.reader.as_ref() {
            r
        } else {
            owned = File::open(&seg.path)?;
            &owned
        };
        let mut off = 0;
        while off < dst.len() {
            let n = f.read_at(&mut dst[off..], (local + off as i64) as u64)?;
            if n == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            off += n;
        }
        Ok(())
    }

    fn write_segment_at(&mut self, seg_idx: usize, local_off: i64, buf: &[u8]) -> io::Result<usize> {
        let seg = &mut self.segments[seg_idx];
        let f = seg.writer.as_mut().ok_or_else(|| {
            io::Error::new(ErrorKind::NotConnected, "no active wal writer")
        })?;
        let mut off = 0;
        while off < buf.len() {
            let n = f.write_at(&buf[off..], (local_off + off as i64) as u64)?;
            if n == 0 {
                return Err(ErrorKind::WriteZero.into());
            }
            off += n;
        }
        Ok(off)
    }

    fn rotate_if_needed_locked(&mut self, log: &DataLog, need: i64) -> Result<(), EngineError> {
        if need <= 0 {
            return Ok(());
        }
        let usable = self.usable_segment_size(log.segment_size);
        let local = self.write_offset - self.segments[self.active_index].base_lsn;
        if local + need <= usable {
            return Ok(());
        }
        if local == 0 {
            return Ok(());
        }
        self.rotate_segment_locked(log)
    }

    fn rotate_segment_locked(&mut self, log: &DataLog) -> Result<(), EngineError> {
        log.inflight_syncs.wait();
        let active_idx = self.active_index;
        if self.segments[active_idx].allocated {
            if let Some(w) = self.segments[active_idx].writer.as_ref() {
                write_segment_footer(
                    w,
                    log.segment_size,
                    self.write_offset - self.segments[active_idx].base_lsn,
                )
                .map_err(|e| EngineError::Other(e.to_string()))?;
            }
        }
        if let Some(w) = self.segments[active_idx].writer.as_ref() {
            sync_file(w).map_err(|e| EngineError::Other(e.to_string()))?;
        }
        log.publish_durable(self.write_offset);
        self.segments[active_idx].end_lsn = self.write_offset;
        self.segments[active_idx].writer.take();
        if let Ok(rf) = File::open(&self.segments[active_idx].path) {
            self.segments[active_idx].reader = Some(rf);
        }

        let next_id = self.segments[active_idx].id + 1;
        let next_name = wal_segment_file_name(next_id);
        let next_path = log.wal_dir.join(&next_name);
        let f = match self.take_recycled_segment(&next_path) {
            Ok((f, _)) => f,
            Err(_) => create_allocated_wal_file(&next_path, log.segment_size)
                .map_err(|e| EngineError::Other(e.to_string()))?,
        };

        self.segments.push(WalSegment {
            id: next_id,
            file: next_name,
            path: next_path.to_string_lossy().into_owned(),
            base_lsn: self.write_offset,
            end_lsn: 0,
            writer: Some(f),
            reader: None,
            mapping: None,
            allocated: false,
        });
        let new_idx = self.segments.len() - 1;
        self.segments[new_idx].allocated =
            file_allocated(&self.segments[new_idx].writer, log.segment_size);
        log.map_segment(&mut self.segments[new_idx]);
        self.active_index = new_idx;
        log.persist_manifest_locked(self)
    }

    fn take_recycled_segment(&mut self, next_path: &Path) -> Result<(File, bool), EngineError> {
        while !self.recycle.is_empty() {
            let src = self.recycle.remove(0);
            if fs::rename(&src, next_path).is_err() {
                let _ = fs::remove_file(&src);
                continue;
            }
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(next_path)
                .map_err(|e| EngineError::Other(e.to_string()))?;
            return Ok((f, true));
        }
        Err(EngineError::Other("no recycled segment".into()))
    }

    fn recycle_or_remove(&mut self, log: &DataLog, path: &str) -> Result<bool, EngineError> {
        if self.recycle.len() >= super::recycle::MAX_RECYCLED_SEGMENTS {
            fs::remove_file(path).ok();
            return Ok(false);
        }
        fs::create_dir_all(recycle_dir(&log.wal_dir)).ok();
        self.recycle_seq += 1;
        let dst = recycle_dir(&log.wal_dir).join(format!("r-{:06}", self.recycle_seq));
        if fs::rename(path, &dst).is_err() {
            fs::remove_file(path).ok();
            return Ok(false);
        }
        if reset_allocated_wal_file(&dst, log.segment_size).is_err() {
            let _ = fs::remove_file(&dst);
            return Ok(false);
        }
        self.recycle.push(dst.to_string_lossy().into_owned());
        Ok(true)
    }

    fn delete_segments_through_locked(
        &mut self,
        log: &DataLog,
        max_end_lsn: i64,
    ) -> Result<(i32, i64), EngineError> {
        if max_end_lsn <= 0 {
            return Ok((0, 0));
        }
        let active_index = self.active_index;
        let mut kept = Vec::new();
        let mut doomed = Vec::new();
        let mut reclaimed = 0i64;
        for (i, seg) in self.segments.iter().enumerate() {
            if i == active_index {
                kept.push(i);
                continue;
            }
            let mut end = seg.end_lsn;
            if end == 0 {
                end = seg.base_lsn
                    + log.logical_size_from_file(Path::new(&seg.path), file_size_of(&seg.path));
            }
            if end > max_end_lsn {
                kept.push(i);
                continue;
            }
            if let Ok(info) = fs::metadata(&seg.path) {
                reclaimed += info.len() as i64;
            }
            doomed.push(i);
        }
        if doomed.is_empty() {
            return Ok((0, 0));
        }
        let old_segments = std::mem::take(&mut self.segments);
        let old_active = self.active_index;
        let mut doomed_segs = Vec::new();
        let mut new_segments = Vec::new();
        for (i, seg) in old_segments.into_iter().enumerate() {
            if kept.contains(&i) {
                new_segments.push(seg);
            } else if doomed.contains(&i) {
                doomed_segs.push(seg);
            }
        }
        self.segments = new_segments;
        self.active_index = self
            .segments
            .iter()
            .position(|s| s.writer.is_some())
            .unwrap_or(0);
        if self.segments.iter().all(|s| s.writer.is_none()) {
            return Err(EngineError::Other("wal: no active segment after delete".into()));
        }
        if let Err(e) = log.persist_manifest_locked(self) {
            return Err(EngineError::Other(e.to_string()));
        }
        let mut deleted = 0i32;
        for mut seg in doomed_segs {
            close_segment_files(&mut seg);
            if self.recycle_or_remove(log, &seg.path).unwrap_or(false) {
                deleted += 1;
            }
        }
        let _ = old_active;
        Ok((deleted, reclaimed))
    }

    fn read_frame_at_seg(
        &self,
        seg: &WalSegment,
        offset: i64,
        limit: i64,
    ) -> (i64, Record, RecordSpan, Result<(), ErrorKind>) {
        if let Some(m) = &seg.mapping {
            let m = &m.mmap;
            let limit = limit.min(m.len() as i64);
            return read_frame_at_mapping(m, offset, limit);
        }
        let f = match seg.writer.as_ref().or(seg.reader.as_ref()) {
            Some(f) => f,
            None => {
                return (
                    offset,
                    Record {
                        ty: RecordType::Begin,
                        xid: 0,
                        key: vec![],
                        value: vec![],
                    },
                    RecordSpan { offset: 0, length: 0 },
                    Err(ErrorKind::NotFound),
                )
            }
        };
        read_frame_at_file(f, offset, limit)
    }

    fn replay_segment_at_index<F>(
        &mut self,
        seg_idx: usize,
        is_active: bool,
        truncate_corrupt: bool,
        records: &mut u64,
        on_record: &mut F,
        segment_size: i64,
    ) -> Result<(), EngineError>
    where
        F: FnMut(Record, RecordSpan),
    {
        let base_lsn = self.segments[seg_idx].base_lsn;
        let limit = {
            let seg = &self.segments[seg_idx];
            self.segment_scan_limit(seg, file_size_of(&seg.path), segment_size)
        };
        if limit <= 0 {
            return Ok(());
        }
        if let Some(m) = &self.segments[seg_idx].mapping {
            advise_wal_range(&m.mmap, 0, limit, wal_advise_sequential());
        }
        let mut pos = 0i64;
        while pos < limit {
            let (valid_end, rec, mut span, rerr) = {
                let seg = &self.segments[seg_idx];
                self.read_frame_at_seg(seg, pos, limit)
            };
            if let Err(kind) = rerr {
                if (kind == ErrorKind::UnexpectedEof
                    || kind == ErrorKind::InvalidData
                    || kind == ErrorKind::Other)
                    && truncate_corrupt
                    && is_active
                {
                    if let Some(w) = self.segments[self.active_index].writer.as_ref() {
                        let _ = write_segment_footer_if_allocated(w, segment_size, pos);
                    }
                    self.write_offset = base_lsn + pos;
                    return Err(EngineError::Truncated);
                }
                if kind == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(EngineError::CorruptData);
            }
            span.offset = base_lsn + span.offset;
            on_record(rec, span);
            *records += 1;
            pos = valid_end;
        }
        if let Some(m) = &self.segments[seg_idx].mapping {
            advise_wal_range(&m.mmap, 0, limit, wal_advise_random());
        }
        Ok(())
    }
}

fn file_allocated(writer: &Option<File>, segment_size: i64) -> bool {
    writer
        .as_ref()
        .and_then(|f| f.metadata().ok())
        .map(|m| m.len() as i64 >= segment_size)
        .unwrap_or(false)
}

fn file_size_of(path: &str) -> i64 {
    fs::metadata(path).map(|m| m.len() as i64).unwrap_or(0)
}

fn close_segment_files(seg: &mut WalSegment) {
    if let Some(m) = seg.mapping.take() {
        unmap_wal(m);
    }
    seg.writer.take();
    seg.reader.take();
}

fn read_frame_at_mapping(
    m: &[u8],
    offset: i64,
    file_size: i64,
) -> (i64, Record, RecordSpan, Result<(), ErrorKind>) {
    if offset < 0 {
        return frame_err(offset, ErrorKind::InvalidInput);
    }
    if offset + LOG_FRAME_HEADER_SIZE as i64 > file_size
        || offset + LOG_FRAME_HEADER_SIZE as i64 > m.len() as i64
    {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    let length = u32::from_be_bytes(m[offset as usize..offset as usize + 4].try_into().unwrap());
    let checksum =
        u32::from_be_bytes(m[offset as usize + 4..offset as usize + 8].try_into().unwrap());
    if length > 1 << 30 {
        return frame_err(offset, ErrorKind::InvalidData);
    }
    let total = frame_size(length as usize);
    if offset + total > file_size || offset + total > m.len() as i64 {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    let payload = &m[offset as usize + LOG_FRAME_HEADER_SIZE..offset as usize + LOG_FRAME_HEADER_SIZE + length as usize];
    if castagnoli_checksum(payload) != checksum {
        return frame_err(offset, ErrorKind::Other);
    }
    match decode_record(payload) {
        Ok(rec) => (
            offset + total,
            rec,
            RecordSpan {
                offset,
                length: total,
            },
            Ok(()),
        ),
        Err(_) => frame_err(offset, ErrorKind::InvalidData),
    }
}

fn read_frame_at_file(
    f: &File,
    offset: i64,
    file_size: i64,
) -> (i64, Record, RecordSpan, Result<(), ErrorKind>) {
    if offset < 0 {
        return frame_err(offset, ErrorKind::InvalidInput);
    }
    if offset + LOG_FRAME_HEADER_SIZE as i64 > file_size {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    let mut header = [0u8; LOG_FRAME_HEADER_SIZE];
    if f.read_exact_at(&mut header, offset as u64).is_err() {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    let length = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let checksum = u32::from_be_bytes(header[4..8].try_into().unwrap());
    if length > 1 << 30 {
        return frame_err(offset, ErrorKind::InvalidData);
    }
    let total = frame_size(length as usize);
    if offset + total > file_size {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    let mut payload = vec![0u8; length as usize];
    if f.read_exact_at(&mut payload, (offset + LOG_FRAME_HEADER_SIZE as i64) as u64)
        .is_err()
    {
        return frame_err(offset, ErrorKind::UnexpectedEof);
    }
    if castagnoli_checksum(&payload) != checksum {
        return frame_err(offset, ErrorKind::Other);
    }
    match decode_record(&payload) {
        Ok(rec) => (
            offset + total,
            rec,
            RecordSpan {
                offset,
                length: total,
            },
            Ok(()),
        ),
        Err(_) => frame_err(offset, ErrorKind::InvalidData),
    }
}

fn frame_err(
    offset: i64,
    kind: ErrorKind,
) -> (i64, Record, RecordSpan, Result<(), ErrorKind>) {
    (
        offset,
        Record {
            ty: RecordType::Begin,
            xid: 0,
            key: vec![],
            value: vec![],
        },
        RecordSpan { offset: 0, length: 0 },
        Err(kind),
    )
}

fn gather_from_buffers(
    buffers: &SharedBuffers,
    seg: &WalSegment,
    local: i64,
    n: i64,
    inner: &Inner,
    pins: &mut Vec<usize>,
) -> Result<Vec<u8>, EngineError> {
    if n <= 0 {
        return Ok(Vec::new());
    }
    let mut out = vec![0u8; n as usize];
    let mut copied = 0i64;
    while copied < n {
        let pos = local + copied;
        let page_no = (pos / SHARED_BUFFER_PAGE_SIZE) as u32;
        let page_off = (pos % SHARED_BUFFER_PAGE_SIZE) as usize;
        let tag = BufferTag {
            id: seg.id,
            page: page_no,
        };
        let page_start = page_no as i64 * SHARED_BUFFER_PAGE_SIZE;
        let (idx, ()) = buffers
            .pin(tag, |page| {
                inner
                    .read_segment_at(seg, page_start, page)
                    .map_err(|e| e.to_string())
            })
            .map_err(|e| EngineError::Other(e))?;
        pins.push(idx);
        let chunk = ((SHARED_BUFFER_PAGE_SIZE as i64) - (pos % SHARED_BUFFER_PAGE_SIZE))
            .min(n - copied) as usize;
        let mut buf = vec![0u8; chunk];
        buffers.copy_page(idx, page_off, &mut buf);
        out[copied as usize..copied as usize + chunk].copy_from_slice(&buf);
        copied += chunk as i64;
    }
    Ok(out)
}

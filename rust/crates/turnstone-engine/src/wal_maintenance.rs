// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use turnstone_hashindex::Version;

use crate::encode_record;
use crate::index_gc::IndexGcContext;
use crate::types::{EngineError, IndexVersion, Record, RecordType};
use crate::Db;

pub const DEFAULT_WAL_COPY_FORWARD_RATIO: f64 = 3.0;
const COPY_FORWARD_DRAIN_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Default)]
pub struct WalRetentionResult {
    pub segments_deleted: i32,
    pub bytes_reclaimed: i64,
    pub delete_through: i64,
    pub frames_copied: i32,
}

impl Db {
    pub fn run_wal_maintenance(&self) -> Result<(), EngineError> {
        let _guard = self.wal_maint_mu.lock();
        if self.index_compact_on_retention {
            let _ = self.maybe_compact_index()?;
        }
        if self.wal_copy_forward_on_retention {
            let _ = self.maybe_copy_forward_wal(self.scan_floor())?;
        }
        let _ = self.delete_wal_segments(self.min_deletable_lsn())?;
        Ok(())
    }

    pub fn delete_wal_segments(&self, min_deletable_lsn: i64) -> Result<WalRetentionResult, EngineError> {
        let delete_through = self.effective_wal_delete_through(min_deletable_lsn);
        if delete_through <= 0 {
            return Ok(WalRetentionResult::default());
        }
        let (deleted, reclaimed) = self
            .log
            .delete_segments_through(delete_through)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        Ok(WalRetentionResult {
            segments_deleted: deleted,
            bytes_reclaimed: reclaimed,
            delete_through,
            ..Default::default()
        })
    }

    pub fn min_deletable_lsn(&self) -> i64 {
        self.effective_wal_delete_through(self.scan_floor())
    }

    fn effective_wal_delete_through(&self, min_deletable_lsn: i64) -> i64 {
        let mut scan_floor = self.scan_floor();
        if min_deletable_lsn > 0 {
            scan_floor = min_deletable_lsn;
        }
        let ctx = self.build_index_gc_context();
        if let Some(mvcc_min) = self.index_min_mvcc_referenced_offset(&ctx) {
            if scan_floor <= 0 {
                return mvcc_min;
            }
            if mvcc_min < scan_floor {
                return mvcc_min;
            }
        }
        scan_floor
    }

    fn index_min_mvcc_referenced_offset(&self, ctx: &IndexGcContext) -> Option<i64> {
        let mut min_off = i64::MAX;
        let mut found = false;
        self.index.for_each_key(|_key, chain| {
            let kept = ctx.filter_versions(_key, &chain, self, false);
            for v in kept {
                found = true;
                if v.offset < min_off {
                    min_off = v.offset;
                }
            }
        });
        if found {
            Some(min_off)
        } else {
            None
        }
    }

    pub fn maybe_copy_forward_wal(
        &self,
        min_deletable_lsn: i64,
    ) -> Result<WalRetentionResult, EngineError> {
        let ratio = if self.wal_copy_forward_ratio > 0.0 {
            self.wal_copy_forward_ratio
        } else {
            DEFAULT_WAL_COPY_FORWARD_RATIO
        };
        if !self.copy_forward_fragmented(ratio) {
            return Ok(WalRetentionResult::default());
        }

        let _rewrite = self.wal_rewrite_mu.write();
        if !self.wait_for_write_transactions(COPY_FORWARD_DRAIN_TIMEOUT) {
            return Ok(WalRetentionResult::default());
        }

        let ctx = self.build_index_gc_context();
        let (old_offsets, live_bytes) = self.collect_live_frame_offsets(&ctx);
        if old_offsets.is_empty() || !self.wal_exceeds_live_ratio(ratio, live_bytes) {
            return Ok(WalRetentionResult::default());
        }

        let mut frames = Vec::with_capacity(old_offsets.len());
        for off in &old_offsets {
            frames.push(
                self.log
                    .read_frame_bytes_at(*off)
                    .map_err(|e| EngineError::Other(e.to_string()))?,
            );
        }

        let _commit = self.commit_mu.lock();
        let outcome = self
            .log
            .append_copy_forward_frames(&old_offsets, &frames)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        self.append_copy_forward_commits(&frames)?;
        self.remap_index_offsets(&ctx, &outcome.remap)?;

        let delete_through =
            copy_forward_segment_delete_through(min_deletable_lsn, outcome.head_before, self.scan_floor());
        let (deleted, _) = self
            .log
            .delete_segments_through(delete_through)
            .map_err(|e| EngineError::Other(e.to_string()))?;
        let reclaimed = (outcome.bytes_before - self.log.allocated_size()).max(0);
        Ok(WalRetentionResult {
            segments_deleted: deleted,
            bytes_reclaimed: reclaimed,
            frames_copied: old_offsets.len() as i32,
            delete_through,
        })
    }

    fn copy_forward_fragmented(&self, ratio: f64) -> bool {
        let ctx = self.build_index_gc_context();
        let live = self.estimate_live_wal_bytes(&ctx);
        self.wal_exceeds_live_ratio(ratio, live)
    }

    fn wal_exceeds_live_ratio(&self, ratio: f64, live_bytes: i64) -> bool {
        if live_bytes <= 0 {
            return false;
        }
        let mut allocated = self.log.logical_size();
        if allocated == 0 {
            allocated = self.log.allocated_size();
        }
        (allocated as f64) > (live_bytes as f64) * ratio
    }

    fn estimate_live_wal_bytes(&self, ctx: &IndexGcContext) -> i64 {
        let mut live = 0i64;
        self.for_each_live_wal_version(ctx, |_key, v| {
            live += estimated_wal_frame_size(_key, v.tombstone, v.value_len);
        });
        live
    }

    fn collect_live_frame_offsets(&self, ctx: &IndexGcContext) -> (Vec<i64>, i64) {
        let mut offsets = Vec::new();
        let mut live_bytes = 0i64;
        let mut seen = HashSet::new();
        self.for_each_live_wal_version(ctx, |key, v| {
            if seen.insert(v.offset) {
                offsets.push(v.offset);
                live_bytes += estimated_wal_frame_size(key, v.tombstone, v.value_len);
            }
        });
        offsets.sort_unstable();
        (offsets, live_bytes)
    }

    fn for_each_live_wal_version<F>(&self, ctx: &IndexGcContext, mut f: F)
    where
        F: FnMut(&[u8], IndexVersion),
    {
        let mut seen = HashSet::new();
        self.index.for_each_key(|key, chain| {
            let kept = ctx.filter_versions(key, &chain, self, false);
            for v in kept {
                if seen.insert(v.offset) {
                    f(key, v);
                }
            }
        });
    }

    fn append_copy_forward_commits(&self, frames: &[Vec<u8>]) -> Result<(), EngineError> {
        let mut seen = HashSet::new();
        let mut builders: Vec<Box<dyn Fn() -> Vec<u8> + Send>> = Vec::new();
        for frame in frames {
            if frame.len() < crate::types::LOG_FRAME_HEADER_SIZE + crate::types::LOG_RECORD_HEADER_SIZE
            {
                continue;
            }
            let payload = &frame[crate::types::LOG_FRAME_HEADER_SIZE..];
            let rec = crate::decode_record(payload)?;
            if rec.ty != RecordType::Set && rec.ty != RecordType::Delete {
                continue;
            }
            if !seen.insert(rec.xid) {
                continue;
            }
            let xid = rec.xid;
            builders.push(Box::new(move || {
                encode_record(&Record {
                    ty: RecordType::Commit,
                    xid,
                    key: vec![],
                    value: vec![],
                })
            }));
        }
        if builders.is_empty() {
            return Ok(());
        }
        let payloads: Vec<Vec<u8>> = builders.iter().map(|b| b()).collect();
        self.log
            .append_encoded_batch(&payloads, !self.unsafe_disable_fsync)
            .map(|_| ())?;
        Ok(())
    }

    fn remap_index_offsets(
        &self,
        ctx: &IndexGcContext,
        remap: &HashMap<i64, i64>,
    ) -> Result<(), EngineError> {
        if remap.is_empty() {
            return Ok(());
        }
        let remap = remap.clone();
        let filter = |key: &[u8], chain: &[Version]| {
            let in_v: Vec<IndexVersion> = chain
                .iter()
                .map(|v| IndexVersion {
                    offset: v.offset,
                    value_len: v.value_len,
                    xmin: v.xmin,
                    tombstone: v.tombstone,
                })
                .collect();
            let mut kept = ctx.filter_versions(key, &in_v, self, false);
            for v in &mut kept {
                if let Some(&n) = remap.get(&v.offset) {
                    v.offset = n;
                }
            }
            kept.into_iter()
                .map(|v| Version {
                    offset: v.offset,
                    value_len: v.value_len,
                    xmin: v.xmin,
                    tombstone: v.tombstone,
                })
                .collect()
        };
        self.index.compact_all_filtered(Some(&filter))?;
        if let Some(ref c) = self.value_cache {
            c.clear();
        }
        if let Some(ref b) = self.shared_buffers {
            b.clear();
        }
        Ok(())
    }

    fn wait_for_write_transactions(&self, timeout: Duration) -> bool {
        if self.active_xids.lock().is_empty() {
            return true;
        }
        if timeout.is_zero() {
            return false;
        }
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
            if self.active_xids.lock().is_empty() {
                return true;
            }
        }
        self.active_xids.lock().is_empty()
    }
}

fn estimated_wal_frame_size(key: &[u8], tombstone: bool, value_len: u32) -> i64 {
    let payload = 9 + key.len() + if tombstone { 0 } else { value_len as usize };
    crate::types::frame_size(payload)
}

fn copy_forward_segment_delete_through(min_deletable: i64, head_before: i64, scan_floor: i64) -> i64 {
    let mut through = head_before;
    if min_deletable > 0 && min_deletable < through {
        through = min_deletable;
    }
    if scan_floor > 0 && scan_floor < through {
        through = scan_floor;
    }
    through
}

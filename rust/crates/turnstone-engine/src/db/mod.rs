// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

mod transaction;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::clog::ClogState;
use crate::encode_record;
use crate::hashindex::{Index, IndexExt};
use crate::index::{IndexHashMetrics, MvccIndex};
use crate::recovery::replay_log;
use crate::types::{
    EngineError, IndexVersion, Record, RecordType, TxStatus, DIR_MODE,
};
use crate::wal::validate_frames;
use crate::wal::DataLog;

pub use transaction::{Transaction, TxnHandle};

#[derive(Debug, Clone, Default)]
pub struct Options {
    pub wal_segment_size: i64,
    pub max_index_arena_bytes: i64,
    pub max_disk_usage_percent: i32,
    pub truncate_corrupt_tail: bool,
    pub unsafe_disable_fsync: bool,
}

/// Main database (matches Go `engine.DB`).
pub struct Db {
    pub(crate) dir: PathBuf,
    pub(crate) log: Arc<DataLog>,
    pub(crate) index: Arc<MvccIndex>,
    pub(crate) clog: Arc<ClogState>,
    pub(crate) tx_mu: Mutex<()>,
    pub(crate) active_xids: Mutex<std::collections::HashMap<u64, Arc<transaction::WriteTransaction>>>,
    pub(crate) key_locks: Mutex<std::collections::HashMap<String, u64>>,
    pub(crate) begin_offsets: Mutex<std::collections::HashMap<u64, i64>>,
    pub(crate) active_txns: Mutex<std::collections::HashMap<u64, u64>>,
    pub(crate) transaction_id: AtomicU64,
    pub(crate) key_count: AtomicI64,
    pub(crate) scan_floor: AtomicI64,
    pub(crate) retention_offset: AtomicI64,
    pub(crate) metrics_conflicts: AtomicU64,
    pub(crate) closed: AtomicBool,
    pub(crate) unsafe_disable_fsync: bool,
    wal_rewrite_mu: RwLock<()>,
    value_cache: Option<Arc<crate::valuecache::ValueCache>>,
}

impl Db {
    pub fn open(dir: impl AsRef<Path>, opts: Options) -> Result<Arc<Self>, EngineError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(|e| EngineError::Other(e.to_string()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(DIR_MODE));
        }

        let seg_size = if opts.wal_segment_size > 0 {
            opts.wal_segment_size
        } else if let Ok(v) = std::env::var("TS_TEST_WAL_SEGMENT_SIZE") {
            v.parse().unwrap_or(0)
        } else {
            0
        };

        let mut truncate = opts.truncate_corrupt_tail;
        if std::env::var("TS_TEST_LOG_TRUNCATE").as_deref() == Ok("true") {
            truncate = true;
        }
        let mut unsafe_fsync = opts.unsafe_disable_fsync;
        if std::env::var("TS_UNSAFE_DISABLE_FSYNC").as_deref() == Ok("true") {
            unsafe_fsync = true;
        }

        let log = Arc::new(
            DataLog::open(&dir, seg_size).map_err(|e| EngineError::Other(e.to_string()))?,
        );
        let raw_index = Index::new();
        raw_index.set_max_arena_bytes(opts.max_index_arena_bytes);
        let clog = Arc::new(ClogState::default());
        let txid = AtomicU64::new(0);

        replay_log(&log, &raw_index, &clog, truncate, &txid)?;

        let index = Arc::new(MvccIndex::from_index(raw_index));
        let key_count = index.live_key_count(|xid| clog.clog_status(xid));
        let scan_floor = log.oldest_segment_base_lsn();
        let write_off = log.write_offset();
        index.set_enforce_limit(true);
        index.recalc_used_bytes();

        Ok(Arc::new(Self {
            dir,
            log,
            index,
            clog,
            tx_mu: Mutex::new(()),
            active_xids: Mutex::new(std::collections::HashMap::new()),
            key_locks: Mutex::new(std::collections::HashMap::new()),
            begin_offsets: Mutex::new(std::collections::HashMap::new()),
            active_txns: Mutex::new(std::collections::HashMap::new()),
            transaction_id: txid,
            key_count: AtomicI64::new(key_count),
            scan_floor: AtomicI64::new(scan_floor),
            retention_offset: AtomicI64::new(write_off),
            metrics_conflicts: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            unsafe_disable_fsync: unsafe_fsync,
            wal_rewrite_mu: RwLock::new(()),
            value_cache: None,
        }))
    }

    pub fn new_transaction(self: &Arc<Self>, update: bool) -> Transaction {
        Transaction::from_handle(self.new_txn_handle(update))
    }

    fn new_txn_handle(self: &Arc<Self>, update: bool) -> TxnHandle {
        if self.closed.load(Ordering::Acquire) {
            return TxnHandle::closed();
        }
        if update {
            let _guard = self.wal_rewrite_mu.write();
            let xid = self.transaction_id.fetch_add(1, Ordering::AcqRel) + 1;
            let snap = self.build_snapshot_locked();
            let begin_off = self.log.write_offset();
            let tx = Arc::new(transaction::WriteTransaction::new_write(self.clone(), xid, snap));
            self.active_xids.lock().insert(xid, tx.clone());
            self.begin_offsets.lock().insert(xid, begin_off);
            self.clog
                .active_xids
                .write()
                .unwrap()
                .insert(xid);
            let id = self.register_active_txn(xid);
            TxnHandle::Write(tx, id)
        } else {
            let _guard = self.wal_rewrite_mu.read();
            let snap = self.build_snapshot_locked();
            let id = self.register_active_txn(snap.xmax);
            TxnHandle::Read(self.clone(), snap, id)
        }
    }

    fn register_active_txn(&self, token: u64) -> u64 {
        let mut m = self.active_txns.lock();
        m.insert(token, token);
        token
    }

    pub(crate) fn unregister_active_txn(&self, token: u64) {
        self.active_txns.lock().remove(&token);
    }

    pub(crate) fn build_snapshot_locked(&self) -> crate::types::Snapshot {
        self.clog
            .build_snapshot(self.transaction_id.load(Ordering::Acquire))
    }

    pub(crate) fn clog_status(&self, xid: u64) -> TxStatus {
        self.clog.clog_status(xid)
    }

    pub(crate) fn is_visible(&self, xmin: u64, snap: &crate::types::Snapshot) -> bool {
        self.clog.is_visible(xmin, snap)
    }

    pub fn last_log_offset(&self) -> i64 {
        self.log.write_offset()
    }

    pub fn write_offset(&self) -> u64 {
        self.log.write_offset() as u64
    }

    pub fn durable_offset(&self) -> u64 {
        self.log.durable_offset() as u64
    }

    pub fn conflicts(&self) -> u64 {
        self.get_conflicts()
    }

    pub fn oldest_log_offset(&self) -> i64 {
        self.log.oldest_segment_base_lsn()
    }

    pub fn retention_offset(&self) -> i64 {
        self.retention_offset.load(Ordering::Acquire)
    }

    pub fn init_log_at_lsn(&self, lsn: i64) -> Result<(), EngineError> {
        let _guard = self.wal_rewrite_mu.write();
        self.log.init_log_at_lsn(lsn)?;
        if lsn > 0 {
            self.scan_floor.store(lsn, Ordering::Release);
            self.retention_offset.store(lsn, Ordering::Release);
        }
        Ok(())
    }

    pub fn is_valid_frame_offset(&self, offset: i64) -> bool {
        self.log.is_frame_boundary(offset)
    }

    pub fn apply_log_range(&self, data: &[u8]) -> Result<i64, EngineError> {
        let frames = validate_frames(data)?;
        if frames.is_empty() {
            return Ok(self.log.write_offset());
        }
        let _guard = self.wal_rewrite_mu.write();
        let fsync = frames.last().map(|f| f.rec.ty == RecordType::Commit) == Some(true);
        let start_off = self.log.append_raw_frames(data, fsync)?;
        let mut off = start_off;
        for fr in frames {
            let rec = fr.rec;
            match rec.ty {
                RecordType::Begin => {
                    self.clog
                        .active_xids
                        .write()
                        .unwrap()
                        .insert(rec.xid);
                }
                RecordType::Set | RecordType::Delete => {
                    self.clog
                        .active_xids
                        .write()
                        .unwrap()
                        .insert(rec.xid);
                    let is_delete = rec.ty == RecordType::Delete;
                    self.index.put(
                        &rec.key,
                        IndexVersion {
                            offset: off,
                            value_len: rec.value.len() as u32,
                            xmin: rec.xid,
                            tombstone: is_delete,
                        },
                    )?;
                    if is_delete {
                        self.account_replicated_delete(&rec.key, rec.xid);
                    } else {
                        self.account_replicated_set(&rec.key, rec.xid);
                    }
                }
                RecordType::Commit => {
                    self.clog.forget_clog(rec.xid);
                    self.clog.active_xids.write().unwrap().remove(&rec.xid);
                }
                RecordType::Abort => {
                    self.clog.set_clog(rec.xid, TxStatus::Aborted);
                    self.index.drop_xid(rec.xid)?;
                    self.clog.active_xids.write().unwrap().remove(&rec.xid);
                    self.clog.forget_clog(rec.xid);
                }
            }
            self.advance_xid(rec.xid);
            off += fr.length;
        }
        self.key_count.store(
            self.index.live_key_count(|xid| self.clog.clog_status(xid)),
            Ordering::Release,
        );
        Ok(off)
    }

    fn account_replicated_set(&self, key: &[u8], xid: u64) {
        let (ver, _, found) = self.index.latest_resolved(key, xid, |x| self.clog.clog_status(x));
        let was_live = ver.filter(|v| !v.tombstone && self.clog.clog_status(v.xmin) == TxStatus::Committed).is_some();
        if !was_live {
            self.key_count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn account_replicated_delete(&self, key: &[u8], xid: u64) {
        let (ver, _, found) = self.index.latest_resolved(key, xid, |x| self.clog.clog_status(x));
        if found {
            if let Some(v) = ver {
                if !v.tombstone && self.clog.clog_status(v.xmin) == TxStatus::Committed {
                    self.key_count.fetch_sub(1, Ordering::AcqRel);
                }
            }
        }
    }

    pub fn read_log_range(
        &self,
        start: i64,
        max_bytes: i64,
    ) -> Result<(Vec<u8>, i64), EngineError> {
        self.log.read_log_range(start, max_bytes)
    }

    pub fn set_scan_floor(&self, min_offset: i64) -> Result<(), EngineError> {
        let mut floor = min_offset;
        for off in self.begin_offsets.lock().values() {
            if *off < floor {
                floor = *off;
            }
        }
        self.scan_floor.store(floor, Ordering::Release);
        Ok(())
    }

    pub fn scan_floor(&self) -> i64 {
        self.scan_floor.load(Ordering::Acquire)
    }

    pub fn run_wal_maintenance(&self) -> Result<(), EngineError> {
        let floor = self.scan_floor();
        if floor > 0 {
            let _ = self.log.delete_segments_through(floor);
        }
        Ok(())
    }

    pub(crate) fn cache_value(&self, offset: i64, val: &[u8]) {
        if let Some(ref c) = self.value_cache {
            c.put(offset, val);
        }
    }

    pub(crate) fn cached_value(&self, offset: i64) -> Option<Vec<u8>> {
        self.value_cache.as_ref().and_then(|c| c.get(offset))
    }

    pub fn mark_retention(&self) -> Result<(), EngineError> {
        self.retention_offset
            .store(self.log.write_offset(), Ordering::Release);
        Ok(())
    }

    pub fn storage_stats(&self) -> (i64, i64) {
        (self.log.logical_size(), self.log.allocated_size())
    }

    pub fn key_count(&self) -> i64 {
        self.key_count.load(Ordering::Acquire)
    }

    pub fn get_conflicts(&self) -> u64 {
        self.metrics_conflicts.load(Ordering::Acquire)
    }

    pub fn active_transaction_count(&self) -> i32 {
        self.active_txns.lock().len() as i32
    }

    pub fn abort_all_active_write_transactions(&self) {
        let txs: Vec<Arc<transaction::WriteTransaction>> =
            self.active_xids.lock().values().cloned().collect();
        for tx in txs {
            tx.force_abort();
        }
    }

    pub fn wal_segment_count(&self) -> i32 {
        self.log.segment_count() as i32
    }

    pub fn index_hash_metrics(&self) -> IndexHashMetrics {
        self.index.index_hash_metrics()
    }

    pub fn hash_shards_compacted(&self) -> u64 {
        0
    }

    pub fn hash_compact_bytes_reclaimed(&self) -> u64 {
        0
    }

    pub fn hash_compact_unix(&self) -> i64 {
        0
    }

    pub fn wal_segment_metrics(&self) -> WalSegmentMetrics {
        let segs = self.log.segments();
        let mut infos = Vec::new();
        for (id, base, end) in segs {
            let path = self
                .dir
                .join("wal")
                .join(crate::wal::wal_segment_file_name(id));
            let size = std::fs::metadata(&path).map(|m| m.len() as i64).unwrap_or(0);
            let end_lsn = if end == 0 {
                self.log.write_offset()
            } else {
                end
            };
            infos.push(WalSegmentInfo {
                id,
                file: path.file_name().unwrap().to_string_lossy().into_owned(),
                base_lsn: base,
                end_lsn,
                size_bytes: size,
                live_bytes: 0,
                garbage_bytes: size,
                active: end == 0,
            });
        }
        let mut live_total = 0i64;
        let mut garbage_total = 0i64;
        self.index.for_each_key(|key, chain| {
            for v in chain {
                if self.clog.clog_status(v.xmin) != TxStatus::Committed {
                    continue;
                }
                let frame = estimated_frame_size(key, v.tombstone, v.value_len);
                if let Some(seg) = infos.iter_mut().find(|s| v.offset >= s.base_lsn && v.offset < s.end_lsn)
                {
                    seg.live_bytes += frame;
                    live_total += frame;
                }
            }
        });
        for seg in &mut infos {
            seg.garbage_bytes = (seg.size_bytes - seg.live_bytes).max(0);
            garbage_total += seg.garbage_bytes;
        }
        WalSegmentMetrics {
            segments: infos,
            live_bytes: live_total,
            garbage_bytes: garbage_total,
        }
    }

    pub(crate) fn append_record(
        &self,
        ty: RecordType,
        xid: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<i64, EngineError> {
        let payload = encode_record(&Record {
            ty,
            xid,
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self.log.append_encoded(&payload, false)
    }

    pub(crate) fn advance_xid(&self, xid: u64) {
        let mut cur = self.transaction_id.load(Ordering::Acquire);
        while xid > cur {
            match self.transaction_id.compare_exchange_weak(
                cur,
                xid,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    pub fn close(&self) -> Result<(), EngineError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.log
            .close()
            .map_err(|e| EngineError::Other(e.to_string()))?;
        self.index.close()?;
        Ok(())
    }
}

fn estimated_frame_size(key: &[u8], tombstone: bool, value_len: u32) -> i64 {
    let payload = 9 + key.len() + if tombstone { 0 } else { value_len as usize };
    crate::types::frame_size(payload)
}

#[derive(Debug, Clone, Default)]
pub struct WalSegmentInfo {
    pub id: u32,
    pub file: String,
    pub base_lsn: i64,
    pub end_lsn: i64,
    pub size_bytes: i64,
    pub live_bytes: i64,
    pub garbage_bytes: i64,
    pub active: bool,
}

#[derive(Debug, Clone, Default)]
pub struct WalSegmentMetrics {
    pub segments: Vec<WalSegmentInfo>,
    pub live_bytes: i64,
    pub garbage_bytes: i64,
}

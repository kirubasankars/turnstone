// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

pub(crate) mod transaction;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

use crate::clog::ClogState;
use crate::encode_record;
use crate::hashindex::Index;
use crate::index::{IndexHashMetrics, MvccIndex};
use crate::recovery::replay_log;
use crate::types::{
    EngineError, IndexVersion, Record, RecordType, TxStatus, DIR_MODE,
};
use crate::shared_buffers::{SharedBuffers, DEFAULT_SHARED_BUFFERS_BYTES};
use crate::valuecache::ValueCache;
use crate::wal::validate_frames;
use crate::wal::DataLog;

pub use transaction::{Transaction, TxnHandle};

/// Snapshot metadata for an open client transaction (index GC / WAL retain).
pub(crate) struct ActiveTxnRegistration {
    pub snapshot: crate::types::Snapshot,
    pub my_xid: u64,
    pub update: bool,
}

const DEFAULT_VALUE_CACHE_BYTES: i64 = 64 << 20;

#[derive(Debug, Clone)]
pub struct Options {
    pub wal_segment_size: i64,
    pub max_index_arena_bytes: i64,
    pub max_disk_usage_percent: i32,
    pub truncate_corrupt_tail: bool,
    pub unsafe_disable_fsync: bool,
    pub commit_delay: Duration,
    pub commit_siblings: i32,
    pub value_cache_bytes: i64,
    pub shared_buffers_bytes: i64,
    pub index_compact_fragmentation: f64,
    pub index_compact_on_retention: bool,
    pub wal_copy_forward_fragmentation: f64,
    pub wal_copy_forward_on_retention: bool,
    pub mlock: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            wal_segment_size: 0,
            max_index_arena_bytes: 0,
            max_disk_usage_percent: 90,
            truncate_corrupt_tail: false,
            unsafe_disable_fsync: false,
            commit_delay: Duration::ZERO,
            commit_siblings: 2,
            value_cache_bytes: 0,
            shared_buffers_bytes: 0,
            index_compact_fragmentation: 0.0,
            index_compact_on_retention: true,
            wal_copy_forward_fragmentation: 0.0,
            wal_copy_forward_on_retention: true,
            mlock: false,
        }
    }
}

/// Main database (matches Go `engine.DB`).
pub struct Db {
    pub(crate) dir: PathBuf,
    pub(crate) log: Arc<DataLog>,
    pub(crate) index: Arc<MvccIndex>,
    pub(crate) clog: Arc<ClogState>,
    pub(crate) active_xids: Mutex<std::collections::HashMap<u64, Arc<transaction::WriteTransaction>>>,
    pub(crate) key_locks: Mutex<std::collections::HashMap<String, u64>>,
    pub(crate) begin_offsets: Mutex<std::collections::HashMap<u64, i64>>,
    pub(crate) active_txns: Mutex<std::collections::HashMap<u64, ActiveTxnRegistration>>,
    active_txn_token: AtomicU64,
    pub(crate) transaction_id: AtomicU64,
    pub(crate) key_count: AtomicI64,
    pub(crate) scan_floor: AtomicI64,
    pub(crate) retention_offset: AtomicI64,
    pub(crate) metrics_conflicts: AtomicU64,
    pub(crate) closed: AtomicBool,
    pub(crate) unsafe_disable_fsync: bool,
    pub(crate) commit_tx: mpsc::Sender<crate::committer::CommitRequest>,
    committer_shutdown: Mutex<Option<mpsc::Sender<()>>>,
    committer_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    pub(crate) commit_mu: Mutex<()>,
    pub(crate) wal_maint_mu: Mutex<()>,
    commit_delay: Duration,
    pub(crate) commit_siblings: i32,
    pub(crate) index_fragmentation_ratio: f64,
    pub(crate) index_compact_on_retention: bool,
    pub(crate) wal_copy_forward_ratio: f64,
    pub(crate) wal_copy_forward_on_retention: bool,
    pub(crate) metrics_hash_shards_compacted: AtomicU64,
    pub(crate) metrics_hash_compact_reclaimed: AtomicU64,
    pub(crate) metrics_hash_compact_unix: AtomicI64,
    pub(crate) is_disk_full: AtomicBool,
    pub(crate) is_corrupt: AtomicBool,
    pub(crate) wal_rewrite_mu: RwLock<()>,
    pub(crate) value_cache: Option<Arc<ValueCache>>,
    pub(crate) shared_buffers: Option<Arc<SharedBuffers>>,
    max_disk_usage_percent: i32,
    disk_monitor_shutdown: Mutex<Option<mpsc::Sender<()>>>,
    disk_monitor_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
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

        let shared_buffers = if opts.shared_buffers_bytes < 0 {
            None
        } else {
            let bytes = if opts.shared_buffers_bytes == 0 {
                DEFAULT_SHARED_BUFFERS_BYTES
            } else {
                opts.shared_buffers_bytes
            };
            Some(Arc::new(
                SharedBuffers::new_locked(bytes, opts.mlock)
                    .map_err(|e| EngineError::Other(e))?,
            ))
        };
        let value_cache = if opts.value_cache_bytes < 0 {
            None
        } else {
            let bytes = if opts.value_cache_bytes == 0 {
                DEFAULT_VALUE_CACHE_BYTES
            } else {
                opts.value_cache_bytes
            };
            Some(Arc::new(ValueCache::new(bytes)))
        };

        let log = Arc::new(
            DataLog::open_with_buffers(&dir, seg_size, shared_buffers.clone())
                .map_err(|e| EngineError::Other(e.to_string()))?,
        );
        let raw_index = Index::new();
        raw_index.set_enforce_limit(false);
        let clog = Arc::new(ClogState::default());
        let txid = AtomicU64::new(0);

        replay_log(&log, &raw_index, &clog, truncate, &txid)?;

        let index = Arc::new(MvccIndex::from_index(raw_index));
        let key_count = index.live_key_count(|xid| clog.clog_status(xid));
        let scan_floor = log.oldest_segment_base_lsn();
        let write_off = log.write_offset();
        index.set_max_arena_bytes(opts.max_index_arena_bytes);
        index.set_enforce_limit(true);
        index.recalc_used_bytes();

        let commit_delay = if opts.commit_delay < Duration::ZERO {
            Duration::ZERO
        } else {
            opts.commit_delay
        };
        let commit_siblings = if opts.commit_siblings <= 0 {
            2
        } else {
            opts.commit_siblings
        };
        let index_frag = if opts.index_compact_fragmentation > 0.0 {
            opts.index_compact_fragmentation
        } else {
            crate::index_gc::DEFAULT_INDEX_FRAGMENTATION_RATIO
        };
        let wal_copy = if opts.wal_copy_forward_fragmentation > 0.0 {
            opts.wal_copy_forward_fragmentation
        } else {
            crate::wal_maintenance::DEFAULT_WAL_COPY_FORWARD_RATIO
        };

        let (commit_tx, commit_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();
        let max_disk_usage_percent = opts.max_disk_usage_percent;

        let db = Arc::new(Self {
            dir,
            log,
            index,
            clog,
            active_xids: Mutex::new(std::collections::HashMap::new()),
            key_locks: Mutex::new(std::collections::HashMap::new()),
            begin_offsets: Mutex::new(std::collections::HashMap::new()),
            active_txns: Mutex::new(std::collections::HashMap::new()),
            active_txn_token: AtomicU64::new(0),
            transaction_id: txid,
            key_count: AtomicI64::new(key_count),
            scan_floor: AtomicI64::new(scan_floor),
            retention_offset: AtomicI64::new(write_off),
            metrics_conflicts: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            unsafe_disable_fsync: unsafe_fsync,
            commit_tx,
            committer_shutdown: Mutex::new(Some(shutdown_tx)),
            committer_handle: Mutex::new(None),
            commit_mu: Mutex::new(()),
            wal_maint_mu: Mutex::new(()),
            commit_delay,
            commit_siblings,
            index_fragmentation_ratio: index_frag,
            index_compact_on_retention: opts.index_compact_on_retention,
            wal_copy_forward_ratio: wal_copy,
            wal_copy_forward_on_retention: opts.wal_copy_forward_on_retention,
            metrics_hash_shards_compacted: AtomicU64::new(0),
            metrics_hash_compact_reclaimed: AtomicU64::new(0),
            metrics_hash_compact_unix: AtomicI64::new(0),
            is_disk_full: AtomicBool::new(false),
            is_corrupt: AtomicBool::new(false),
            wal_rewrite_mu: RwLock::new(()),
            value_cache,
            shared_buffers,
            max_disk_usage_percent,
            disk_monitor_shutdown: Mutex::new(None),
            disk_monitor_handle: Mutex::new(None),
        });

        let db_runner = Arc::clone(&db);
        let handle = std::thread::spawn(move || {
            db_runner.run_group_commits(commit_rx, shutdown_rx);
        });
        *db.committer_handle.lock() = Some(handle);

        if max_disk_usage_percent > 0 {
            let (disk_tx, disk_rx) = mpsc::channel();
            let db_disk = Arc::clone(&db);
            let disk_handle = std::thread::spawn(move || {
                db_disk.run_disk_monitor(disk_rx);
            });
            *db.disk_monitor_shutdown.lock() = Some(disk_tx);
            *db.disk_monitor_handle.lock() = Some(disk_handle);
        }

        Ok(db)
    }

    pub fn shared_buffers(&self) -> Option<&Arc<SharedBuffers>> {
        self.shared_buffers.as_ref()
    }

    pub fn commit_delay(&self) -> Duration {
        self.commit_delay
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
            let tx = Arc::new(transaction::WriteTransaction::new_write(
                self.clone(),
                xid,
                snap.clone(),
            ));
            self.active_xids.lock().insert(xid, tx.clone());
            self.begin_offsets.lock().insert(xid, begin_off);
            self.clog
                .active_xids
                .write()
                .unwrap()
                .insert(xid);
            let id = self.register_active_txn(ActiveTxnRegistration {
                snapshot: snap,
                my_xid: xid,
                update: true,
            });
            TxnHandle::Write(tx, id)
        } else {
            let _guard = self.wal_rewrite_mu.read();
            let snap = self.build_snapshot_locked();
            let id = self.register_active_txn(ActiveTxnRegistration {
                snapshot: snap.clone(),
                my_xid: 0,
                update: false,
            });
            TxnHandle::Read(self.clone(), snap, id)
        }
    }

    fn register_active_txn(&self, reg: ActiveTxnRegistration) -> u64 {
        let token = self.active_txn_token.fetch_add(1, Ordering::Relaxed) + 1;
        self.active_txns.lock().insert(token, reg);
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
        let (ver, _, _found) = self.index.latest_resolved(key, xid, |x| self.clog.clog_status(x));
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
        self.metrics_hash_shards_compacted
            .load(Ordering::Acquire)
    }

    pub fn hash_compact_bytes_reclaimed(&self) -> u64 {
        self.metrics_hash_compact_reclaimed
            .load(Ordering::Acquire)
    }

    pub fn hash_compact_unix(&self) -> i64 {
        self.metrics_hash_compact_unix.load(Ordering::Acquire)
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

    fn run_disk_monitor(&self, shutdown: mpsc::Receiver<()>) {
        self.check_disk();
        loop {
            match shutdown.recv_timeout(Duration::from_secs(10)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => self.check_disk(),
            }
        }
    }

    fn check_disk(&self) {
        if self.max_disk_usage_percent <= 0 {
            return;
        }
        #[cfg(unix)]
        {
            match crate::disk_unix::get_disk_usage(&self.dir) {
                Ok(usage) => {
                    self.is_disk_full.store(
                        (usage as i32) > self.max_disk_usage_percent,
                        Ordering::Release,
                    );
                }
                Err(_) => {}
            }
        }
    }

    pub fn close(&self) -> Result<(), EngineError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        if let Some(tx) = self.committer_shutdown.lock().take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.disk_monitor_shutdown.lock().take() {
            let _ = tx.send(());
        }
        drop(self.commit_tx.clone());
        if let Some(h) = self.committer_handle.lock().take() {
            let _ = h.join();
        }
        if let Some(h) = self.disk_monitor_handle.lock().take() {
            let _ = h.join();
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

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use turnstone_engine::{
    Db, EngineError, IndexHashMetrics, Options as EngineOptions, Transaction, WalSegmentInfo,
};
use turnstone_protocol::KeyNotFound;

pub const STATE_UNDEFINED: &str = "UNDEFINED";
pub const STATE_PRIMARY: &str = "PRIMARY";
pub const STATE_REPLICA: &str = "REPLICA";
pub const STATE_STEPPING_DOWN: &str = "STEPPING_DOWN";

pub const REPLICA_ROLE_SERVER: &str = "server";
pub const REPLICA_ROLE_ADMIN: &str = "admin";
pub const REPLICA_ROLE_BACKUP: &str = "backup";

const DEFAULT_QUORUM_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicaSlotSerde {
    offset: u64,
    role: String,
    #[serde(with = "serde_ts")]
    last_seen: SystemTime,
    connected: bool,
    #[serde(default)]
    gen: u64,
}

mod serde_ts {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::{SystemTime, UNIX_EPOCH};

    pub fn serialize<S>(t: &SystemTime, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
        s.serialize_i64(d.as_secs() as i64)
    }

    pub fn deserialize<'de, D>(d: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = i64::deserialize(d)?;
        Ok(UNIX_EPOCH + std::time::Duration::from_secs(secs.max(0) as u64))
    }
}

struct ReplicaSlot {
    offset: u64,
    role: String,
    last_seen: SystemTime,
    connected: bool,
    gen: u64,
    quit_tx: Option<std::sync::mpsc::Sender<()>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaInfo {
    pub id: String,
    pub role: String,
    pub connected: bool,
    pub offset: u64,
    pub lag: u64,
    pub last_seen: String,
}

#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub active_txs: i32,
    pub uptime: String,
    pub offset: i64,
    pub conflicts: u64,
    pub replica_lag: u64,
    pub log_size: i64,
    pub log_allocated: i64,
    pub key_count: i64,
    pub hash_shards_compacted: u64,
    pub hash_compact_bytes_reclaimed: u64,
    pub hash_compact_unix: i64,
    pub server_replicas: i32,
    pub replicas: Vec<ReplicaInfo>,
}

#[derive(Debug, Clone, Default)]
pub struct StorageDetail {
    pub hash_shards: i32,
    pub index_arena_bytes: u64,
    pub index_allocated_bytes: u64,
    pub index_live_bytes: u64,
    pub wal_live_bytes: i64,
    pub wal_garbage_bytes: i64,
    pub segments: Vec<WalSegmentInfo>,
}

pub struct Database {
    pub(crate) engine: RwLock<Arc<Db>>,
    dir: PathBuf,
    db_opts: EngineOptions,
    start_time: Instant,
    min_replicas: Mutex<i32>,
    admin_mu: Mutex<()>,
    repl_mu: Mutex<()>,
    replicas: Mutex<HashMap<String, ReplicaSlot>>,
    repl_cond: Condvar,
    slots_file: PathBuf,
    repl_dirty: Mutex<bool>,
    retention_strategy: String,
    state: Mutex<String>,
    leader_retain_offset: AtomicU64,
    safe_point_seq: AtomicU64,
    safe_point_tx: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    replica_timeout: Duration,
    quorum_timeout: Duration,
    closed: AtomicU64,
    bg_shutdown: Mutex<Vec<mpsc::Sender<()>>>,
    bg_handles: Mutex<Vec<thread::JoinHandle<()>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    KeyNotFound(#[from] KeyNotFound),
    #[error("{0}")]
    Other(String),
}

pub struct OpenOptions {
    pub min_replicas: i32,
    pub retention_strategy: String,
    pub max_disk_usage_percent: i32,
    pub max_index_arena_bytes: i64,
    pub engine: EngineOptions,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            min_replicas: 0,
            retention_strategy: "none".into(),
            max_disk_usage_percent: 90,
            max_index_arena_bytes: 0,
            engine: EngineOptions::default(),
        }
    }
}

pub fn open(dir: impl AsRef<Path>, opts: OpenOptions) -> Result<Arc<Database>, DatabaseError> {
    let dir = dir.as_ref().to_path_buf();
    let mut eng = opts.engine;
    if opts.max_index_arena_bytes > 0 {
        eng.max_index_arena_bytes = opts.max_index_arena_bytes;
    }
    eng.max_disk_usage_percent = opts.max_disk_usage_percent;

    let mut replica_timeout = Duration::from_secs(60);
    if let Ok(v) = std::env::var("TS_TEST_REPLICA_TIMEOUT") {
        if let Ok(d) = parse_duration(&v) {
            replica_timeout = d;
        }
    }
    let mut quorum_timeout = DEFAULT_QUORUM_TIMEOUT;
    if let Ok(v) = std::env::var("TS_TEST_QUORUM_TIMEOUT") {
        if let Ok(d) = parse_duration(&v) {
            quorum_timeout = d;
        }
    }

    let engine = Db::open(&dir, eng.clone())?;
    let db = Arc::new(Database {
        engine: RwLock::new(engine),
        dir: dir.clone(),
        db_opts: eng,
        start_time: Instant::now(),
        min_replicas: Mutex::new(opts.min_replicas),
        admin_mu: Mutex::new(()),
        repl_mu: Mutex::new(()),
        replicas: Mutex::new(HashMap::new()),
        repl_cond: Condvar::new(),
        slots_file: dir.join("repl.slots"),
        repl_dirty: Mutex::new(false),
        retention_strategy: opts.retention_strategy.clone(),
        state: Mutex::new(STATE_UNDEFINED.into()),
        leader_retain_offset: AtomicU64::new(u64::MAX),
        safe_point_seq: AtomicU64::new(0),
        safe_point_tx: Mutex::new(None),
        replica_timeout,
        quorum_timeout,
        closed: AtomicU64::new(0),
        bg_shutdown: Mutex::new(Vec::new()),
        bg_handles: Mutex::new(Vec::new()),
    });

    db.load_slots();
    if db.retention_strategy == "replication" {
        db.start_retention_tasks();
    }
    Ok(db)
}

fn parse_duration(s: &str) -> Result<Duration, ()> {
    if let Ok(secs) = s.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }
    // simple suffix parser: 200ms, 5s
    if let Some(stripped) = s.strip_suffix("ms") {
        return stripped
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|_| ());
    }
    if let Some(stripped) = s.strip_suffix('s') {
        return stripped
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|_| ());
    }
    Err(())
}

impl Database {
    pub fn lock_admin(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.admin_mu.lock()
    }

    pub fn engine(&self) -> Arc<Db> {
        self.engine.read().clone()
    }

    pub fn new_transaction(&self, update: bool) -> Transaction {
        self.engine.read().new_transaction(update)
    }

    pub fn set_state(&self, state: impl Into<String>) {
        *self.state.lock() = state.into();
        self.repl_cond.notify_all();
    }

    pub fn get_state(&self) -> String {
        self.state.lock().clone()
    }

    pub fn follow(&self) {
        self.set_state(STATE_REPLICA);
    }

    pub fn promote(&self) -> Result<(), DatabaseError> {
        self.set_leader_retain_offset(u64::MAX);
        self.set_state(STATE_PRIMARY);
        self.trigger_safe_point();
        Ok(())
    }

    pub fn step_down(&self) -> Result<(), DatabaseError> {
        let current = self.get_state();
        match current.as_str() {
            STATE_REPLICA => {
                self.set_state(STATE_UNDEFINED);
                Ok(())
            }
            STATE_PRIMARY => {
                self.set_state(STATE_STEPPING_DOWN);
                let _ = self.wait_for_active_transactions(Duration::from_secs(5));
                self.abort_all_active_write_transactions();
                let _ = self.wait_for_replication(Duration::from_secs(5));
                self.trigger_safe_point();
                self.remove_all_replicas();
                self.set_state(STATE_UNDEFINED);
                Ok(())
            }
            _ => Err(DatabaseError::Other(
                "step down only valid for PRIMARY or REPLICA".into(),
            )),
        }
    }

    pub fn set_leader_retain_offset(&self, offset: u64) {
        self.leader_retain_offset.store(offset, Ordering::Release);
    }

    pub fn get_leader_retain_offset(&self) -> u64 {
        self.leader_retain_offset.load(Ordering::Acquire)
    }

    pub fn last_log_offset(&self) -> u64 {
        self.engine.read().write_offset()
    }

    pub fn durable_offset(&self) -> u64 {
        self.engine.read().durable_offset()
    }

    pub fn oldest_log_offset(&self) -> u64 {
        self.engine.read().oldest_log_offset().max(0) as u64
    }

    pub fn apply_log_range(&self, data: &[u8]) -> Result<i64, EngineError> {
        self.engine.read().apply_log_range(data)
    }

    pub fn read_log_range(
        &self,
        start: i64,
        max_bytes: i64,
    ) -> Result<(Vec<u8>, i64), EngineError> {
        self.engine.read().read_log_range(start, max_bytes)
    }

    pub fn init_log_at_lsn(&self, lsn: u64) -> Result<(), EngineError> {
        self.engine.read().init_log_at_lsn(lsn as i64)
    }

    pub fn is_valid_replication_cursor(&self, offset: u64) -> bool {
        let db = self.engine.read();
        if offset == 0 || offset == db.write_offset() {
            return true;
        }
        let cursor = offset as i64;
        if cursor > db.last_log_offset() {
            return false;
        }
        db.is_valid_frame_offset(cursor)
    }

    pub fn min_replicas(&self) -> i32 {
        *self.min_replicas.lock()
    }

    pub fn set_min_replicas(&self, n: i32) {
        *self.min_replicas.lock() = n;
        self.repl_cond.notify_all();
    }

    pub fn register_replica(&self, id: impl Into<String>, offset: u64, role: impl Into<String>) {
        let _g = self.repl_mu.lock();
        self.register_replica_locked(id.into(), offset, role.into());
    }

    pub fn register_replica_hello(
        &self,
        id: impl Into<String>,
        mut offset: u64,
        role: impl Into<String>,
    ) -> (u64, u64) {
        let _g = self.repl_mu.lock();
        let db = self.engine.read();
        if offset == 0 {
            let oldest = db.oldest_log_offset();
            if oldest > 0 {
                offset = oldest as u64;
            }
        }
        let gen = self.register_replica_locked(id.into(), offset, role.into());
        (offset, gen)
    }

    fn register_replica_locked(&self, id: String, offset: u64, role: String) -> u64 {
        let mut reps = self.replicas.lock();
        let mut gen = 1u64;
        if let Some(old) = reps.remove(&id) {
            if let Some(tx) = old.quit_tx {
                let _ = tx.send(());
            }
            gen = old.gen.saturating_add(1);
            if gen == 0 {
                gen = 1;
            }
        }
        let (quit_tx, _quit_rx) = std::sync::mpsc::channel();
        reps.insert(
            id,
            ReplicaSlot {
                offset,
                role,
                last_seen: SystemTime::now(),
                connected: true,
                gen,
                quit_tx: Some(quit_tx),
            },
        );
        *self.repl_dirty.lock() = true;
        self.repl_cond.notify_all();
        gen
    }

    pub fn replica_generation(&self, id: &str) -> u64 {
        self.replicas.lock().get(id).map(|s| s.gen).unwrap_or(0)
    }

    pub fn unregister_replica(&self, id: &str) {
        let mut reps = self.replicas.lock();
        if let Some(slot) = reps.get_mut(id) {
            if slot.connected {
                slot.connected = false;
                *self.repl_dirty.lock() = true;
            }
        }
    }

    pub fn unregister_replica_gen(&self, id: &str, gen: u64) {
        let mut reps = self.replicas.lock();
        if let Some(slot) = reps.get_mut(id) {
            if slot.gen == gen && slot.connected {
                slot.connected = false;
                *self.repl_dirty.lock() = true;
            }
        }
    }

    pub fn update_replica_offset(&self, id: &str, offset: u64) {
        let mut reps = self.replicas.lock();
        if let Some(slot) = reps.get_mut(id) {
            if offset > slot.offset {
                slot.offset = offset;
                *self.repl_dirty.lock() = true;
                self.repl_cond.notify_all();
            }
            slot.last_seen = SystemTime::now();
        }
    }

    pub fn min_replica_offset(&self) -> u64 {
        let reps = self.replicas.lock();
        let mut min = u64::MAX;
        let mut has = false;
        for slot in reps.values() {
            if slot.role == REPLICA_ROLE_ADMIN {
                continue;
            }
            if slot.role == REPLICA_ROLE_BACKUP && !slot.connected {
                continue;
            }
            if slot.role != REPLICA_ROLE_SERVER && slot.role != REPLICA_ROLE_BACKUP {
                continue;
            }
            has = true;
            if slot.offset < min {
                min = slot.offset;
            }
        }
        if has {
            min
        } else {
            u64::MAX
        }
    }

    pub fn wait_for_quorum(
        &self,
        offset: u64,
        timeout: Duration,
        cancel: Option<&std::sync::mpsc::Receiver<()>>,
    ) -> Result<(), DatabaseError> {
        let timeout = if timeout.is_zero() {
            self.quorum_timeout
        } else {
            timeout
        };
        let deadline = Instant::now() + timeout;
        let mut guard = self.repl_mu.lock();
        loop {
            let min_needed = *self.min_replicas.lock();
            let mut acks = 0;
            for slot in self.replicas.lock().values() {
                if slot.connected && slot.role == REPLICA_ROLE_SERVER && slot.offset >= offset {
                    acks += 1;
                }
            }
            if acks >= min_needed {
                return Ok(());
            }
            if let Some(c) = cancel {
                if c.try_recv().is_ok() {
                    return Err(DatabaseError::Other(format!(
                        "quorum wait cancelled: have {acks}/{min_needed} acks for offset {offset}"
                    )));
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(DatabaseError::Other(format!(
                    "timeout waiting for replication quorum: have {acks}/{min_needed} acks for offset {offset}"
                )));
            }
            let wait = remaining.min(Duration::from_secs(1));
            self.repl_cond.wait_for(&mut guard, wait);
        }
    }

    pub fn wait_for_active_transactions(&self, timeout: Duration) -> Result<(), DatabaseError> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.engine.read().active_transaction_count() == 0 {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(DatabaseError::Other(
            "timeout waiting for active transactions".into(),
        ))
    }

    pub fn abort_all_active_write_transactions(&self) {
        self.engine.read().abort_all_active_write_transactions();
    }

    pub fn wait_for_replication(&self, timeout: Duration) -> Result<(), DatabaseError> {
        let head = self.last_log_offset();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let min = self.min_replica_offset();
            if min == u64::MAX || min >= head {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Err(DatabaseError::Other(
            "timeout waiting for replication sync".into(),
        ))
    }

    pub fn trigger_safe_point(&self) {
        self.safe_point_seq.fetch_add(1, Ordering::AcqRel);
        if let Some(tx) = self.safe_point_tx.lock().take() {
            let _ = tx.send(());
        }
    }

    pub fn safe_point_seq(&self) -> u64 {
        self.safe_point_seq.load(Ordering::Acquire)
    }

    pub fn safe_point_signal(&self) -> std::sync::mpsc::Receiver<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        *self.safe_point_tx.lock() = Some(tx);
        rx
    }

    pub fn get_replica_signal_channel(&self, _id: &str) -> Option<std::sync::mpsc::Receiver<()>> {
        None
    }

    pub fn remove_all_replicas(&self) {
        let mut reps = self.replicas.lock();
        for slot in reps.values() {
            if let Some(tx) = &slot.quit_tx {
                let _ = tx.send(());
            }
        }
        reps.clear();
        *self.repl_dirty.lock() = true;
        self.repl_cond.notify_all();
    }

    pub fn reset_replicas(&self) {
        self.remove_all_replicas();
        let _ = self.save_slots_locked();
    }

    pub fn stats(&self) -> Stats {
        let db = self.engine.read();
        let (log_size, log_allocated) = db.storage_stats();
        let head = db.write_offset();
        let mut max_lag = 0u64;
        let mut server_replicas = 0i32;
        let mut replicas = Vec::new();
        for (id, r) in self.replicas.lock().iter() {
            let lag = if head > r.offset { head - r.offset } else { 0 };
            if r.role == REPLICA_ROLE_SERVER {
                server_replicas += 1;
                max_lag = max_lag.max(lag);
            }
            replicas.push(ReplicaInfo {
                id: id.clone(),
                role: r.role.clone(),
                connected: r.connected,
                offset: r.offset,
                lag,
                last_seen: humantime_rfc3339(r.last_seen),
            });
        }
        replicas.sort_by(|a, b| a.id.cmp(&b.id));
        Stats {
            active_txs: db.active_transaction_count(),
            uptime: format!("{:?}", self.start_time.elapsed()),
            offset: head as i64,
            conflicts: db.conflicts(),
            replica_lag: max_lag,
            log_size,
            log_allocated,
            key_count: db.key_count(),
            hash_shards_compacted: db.hash_shards_compacted(),
            hash_compact_bytes_reclaimed: db.hash_compact_bytes_reclaimed(),
            hash_compact_unix: db.hash_compact_unix(),
            server_replicas,
            replicas,
        }
    }

    pub fn storage_detail(&self) -> StorageDetail {
        let db = self.engine.read();
        let hash = db.index_hash_metrics();
        let wal = db.wal_segment_metrics();
        StorageDetail {
            hash_shards: hash.shards_used,
            index_arena_bytes: hash.arena_bytes,
            index_allocated_bytes: hash.allocated_bytes,
            index_live_bytes: hash.live_bytes,
            wal_live_bytes: wal.live_bytes,
            wal_garbage_bytes: wal.garbage_bytes,
            segments: wal.segments,
        }
    }

    pub fn index_hash_metrics(&self) -> IndexHashMetrics {
        self.engine.read().index_hash_metrics()
    }

    pub fn wal_segment_count(&self) -> i32 {
        self.engine.read().wal_segment_count()
    }

    pub fn enforce_retention_policy(&self) {
        let db = self.engine.read();
        let min_replica = self.min_replica_offset();
        let leader = self.get_leader_retain_offset();
        let retention = db.retention_offset();
        let mut safe = retention;
        if min_replica != u64::MAX {
            let m = min_replica as i64;
            if m < safe {
                safe = m;
            }
        }
        if leader != u64::MAX {
            let l = leader as i64;
            if l < safe {
                safe = l;
            }
        }
        if safe > 0 {
            let _ = db.set_scan_floor(safe);
        }
        let _ = db.run_wal_maintenance();
    }

    pub fn evict_zombie_replicas_now(&self) {
        let head = self.last_log_offset();
        let mut reps = self.replicas.lock();
        reps.retain(|_, slot| {
            if slot.offset < head
                && slot.last_seen.elapsed().unwrap_or_default() > self.replica_timeout
            {
                if let Some(tx) = &slot.quit_tx {
                    let _ = tx.send(());
                }
                *self.repl_dirty.lock() = true;
                false
            } else {
                true
            }
        });
    }

    #[cfg(test)]
    pub fn set_replica_last_seen_for_test(&self, id: &str, last_seen: SystemTime) {
        if let Some(slot) = self.replicas.lock().get_mut(id) {
            slot.last_seen = last_seen;
        }
    }

    pub fn reset(&self) -> Result<(), DatabaseError> {
        self.remove_all_replicas();
        let mut eng = self.engine.write();
        eng.close()?;
        if let Ok(entries) = std::fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
        *eng = Db::open(&self.dir, self.db_opts.clone())?;
        drop(eng);
        self.set_leader_retain_offset(u64::MAX);
        Ok(())
    }

    fn start_retention_tasks(self: &Arc<Self>) {
        let (ret_tx, ret_rx) = mpsc::channel();
        let ret_db = Arc::clone(self);
        let ret_h = thread::spawn(move || loop {
            match ret_rx.recv_timeout(Duration::from_secs(30)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => ret_db.enforce_retention_policy(),
            }
        });
        let (ev_tx, ev_rx) = mpsc::channel();
        let ev_db = Arc::clone(self);
        let ev_h = thread::spawn(move || loop {
            match ev_rx.recv_timeout(Duration::from_secs(10)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => ev_db.evict_zombie_replicas_now(),
            }
        });
        self.bg_shutdown.lock().extend([ret_tx, ev_tx]);
        self.bg_handles.lock().extend([ret_h, ev_h]);
    }

    pub fn close(&self) -> Result<(), DatabaseError> {
        if self.closed.fetch_add(1, Ordering::AcqRel) > 0 {
            return Ok(());
        }
        for tx in self.bg_shutdown.lock().drain(..) {
            let _ = tx.send(());
        }
        for h in self.bg_handles.lock().drain(..) {
            let _ = h.join();
        }
        self.engine.read().close()?;
        Ok(())
    }

    fn load_slots(&self) {
        let Ok(data) = std::fs::read_to_string(&self.slots_file) else {
            return;
        };
        let Ok(raw): Result<HashMap<String, ReplicaSlotSerde>, _> = serde_json::from_str(&data)
        else {
            return;
        };
        let mut reps = self.replicas.lock();
        for (id, s) in raw {
            reps.insert(
                id,
                ReplicaSlot {
                    offset: s.offset,
                    role: s.role,
                    last_seen: s.last_seen,
                    connected: false,
                    gen: s.gen,
                    quit_tx: None,
                },
            );
        }
    }

    fn save_slots_locked(&self) -> Result<(), DatabaseError> {
        let reps = self.replicas.lock();
        let mut out: HashMap<String, ReplicaSlotSerde> = HashMap::new();
        for (id, s) in reps.iter() {
            out.insert(
                id.clone(),
                ReplicaSlotSerde {
                    offset: s.offset,
                    role: s.role.clone(),
                    last_seen: s.last_seen,
                    connected: s.connected,
                    gen: s.gen,
                },
            );
        }
        let data =
            serde_json::to_string_pretty(&out).map_err(|e| DatabaseError::Other(e.to_string()))?;
        let tmp = self.slots_file.with_extension("slots.tmp");
        std::fs::write(&tmp, data).map_err(|e| DatabaseError::Other(e.to_string()))?;
        std::fs::rename(&tmp, &self.slots_file).map_err(|e| DatabaseError::Other(e.to_string()))?;
        *self.repl_dirty.lock() = false;
        Ok(())
    }
}

fn humantime_rfc3339(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    format!("{}Z", d.as_secs())
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use turnstone_engine::{Db, EngineError, Options as EngineOptions, Transaction};

pub const STATE_UNDEFINED: &str = "UNDEFINED";
pub const STATE_PRIMARY: &str = "PRIMARY";
pub const STATE_REPLICA: &str = "REPLICA";
pub const STATE_STEPPING_DOWN: &str = "STEPPING_DOWN";

pub const REPLICA_ROLE_SERVER: &str = "server";
pub const REPLICA_ROLE_ADMIN: &str = "admin";
pub const REPLICA_ROLE_BACKUP: &str = "backup";

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub replicas: Vec<ReplicaInfo>,
}

struct ReplicaSlot {
    offset: u64,
    role: String,
    connected: bool,
    gen: u64,
    kill_tx: Option<std::sync::mpsc::Sender<()>>,
    kill_rx: Option<std::sync::mpsc::Receiver<()>>,
}

pub struct Database {
    inner: Arc<Db>,
    dir: PathBuf,
    state: RwLock<String>,
    min_replicas: Mutex<i32>,
    admin_mu: Mutex<()>,
    replicas: Mutex<HashMap<String, ReplicaSlot>>,
    safe_point_seq: AtomicU64,
    safe_point_waiters: Mutex<Vec<std::sync::mpsc::Sender<()>>>,
    start_time: Instant,
    closed: parking_lot::RwLock<bool>,
}

use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error(transparent)]
    Engine(#[from] EngineError),
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
    let mut eng_opts = opts.engine;
    if opts.max_index_arena_bytes > 0 {
        eng_opts.max_index_arena_bytes = opts.max_index_arena_bytes;
    }
    let inner = Db::open(&dir, eng_opts)?;
    Ok(Arc::new(Database {
        inner,
        dir,
        state: RwLock::new(STATE_UNDEFINED.into()),
        min_replicas: Mutex::new(opts.min_replicas),
        admin_mu: Mutex::new(()),
        replicas: Mutex::new(HashMap::new()),
        safe_point_seq: AtomicU64::new(0),
        safe_point_waiters: Mutex::new(Vec::new()),
        start_time: Instant::now(),
        closed: parking_lot::RwLock::new(false),
    }))
}

impl Database {
    pub fn lock_admin(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.admin_mu.lock()
    }

    pub fn set_state(&self, state: impl Into<String>) {
        *self.state.write() = state.into();
    }

    pub fn get_state(&self) -> String {
        self.state.read().clone()
    }

    pub fn min_replicas(&self) -> i32 {
        *self.min_replicas.lock()
    }

    pub fn set_min_replicas(&self, n: i32) {
        *self.min_replicas.lock() = n;
    }

    pub fn durable_offset(&self) -> u64 {
        self.inner.durable_offset()
    }

    pub fn last_log_offset(&self) -> u64 {
        self.inner.write_offset()
    }

    pub fn read_log_range(
        &self,
        start: i64,
        max_bytes: i64,
    ) -> Result<(Vec<u8>, i64), EngineError> {
        self.inner.read_log_range(start, max_bytes)
    }

    pub fn is_valid_replication_cursor(&self, offset: u64) -> bool {
        offset <= self.last_log_offset()
    }

    pub fn new_transaction(&self, update: bool) -> Transaction {
        self.inner.new_transaction(update)
    }

    pub fn wait_for_quorum(
        &self,
        _offset: u64,
        _timeout: Duration,
        _cancel: Option<&std::sync::mpsc::Receiver<()>>,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    pub fn register_replica_hello(
        &self,
        id: impl Into<String>,
        offset: u64,
        role: impl Into<String>,
    ) -> (u64, u64) {
        let id = id.into();
        let mut reps = self.replicas.lock();
        let gen = reps.len() as u64 + 1;
        let (kill_tx, kill_rx) = std::sync::mpsc::channel();
        reps.insert(
            id,
            ReplicaSlot {
                offset,
                role: role.into(),
                connected: true,
                gen,
                kill_tx: Some(kill_tx),
                kill_rx: Some(kill_rx),
            },
        );
        (offset, gen)
    }

    pub fn unregister_replica_gen(&self, id: &str, gen: u64) {
        let mut reps = self.replicas.lock();
        if let Some(slot) = reps.get(id) {
            if slot.gen == gen {
                reps.remove(id);
            }
        }
    }

    pub fn update_replica_offset(&self, id: &str, offset: u64) {
        if let Some(slot) = self.replicas.lock().get_mut(id) {
            slot.offset = offset;
        }
    }

    pub fn min_replica_offset(&self) -> u64 {
        let reps = self.replicas.lock();
        reps.values()
            .map(|s| s.offset)
            .min()
            .unwrap_or(u64::MAX)
    }

    pub fn safe_point_seq(&self) -> u64 {
        self.safe_point_seq.load(Ordering::Acquire)
    }

    pub fn trigger_safe_point(&self) {
        self.safe_point_seq.fetch_add(1, Ordering::AcqRel);
        for tx in self.safe_point_waiters.lock().drain(..) {
            let _ = tx.send(());
        }
    }

    pub fn safe_point_signal(&self) -> std::sync::mpsc::Receiver<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.safe_point_waiters.lock().push(tx);
        rx
    }

    pub fn get_replica_signal_channel(&self, id: &str) -> Option<std::sync::mpsc::Receiver<()>> {
        self.replicas
            .lock()
            .get_mut(id)
            .and_then(|s| s.kill_rx.take())
    }

    pub fn stats(&self) -> Stats {
        Stats {
            active_txs: 0,
            uptime: format!("{:?}", self.start_time.elapsed()),
            offset: self.inner.durable_offset() as i64,
            conflicts: self.inner.conflicts(),
            replica_lag: 0,
            log_size: self.inner.write_offset() as i64,
            log_allocated: self.inner.write_offset() as i64,
            key_count: self.inner.key_count(),
            replicas: self
                .replicas
                .lock()
                .iter()
                .map(|(id, s)| ReplicaInfo {
                    id: id.clone(),
                    role: s.role.clone(),
                    connected: s.connected,
                    offset: s.offset,
                    lag: 0,
                    last_seen: format!("{:?}", SystemTime::now()),
                })
                .collect(),
        }
    }

    pub fn close(&self) -> Result<(), DatabaseError> {
        *self.closed.write() = true;
        self.inner.close()?;
        Ok(())
    }

    pub fn reset(&self) -> Result<(), DatabaseError> {
        Err(DatabaseError::Other("reset not implemented in Rust port yet".into()))
    }

    pub fn promote(&self) -> Result<(), DatabaseError> {
        self.set_state(STATE_PRIMARY);
        Ok(())
    }

    pub fn wait_for_active_transactions(&self, _timeout: Duration) -> Result<(), DatabaseError> {
        Ok(())
    }

    pub fn abort_all_active_write_transactions(&self) {}

    pub fn wait_for_replication(&self, _timeout: Duration) -> Result<(), DatabaseError> {
        Ok(())
    }

    pub fn reset_replicas(&self) {
        self.replicas.lock().clear();
    }
}

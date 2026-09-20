// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::types::{EngineError, IndexVersion, RecordType, Snapshot, TxStatus};
use crate::Db;

/// Go-compatible transaction handle (`Put` / `Commit` / `Discard`).
pub struct Transaction(TxnHandle);

impl Transaction {
    pub fn from_handle(inner: TxnHandle) -> Self {
        Self(inner)
    }
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.0.put(key, value)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.0.delete(key)
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, EngineError> {
        self.0.get(key)
    }

    pub fn commit(&mut self) -> Result<(), EngineError> {
        self.0.commit()
    }

    pub fn discard(&mut self) {
        self.0.discard()
    }
}

pub struct WriteTransaction {
    pub(crate) db: Arc<Db>,
    pub xid: u64,
    pub snapshot: Snapshot,
    inner: Mutex<TxnInner>,
    aborted: AtomicBool,
    finished: AtomicBool,
}

pub enum TxnHandle {
    Write(Arc<WriteTransaction>, u64),
    Read(Arc<Db>, Snapshot, u64),
    Closed,
}

impl TxnHandle {
    pub fn closed() -> Self {
        Self::Closed
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        match self {
            Self::Write(tx, _) => tx.put(key, value),
            _ => Err(EngineError::Other("read-only transaction".into())),
        }
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        match self {
            Self::Write(tx, _) => tx.delete(key),
            _ => Err(EngineError::Other("read-only transaction".into())),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, EngineError> {
        match self {
            Self::Write(tx, _) => tx.get(key),
            Self::Read(db, snap, _) => db.read_snapshot(key, snap),
            Self::Closed => Err(EngineError::TxnFinished),
        }
    }

    pub fn commit(&mut self) -> Result<(), EngineError> {
        match self {
            Self::Write(tx, token) => {
                let res = tx.commit();
                tx.db.unregister_active_txn(*token);
                res
            }
            Self::Read(db, _, token) => {
                db.unregister_active_txn(*token);
                Ok(())
            }
            Self::Closed => Err(EngineError::TxnFinished),
        }
    }

    pub fn discard(&mut self) {
        match self {
            Self::Write(tx, token) => {
                tx.discard();
                tx.db.unregister_active_txn(*token);
            }
            Self::Read(db, _, token) => {
                db.unregister_active_txn(*token);
            }
            Self::Closed => {}
        }
        *self = Self::Closed;
    }
}

struct TxnInner {
    key_locks: HashSet<String>,
    disposition_seen: HashMap<String, bool>,
    key_delta: i64,
    read_set: HashSet<String>,
    current_size: i64,
    did_write: bool,
}

impl WriteTransaction {
    pub fn new_write(db: Arc<Db>, xid: u64, snapshot: Snapshot) -> Self {
        Self {
            db,
            xid,
            snapshot,
            inner: Mutex::new(TxnInner {
                key_locks: HashSet::new(),
                disposition_seen: HashMap::new(),
                key_delta: 0,
                read_set: HashSet::new(),
                current_size: 0,
                did_write: false,
            }),
            aborted: AtomicBool::new(false),
            finished: AtomicBool::new(false),
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.write(key, value, false)
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), EngineError> {
        self.write(key, &[], true)
    }

    fn write(&self, key: &[u8], value: &[u8], is_delete: bool) -> Result<(), EngineError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(EngineError::TxnFinished);
        }
        if self.aborted.load(Ordering::Acquire) {
            return Err(EngineError::WriteConflict);
        }
        if key.is_empty() {
            return Err(EngineError::Other("empty key".into()));
        }
        let key_str = String::from_utf8_lossy(key).into_owned();
        let entry_size = (key.len() + value.len() + 9) as i64;
        let mut inner = self.inner.lock();
        if inner.current_size + entry_size > turnstone_protocol::MAX_TX_SIZE as i64 {
            return Err(EngineError::Other(format!(
                "transaction size exceeds limit {}",
                turnstone_protocol::MAX_TX_SIZE
            )));
        }

        {
            let locks = self.db.key_locks.lock();
            if let Some(owner) = locks.get(&key_str) {
                if *owner != self.xid {
                    drop(locks);
                    self.db
                        .metrics_conflicts
                        .fetch_add(1, Ordering::AcqRel);
                    self.aborted.store(true, Ordering::Release);
                    return Err(EngineError::WriteConflict);
                }
            }
        }
        let first_write = !inner.key_locks.contains(&key_str);
        if first_write {
            self.db.key_locks.lock().insert(key_str.clone(), self.xid);
            inner.key_locks.insert(key_str.clone());
            let (ver, xmin, found) = self.db.index.latest_resolved(
                key,
                self.xid,
                |x| self.db.clog_status(x),
            );
            if found && (xmin >= self.snapshot.xmax || self.snapshot.contains(xmin)) {
                self.release_key_lock(&key_str, &inner);
                self.db
                    .metrics_conflicts
                    .fetch_add(1, Ordering::AcqRel);
                self.aborted.store(true, Ordering::Release);
                return Err(EngineError::WriteConflict);
            }
            Self::record_baseline(&mut inner, &self.db, &key_str, ver.as_ref(), is_delete);
        } else {
            Self::record_own(&mut inner, &key_str, is_delete);
        }

        let rec_type = if is_delete {
            RecordType::Delete
        } else {
            RecordType::Set
        };
        let offset = self.db.append_record(rec_type, self.xid, key, value)?;
        inner.did_write = true;
        self.db.index.put(
            key,
            IndexVersion {
                offset,
                value_len: value.len() as u32,
                xmin: self.xid,
                tombstone: is_delete,
            },
        )?;
        if !is_delete {
            self.db.cache_value(offset, value);
        }
        inner.current_size += entry_size;
        Ok(())
    }

    fn record_baseline(
        inner: &mut TxnInner,
        db: &Db,
        key_str: &str,
        ver: Option<&IndexVersion>,
        is_delete: bool,
    ) {
        let was_live = ver
            .filter(|v| !v.tombstone && db.clog_status(v.xmin) == TxStatus::Committed)
            .is_some();
        if is_delete {
            if was_live {
                inner.key_delta -= 1;
            }
        } else if !was_live {
            inner.key_delta += 1;
        }
        inner.disposition_seen.insert(key_str.to_string(), !is_delete);
    }

    fn record_own(inner: &mut TxnInner, key_str: &str, is_delete: bool) {
        let was_live = inner.disposition_seen.get(key_str).copied().unwrap_or(false);
        if is_delete {
            if was_live {
                inner.key_delta -= 1;
            }
        } else if !was_live {
            inner.key_delta += 1;
        }
        inner.disposition_seen.insert(key_str.to_string(), !is_delete);
    }

    fn release_key_lock(&self, key: &str, inner: &TxnInner) {
        let mut locks = self.db.key_locks.lock();
        if locks.get(key) == Some(&self.xid) {
            locks.remove(key);
        }
        let _ = inner;
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, EngineError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(EngineError::TxnFinished);
        }
        if self.aborted.load(Ordering::Acquire) {
            return Err(EngineError::WriteConflict);
        }
        self.inner
            .lock()
            .read_set
            .insert(String::from_utf8_lossy(key).into_owned());
        self.get_visible(key)
    }

    fn get_visible(&self, key: &[u8]) -> Result<Vec<u8>, EngineError> {
        let (ver, ok) = self.db.index.get_visible(
            key,
            &self.snapshot,
            self.xid,
            true,
            |xmin, snap| self.db.is_visible(xmin, snap),
        );
        if !ok {
            return Err(EngineError::KeyNotFound);
        }
        let ver = ver.unwrap();
        if ver.tombstone {
            return Err(EngineError::KeyNotFound);
        }
        if let Some(v) = self.db.cached_value(ver.offset) {
            return Ok(v);
        }
        let val = self.db.log.read_value_at(ver.offset, ver.value_len)?;
        self.db.cache_value(ver.offset, &val);
        Ok(val)
    }

    pub fn commit(&self) -> Result<(), EngineError> {
        if self.finished.swap(true, Ordering::AcqRel) {
            return Err(EngineError::TxnFinished);
        }
        if self.aborted.load(Ordering::Acquire) {
            self.force_abort();
            return Err(EngineError::WriteConflict);
        }
        let mut inner = self.inner.lock();
        if !inner.did_write || inner.key_locks.is_empty() {
            drop(inner);
            self.cleanup_xid(false);
            return Ok(());
        }

        for k in &inner.read_set {
            if self.db.index.has_newer_committed(
                k.as_bytes(),
                self.xid,
                &self.snapshot,
                |x| self.db.clog_status(x),
            ) {
                drop(inner);
                self.db
                    .metrics_conflicts
                    .fetch_add(1, Ordering::AcqRel);
                self.force_abort();
                return Err(EngineError::WriteConflict);
            }
        }

        let key_delta = inner.key_delta;
        drop(inner);

        let xid = self.xid;
        let sync = !self.db.unsafe_disable_fsync;
        self.db
            .log
            .append_records(
                &[|| {
                    crate::encode_record(&crate::types::Record {
                        ty: RecordType::Commit,
                        xid,
                        key: vec![],
                        value: vec![],
                    })
                }],
                sync,
            )?;

        self.db.clog.forget_clog(xid);
        self.db.clog.active_xids.write().unwrap().remove(&xid);
        if key_delta != 0 {
            self.db
                .key_count
                .fetch_add(key_delta, Ordering::AcqRel);
        }
        self.cleanup_xid(true);
        Ok(())
    }

    pub fn discard(&self) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        self.force_abort();
    }

    pub fn force_abort(&self) {
        let did_write = self.inner.lock().did_write;
        if !did_write {
            self.cleanup_xid(false);
            return;
        }
        let _ = self.db.append_record(RecordType::Abort, self.xid, &[], &[]);
        self.db.clog.set_clog(self.xid, TxStatus::Aborted);
        let _ = self.db.index.drop_xid(self.xid);
        self.cleanup_xid(false);
    }

    fn cleanup_xid(&self, committed: bool) {
        self.db.active_xids.lock().remove(&self.xid);
        self.db.begin_offsets.lock().remove(&self.xid);
        self.db.clog.active_xids.write().unwrap().remove(&self.xid);
        if !committed {
            self.db.clog.forget_clog(self.xid);
        }
        let keys: Vec<String> = self.inner.lock().key_locks.iter().cloned().collect();
        for k in keys {
            let mut locks = self.db.key_locks.lock();
            if locks.get(k.as_str()) == Some(&self.xid) {
                locks.remove(k.as_str());
            }
        }
    }
}

impl Db {
    pub(crate) fn read_snapshot(&self, key: &[u8], snap: &Snapshot) -> Result<Vec<u8>, EngineError> {
        let (ver, ok) = self.index.get_visible(
            key,
            snap,
            0,
            false,
            |xmin, s| self.is_visible(xmin, s),
        );
        if !ok {
            return Err(EngineError::KeyNotFound);
        }
        let ver = ver.unwrap();
        if ver.tombstone {
            return Err(EngineError::KeyNotFound);
        }
        if let Some(v) = self.cached_value(ver.offset) {
            return Ok(v);
        }
        let val = self.log.read_value_at(ver.offset, ver.value_len)?;
        self.cache_value(ver.offset, &val);
        Ok(val)
    }
}

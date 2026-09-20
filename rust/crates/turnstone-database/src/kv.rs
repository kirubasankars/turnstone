// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use turnstone_engine::EngineError;
use turnstone_protocol::{self as protocol, KeyNotFound};

use crate::{Database, DatabaseError, STATE_PRIMARY, STATE_STEPPING_DOWN};

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("key must contain only ASCII characters")]
    InvalidKey,
    #[error("database is not writable")]
    ReadOnlyDb,
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

impl From<KvError> for DatabaseError {
    fn from(e: KvError) -> Self {
        match e {
            KvError::Engine(e) => DatabaseError::Engine(e),
            KvError::Database(e) => e,
            KvError::InvalidKey | KvError::ReadOnlyDb => {
                DatabaseError::Other(e.to_string())
            }
        }
    }
}

impl Database {
    pub fn get(&self, key: &str) -> Result<Vec<u8>, KvError> {
        let db = self.engine.read();
        let mut tx = db.new_transaction(false);
        let val = tx.get(key.as_bytes()).map_err(|e| match e {
            EngineError::KeyNotFound => KvError::Engine(e),
            _ => KvError::Engine(e),
        })?;
        tx.discard();
        Ok(val)
    }

    pub fn get_protocol(&self, key: &str) -> Result<Vec<u8>, KeyNotFound> {
        self.get(key).map_err(|e| match e {
            KvError::Engine(EngineError::KeyNotFound) => KeyNotFound,
            _ => KeyNotFound,
        })
    }

    pub fn set(&self, key: &str, value: &[u8]) -> Result<(), KvError> {
        if !protocol::is_ascii(key) {
            return Err(KvError::InvalidKey);
        }
        let state = self.get_state();
        if state != STATE_PRIMARY && state != STATE_STEPPING_DOWN {
            return Err(KvError::ReadOnlyDb);
        }
        let db = self.engine.read();
        let mut tx = db.new_transaction(true);
        tx.put(key.as_bytes(), value)?;
        tx.commit()?;
        Ok(())
    }

    pub fn del(&self, key: &str) -> Result<(), KvError> {
        if !protocol::is_ascii(key) {
            return Err(KvError::InvalidKey);
        }
        let state = self.get_state();
        if state != STATE_PRIMARY && state != STATE_STEPPING_DOWN {
            return Err(KvError::ReadOnlyDb);
        }
        let db = self.engine.read();
        let mut tx = db.new_transaction(true);
        tx.delete(key.as_bytes())?;
        tx.commit()?;
        Ok(())
    }
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use turnstone_database::Database;

#[derive(Debug, Clone)]
pub struct Source {
    pub local_db: String,
    pub remote_db: String,
}

pub struct Manager {
    server_id: String,
    peers: Mutex<HashMap<String, Vec<Source>>>,
    stores: HashMap<String, Arc<Database>>,
    tls: Arc<rustls::ClientConfig>,
}

impl Manager {
    pub fn new(
        server_id: impl Into<String>,
        stores: HashMap<String, Arc<Database>>,
        tls: Arc<rustls::ClientConfig>,
    ) -> Self {
        Self {
            server_id: server_id.into(),
            peers: Mutex::new(HashMap::new()),
            stores,
            tls,
        }
    }

    pub fn start(&self) {}

    pub fn stop_all(&self) {
        self.peers.lock().clear();
    }

    pub fn is_following(&self, db_name: &str) -> bool {
        let peers = self.peers.lock();
        peers.values().any(|sources| {
            sources.iter().any(|s| s.local_db == db_name)
        })
    }

    pub fn source(&self, db_name: &str) -> (String, String) {
        let peers = self.peers.lock();
        for (addr, sources) in peers.iter() {
            for src in sources {
                if src.local_db == db_name {
                    return (addr.clone(), src.remote_db.clone());
                }
            }
        }
        (String::new(), String::new())
    }

    pub fn follow(
        &self,
        db_name: &str,
        source_addr: &str,
        source_db: &str,
    ) -> Result<(), String> {
        if !self.stores.contains_key(db_name) {
            return Err(format!("unknown local database {db_name}"));
        }
        let mut peers = self.peers.lock();
        peers
            .entry(source_addr.to_string())
            .or_default()
            .push(Source {
                local_db: db_name.to_string(),
                remote_db: source_db.to_string(),
            });
        Ok(())
    }
}

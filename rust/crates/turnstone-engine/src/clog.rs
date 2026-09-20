// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::types::{Snapshot, TxStatus};

/// Commit-log and active-transaction tracking (subset of Go `clog.go`).
#[derive(Default)]
pub struct ClogState {
    pub active_xids: RwLock<HashSet<u64>>,
    clog: RwLock<HashMap<u64, TxStatus>>,
}

impl ClogState {
    pub fn build_snapshot(&self, transaction_id: u64) -> Snapshot {
        let xmax = transaction_id + 1;
        let active = self.active_xids.read().unwrap();
        if active.is_empty() {
            return Snapshot {
                xmax,
                xip: HashMap::new(),
            };
        }
        let xip = active.iter().map(|&xid| (xid, true)).collect();
        Snapshot { xmax, xip }
    }

    pub fn clog_status(&self, xid: u64) -> TxStatus {
        if self.active_xids.read().unwrap().contains(&xid) {
            return TxStatus::InProgress;
        }
        if let Some(st) = self.clog.read().unwrap().get(&xid) {
            return *st;
        }
        TxStatus::Committed
    }

    pub fn set_clog(&self, xid: u64, status: TxStatus) {
        self.clog.write().unwrap().insert(xid, status);
    }

    pub fn forget_clog(&self, xid: u64) {
        self.clog.write().unwrap().remove(&xid);
    }

    pub fn clog_len(&self) -> usize {
        self.clog.read().unwrap().len()
    }

    pub fn is_visible(&self, xmin: u64, snap: &Snapshot) -> bool {
        if xmin >= snap.xmax || snap.contains(xmin) {
            return false;
        }
        self.clog_status(xmin) == TxStatus::Committed
    }
}

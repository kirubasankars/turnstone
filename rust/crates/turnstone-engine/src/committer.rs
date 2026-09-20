// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::db::transaction::WriteTransaction;
use crate::types::{EngineError, Record, RecordType};
use crate::Db;

pub(crate) const MAX_COMMIT_BATCH_SIZE: usize = 1024;

pub(crate) struct CommitRequest {
    pub tx: Arc<WriteTransaction>,
    pub resp: std::sync::mpsc::SyncSender<Result<(), EngineError>>,
}

impl Db {
    pub(crate) fn run_group_commits(
        self: &Arc<Self>,
        commit_rx: std::sync::mpsc::Receiver<CommitRequest>,
        shutdown: std::sync::mpsc::Receiver<()>,
    ) {
        let mut batch = Vec::with_capacity(64);
        loop {
            batch.clear();
            match commit_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(req) => batch.push(req),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if shutdown.try_recv().is_ok() {
                        break;
                    }
                    continue;
                }
            }

            if self.commit_delay() > Duration::ZERO
                && !self.unsafe_disable_fsync
                && batch.len() < self.commit_siblings as usize
            {
                let deadline = std::time::Instant::now() + self.commit_delay();
                while batch.len() < MAX_COMMIT_BATCH_SIZE && std::time::Instant::now() < deadline {
                    match commit_rx.recv_timeout(deadline - std::time::Instant::now()) {
                        Ok(req) => batch.push(req),
                        Err(_) => break,
                    }
                }
            }

            while batch.len() < MAX_COMMIT_BATCH_SIZE {
                match commit_rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            self.process_commit_batch(&batch);
        }
        for req in batch {
            let _ = req.resp.try_send(Err(EngineError::DatabaseClosed));
        }
        while let Ok(req) = commit_rx.try_recv() {
            let _ = req.resp.try_send(Err(EngineError::DatabaseClosed));
        }
    }

    pub(crate) fn process_commit_batch(&self, requests: &[CommitRequest]) {
        if requests.is_empty() {
            return;
        }
        if self.closed.load(Ordering::Acquire) {
            for req in requests {
                let _ = req.resp.try_send(Err(EngineError::DatabaseClosed));
            }
            return;
        }

        struct Outcome {
            tx: Arc<WriteTransaction>,
            err: Option<EngineError>,
            claimed: bool,
        }
        let mut outcomes = Vec::new();

        {
            let _guard = self.commit_mu.lock();
            if self.is_corrupt.load(Ordering::Acquire) {
                for req in requests {
                    outcomes.push(Outcome {
                        tx: req.tx.clone(),
                        err: Some(EngineError::DatabaseCorrupt),
                        claimed: false,
                    });
                }
            } else if self.is_disk_full.load(Ordering::Acquire) {
                for req in requests {
                    outcomes.push(Outcome {
                        tx: req.tx.clone(),
                        err: Some(EngineError::DiskFull),
                        claimed: false,
                    });
                }
            } else {
                let mut valid = Vec::new();
                let mut builders: Vec<Box<dyn Fn() -> Vec<u8> + Send>> = Vec::new();
                for req in requests {
                    let tx = &req.tx;
                    if let Err(e) = tx.check_read_set_conflicts() {
                        self.metrics_conflicts.fetch_add(1, Ordering::AcqRel);
                        outcomes.push(Outcome {
                            tx: tx.clone(),
                            err: Some(e),
                            claimed: false,
                        });
                        continue;
                    }
                    if !tx.claim_decision() {
                        outcomes.push(Outcome {
                            tx: tx.clone(),
                            err: Some(EngineError::WriteConflict),
                            claimed: false,
                        });
                        continue;
                    }
                    valid.push(tx.clone());
                    let xid = tx.xid;
                    builders.push(Box::new(move || {
                        crate::encode_record(&Record {
                            ty: RecordType::Commit,
                            xid,
                            key: vec![],
                            value: vec![],
                        })
                    }));
                }

                if !valid.is_empty() {
                    let payloads: Vec<Vec<u8>> = builders.iter().map(|b| b()).collect();
                    match self
                        .log
                        .append_encoded_batch(&payloads, !self.unsafe_disable_fsync)
                    {
                        Err(e) => {
                            self.is_corrupt.store(true, Ordering::Release);
                            for tx in valid {
                                let _ = self.index.drop_xid(tx.xid);
                                self.clog.set_clog(tx.xid, crate::types::TxStatus::Aborted);
                                outcomes.push(Outcome {
                                    tx,
                                    err: Some(e.clone()),
                                    claimed: true,
                                });
                            }
                        }
                        Ok(_offsets) => {
                            for tx in &valid {
                                self.clog.forget_clog(tx.xid);
                            }
                            let total_delta: i64 = valid.iter().map(|t| t.key_delta()).sum();
                            if total_delta != 0 {
                                self.key_count.fetch_add(total_delta, Ordering::AcqRel);
                            }
                            for tx in valid {
                                outcomes.push(Outcome {
                                    tx,
                                    err: None,
                                    claimed: true,
                                });
                            }
                        }
                    }
                }
            }
        }

        let mut results: std::collections::HashMap<u64, Result<(), EngineError>> =
            std::collections::HashMap::new();
        for o in outcomes {
            match (&o.err, o.claimed) {
                (None, _) => o.tx.release_committed_locks(),
                (Some(_), true) => o.tx.force_release_locks(),
                (Some(_), false) => {
                    o.tx.mark_aborted();
                    o.tx.force_abort();
                }
            }
            results.insert(
                o.tx.xid,
                o.err.map_or(Ok(()), Err),
            );
        }
        for req in requests {
            let r = results
                .get(&req.tx.xid)
                .cloned()
                .unwrap_or(Err(EngineError::DatabaseClosed));
            let _ = req.resp.send(r);
        }
    }
}

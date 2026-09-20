// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::hashindex::{Index, Version};
use crate::types::{EngineError, RecordType};
use crate::wal::DataLog;
use crate::clog::ClogState;

/// Replay WAL records into `index`, mirroring Go `replayLog`.
pub fn replay_log(
    log: &DataLog,
    index: &Index,
    clog: &ClogState,
    truncate_corrupt: bool,
    transaction_id: &AtomicU64,
) -> Result<(), EngineError> {
    let mut in_progress = std::collections::HashSet::new();
    let mut committed = std::collections::HashSet::new();

    let result = log.replay(truncate_corrupt, |rec, span| {
        let cur = transaction_id.load(Ordering::Acquire);
        if rec.xid > cur {
            transaction_id.store(rec.xid, Ordering::Release);
        }

        match rec.ty {
            RecordType::Begin => {
                if !committed.contains(&rec.xid) {
                    in_progress.insert(rec.xid);
                }
            }
            RecordType::Set => {
                if !committed.contains(&rec.xid) {
                    in_progress.insert(rec.xid);
                }
                let _ = index.put(
                    &rec.key,
                    Version {
                        offset: span.offset,
                        value_len: rec.value.len() as u32,
                        xmin: rec.xid,
                        tombstone: false,
                    },
                );
            }
            RecordType::Delete => {
                if !committed.contains(&rec.xid) {
                    in_progress.insert(rec.xid);
                }
                let _ = index.put(
                    &rec.key,
                    Version {
                        offset: span.offset,
                        value_len: 0,
                        xmin: rec.xid,
                        tombstone: true,
                    },
                );
            }
            RecordType::Commit => {
                in_progress.remove(&rec.xid);
                committed.insert(rec.xid);
                clog.forget_clog(rec.xid);
            }
            RecordType::Abort => {
                in_progress.remove(&rec.xid);
                let _ = index.drop_xid(rec.xid);
                clog.forget_clog(rec.xid);
            }
        }
    });

    if let Err(e) = result {
        if e != EngineError::Truncated {
            return Err(e);
        }
    }

    for xid in in_progress {
        let _ = index.drop_xid(xid);
        clog.forget_clog(xid);
    }

    Ok(())
}

/// Convenience helper for tests: fresh memory index + replay.
pub fn replay_into_mem(
    log: &DataLog,
    truncate_corrupt: bool,
) -> Result<(Index, ClogState, u64), EngineError> {
    let index = Index::new();
    let clog = ClogState::default();
    let txid = AtomicU64::new(0);
    replay_log(log, &index, &clog, truncate_corrupt, &txid)?;
    Ok((index, clog, txid.load(Ordering::Acquire)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encode_record;
    use crate::hashindex::IndexExt;
    use crate::types::Record;
    use tempfile::tempdir;

    #[test]
    fn replay_committed_set() {
        let dir = tempdir().unwrap();
        let log = DataLog::open(dir.path(), 4096).unwrap();
        let payload = encode_record(&Record {
            ty: RecordType::Begin,
            xid: 1,
            key: vec![],
            value: vec![],
        });
        log.append_encoded(&payload, false).unwrap();
        let payload = encode_record(&Record {
            ty: RecordType::Set,
            xid: 1,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        });
        log.append_encoded(&payload, false).unwrap();
        let payload = encode_record(&Record {
            ty: RecordType::Commit,
            xid: 1,
            key: vec![],
            value: vec![],
        });
        log.append_encoded(&payload, true).unwrap();

        let (index, clog, _) = replay_into_mem(&log, false).unwrap();
        let chain = index.get_chain(b"k");
        assert!(!chain.is_empty());
        assert_eq!(chain[0].value_len, 1);
        assert_eq!(
            index.live_key_count(|xid| clog.clog_status(xid)),
            1
        );
    }
}

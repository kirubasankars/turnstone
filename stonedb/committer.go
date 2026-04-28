package stonedb

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

// testingProcessCommitBatchErr allows tests to inject a system-level error
// before any WAL/clog work happens in a group-commit cycle.
var testingProcessCommitBatchErr error

// processCommitBatch is the group-commit worker. For each request, it
// re-validates the transaction's read set (linearizable with clog updates,
// since this whole function runs under commitMu), appends a COMMIT WAL
// record for every transaction that passes, group-fsyncs once for the whole
// batch, durably marks the clog, and then (outside commitMu) releases key
// locks / writes ABORT records for anything that failed validation.
func (db *DB) processCommitBatch(requests []commitRequest) {
	if len(requests) == 0 {
		return
	}

	type outcome struct {
		tx  *Transaction
		err error
	}
	var outcomes []outcome

	func() {
		db.commitMu.Lock()
		defer db.commitMu.Unlock()

		if testingProcessCommitBatchErr != nil {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, testingProcessCommitBatchErr})
			}
			return
		}
		if db.isDiskFull == 1 {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, ErrDiskFull})
			}
			return
		}
		if atomic.LoadInt32(&db.isCorrupt) == 1 {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, errors.New("database is corrupt")})
			}
			return
		}

		start := time.Now()

		var valid []*Transaction
		var builders []func(uint64) []byte
		for _, req := range requests {
			tx := req.tx
			if err := tx.checkReadSetConflicts(); err != nil {
				atomic.AddUint64(&db.metricsConflicts, 1)
				outcomes = append(outcomes, outcome{tx, err})
				continue
			}
			valid = append(valid, tx)
			xid := tx.xid
			builders = append(builders, func(opID uint64) []byte {
				return encodeWALRecord(WALRecord{Type: WALRecordCommit, XID: xid, OpID: opID})
			})
		}

		if len(valid) == 0 {
			return
		}

		nextOpID := func() uint64 { return atomic.AddUint64(&db.operationID, 1) }
		_, err := db.writeAheadLog.AppendRecordsWithOpIDs(nextOpID, builders, true)
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			db.logger.Error("CRITICAL: WAL commit-group fsync failed. Database entering CORRUPT state.", "err", err)
			for _, tx := range valid {
				outcomes = append(outcomes, outcome{tx, err})
			}
			return
		}

		clogBatch := new(leveldb.Batch)
		for _, tx := range valid {
			clogBatch.Put(encodeClogKey(tx.xid), []byte{byte(TxCommitted)})
		}
		if err := db.ldb.Write(clogBatch, nil); err != nil {
			// The WAL already durably says these are committed; the index
			// must agree. Crash to force a clog rebuild on restart rather
			// than serve inconsistent reads.
			panic("CRITICAL: clog commit persist failed after WAL commit fsync: " + err.Error())
		}

		// Apply key-count / garbage accounting now that commit is durable.
		var totalDelta int64
		staleBytes := make(map[uint32]int64)
		for _, tx := range valid {
			totalDelta += tx.keyDelta
			for fid, sz := range tx.staleBytes {
				staleBytes[fid] += sz
			}
		}
		if totalDelta != 0 {
			atomic.AddInt64(&db.keyCount, totalDelta)
		}
		if len(staleBytes) > 0 {
			db.mu.Lock()
			for fid, sz := range staleBytes {
				db.deletedBytesByFile[fid] += sz
			}
			db.mu.Unlock()
		}

		for _, tx := range valid {
			outcomes = append(outcomes, outcome{tx, nil})
		}

		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			db.logger.Warn("Slow group commit", "count", len(valid), "duration", duration)
		}
	}()

	// Release locks / write ABORT records for failed transactions, and
	// release locks for committed ones, outside commitMu.
	for _, o := range outcomes {
		if o.err != nil {
			o.tx.markAborted()
			db.abortTransaction(o.tx)
		} else {
			db.releaseCommittedLocks(o.tx)
		}
	}

	byTx := make(map[*Transaction]error, len(outcomes))
	for _, o := range outcomes {
		byTx[o.tx] = o.err
	}
	for _, req := range requests {
		req.resp <- byTx[req.tx]
	}
}

// releaseCommittedLocks drops a committed transaction's key locks and
// bookkeeping (mirrors abortTransaction's cleanup, minus the ABORT record).
func (db *DB) releaseCommittedLocks(tx *Transaction) {
	db.txMu.Lock()
	delete(db.activeXids, tx.xid)
	delete(db.beginOpIDs, tx.xid)
	delete(db.txStartTimes, tx.xid)
	for k := range tx.keyLocks {
		if owner, ok := db.keyLocks[k]; ok && owner == tx.xid {
			delete(db.keyLocks, k)
		}
	}
	db.txMu.Unlock()
}

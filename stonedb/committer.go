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
		tx      *Transaction
		err     error
		claimed bool // true if this tx won tx.claimDecision() in this batch
	}
	var outcomes []outcome

	func() {
		db.commitMu.Lock()
		defer db.commitMu.Unlock()

		if testingProcessCommitBatchErr != nil {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, testingProcessCommitBatchErr, false})
			}
			return
		}
		if atomic.LoadInt32(&db.isDiskFull) == 1 {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, ErrDiskFull, false})
			}
			return
		}
		if atomic.LoadInt32(&db.isCorrupt) == 1 {
			for _, req := range requests {
				outcomes = append(outcomes, outcome{req.tx, errors.New("database is corrupt"), false})
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
				outcomes = append(outcomes, outcome{tx, err, false})
				continue
			}
			// Claim the sole right to decide this xid's outcome before
			// committing it. If this fails, the liveness reaper already
			// claimed and durably aborted this xid (e.g. it timed out
			// while queued here) -- we must not also commit it, which
			// would produce both an ABORT and a COMMIT record for the same
			// xid. Report the same error the client would have seen had it
			// discarded the transaction itself.
			if !tx.claimDecision() {
				outcomes = append(outcomes, outcome{tx, ErrWriteConflict, false})
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
		_, err := db.writeAheadLog.AppendRecordsWithOpIDs(nextOpID, builders, !db.unsafeDisableFsync)
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			db.logger.Error("CRITICAL: WAL commit-group fsync failed. Database entering CORRUPT state.", "err", err)
			// Nothing was made durable, so it's safe to just release these
			// transactions' in-memory bookkeeping. We deliberately do not
			// route them through abortTransaction: they already won
			// claimDecision above (so a second attempt to claim it there
			// would just no-op and leak their locks), and WAL writes are
			// already known to be failing here, so trying to append an
			// ABORT record would only fail again.
			for _, tx := range valid {
				outcomes = append(outcomes, outcome{tx, err, true})
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
			outcomes = append(outcomes, outcome{tx, nil, true})
		}

		duration := time.Since(start)
		if duration > 100*time.Millisecond {
			db.logger.Warn("Slow group commit", "count", len(valid), "duration", duration)
		}
	}()

	// Release locks / write ABORT records for failed transactions, and
	// release locks for committed ones, outside commitMu.
	for _, o := range outcomes {
		switch {
		case o.err == nil:
			db.releaseCommittedLocks(o.tx)
		case o.claimed:
			// Already durably resolved (or a WAL failure already left
			// nothing durable to protect) as part of this batch; just drop
			// the in-memory bookkeeping without another WAL/clog attempt.
			db.forceReleaseLocks(o.tx)
		default:
			o.tx.markAborted()
			db.abortTransaction(o.tx)
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

// forceReleaseLocks drops tx's key locks and bookkeeping without attempting
// any further WAL/clog write. Used only for transactions that already won
// tx.claimDecision() as part of a commit batch that then failed at the WAL
// level (nothing was made durable, and the WAL is already known to be
// failing, so a follow-up ABORT append would only fail again).
func (db *DB) forceReleaseLocks(tx *Transaction) {
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

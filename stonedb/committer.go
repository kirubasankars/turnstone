// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"errors"
	"sync/atomic"
	"time"
)

var testingProcessCommitBatchErr error

func (db *DB) processCommitBatch(requests []commitRequest) {
	if len(requests) == 0 {
		return
	}
	if atomic.LoadInt32(&db.closed) == 1 {
		for _, req := range requests {
			req.resp <- ErrDatabaseClosed
		}
		return
	}

	type outcome struct {
		tx      *Transaction
		err     error
		claimed bool
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

		var valid []*Transaction
		var builders []func(uint64) []byte
		for _, req := range requests {
			tx := req.tx
			if err := tx.checkReadSetConflicts(); err != nil {
				atomic.AddUint64(&db.metricsConflicts, 1)
				outcomes = append(outcomes, outcome{tx, err, false})
				continue
			}
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
		_, _, err := db.log.AppendRecordsWithOpIDs(nextOpID, builders, !db.unsafeDisableFsync)
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			for _, tx := range valid {
				outcomes = append(outcomes, outcome{tx, err, true})
			}
			return
		}

		for _, tx := range valid {
			db.forgetClog(tx.xid)
		}

		var totalDelta int64
		for _, tx := range valid {
			totalDelta += tx.keyDelta
		}
		if totalDelta != 0 {
			atomic.AddInt64(&db.keyCount, totalDelta)
		}

		for _, tx := range valid {
			outcomes = append(outcomes, outcome{tx, nil, true})
		}

		if d := time.Since(time.Now()); d > 100*time.Millisecond {
			_ = d
		}
	}()

	for _, o := range outcomes {
		switch {
		case o.err == nil:
			db.releaseCommittedLocks(o.tx)
		case o.claimed:
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

func (db *DB) forceReleaseLocks(tx *Transaction) {
	db.releaseCommittedLocks(tx)
}

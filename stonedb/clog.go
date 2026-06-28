// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"sync/atomic"
)

func (db *DB) buildSnapshotLocked() Snapshot {
	xip := make(map[uint64]bool, len(db.activeXids))
	for xid := range db.activeXids {
		xip[xid] = true
	}
	xmax := atomic.LoadUint64(&db.transactionID) + 1
	return Snapshot{Xmax: xmax, Xip: xip}
}

func (db *DB) clogStatus(xid uint64) TxStatus {
	db.txMu.Lock()
	_, inProgress := db.activeXids[xid]
	db.txMu.Unlock()
	if inProgress {
		return TxInProgress
	}
	db.clogMu.RLock()
	st, ok := db.clog[xid]
	db.clogMu.RUnlock()
	if !ok {
		return TxCommitted
	}
	return st
}

func (db *DB) setClog(xid uint64, status TxStatus) {
	db.clogMu.Lock()
	db.clog[xid] = status
	db.clogMu.Unlock()
}

func (db *DB) forgetClog(xid uint64) {
	db.clogMu.Lock()
	delete(db.clog, xid)
	db.clogMu.Unlock()
}

func (db *DB) isVisible(xmin uint64, snap Snapshot) bool {
	if xmin >= snap.Xmax || snap.contains(xmin) {
		return false
	}
	return db.clogStatus(xmin) == TxCommitted
}

func (db *DB) abortTransaction(tx *Transaction) {
	if tx.xid == 0 {
		return
	}

	tx.abortOnce.Do(func() {
		if !tx.claimDecision() {
			return
		}

		db.txMu.Lock()
		_, stillActive := db.activeXids[tx.xid]
		db.txMu.Unlock()
		if !stillActive {
			return
		}

		if _, err := db.appendRecord(WALRecordAbort, tx.xid, nil, nil); err != nil {
			panic("CRITICAL: ABORT record append failed: " + err.Error())
		}
		db.setClog(tx.xid, TxAborted)
		db.index.DropXid(tx.xid)

		db.txMu.Lock()
		delete(db.activeXids, tx.xid)
		delete(db.beginOffsets, tx.xid)
		delete(db.txStartTimes, tx.xid)
		for k := range tx.keyLocks {
			if o, ok := db.keyLocks[k]; ok && o == tx.xid {
				delete(db.keyLocks, k)
			}
		}
		db.txMu.Unlock()
		db.forgetClog(tx.xid)
	})
}

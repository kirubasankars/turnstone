package stonedb

import (
	"math"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
)

// buildSnapshotLocked captures a Postgres-style snapshot. Callers must hold
// db.txMu. xmax should already reflect the caller's own xid (if any) via the
// global counter having been incremented before this is called; xip is the
// set of xids that are concurrently in-progress at this instant.
func (db *DB) buildSnapshotLocked() Snapshot {
	xip := make(map[uint64]bool, len(db.activeXids))
	for xid := range db.activeXids {
		xip[xid] = true
	}
	xmax := atomic.LoadUint64(&db.transactionID) + 1
	return Snapshot{Xmax: xmax, Xip: xip}
}

// clogStatus resolves the commit-log state of xid: in-progress transactions
// are tracked in memory (local or replicated); resolved ones are looked up
// from the durable per-xid clog key in LevelDB. A missing durable entry is
// only possible after an emergency index rebuild from the ValueLog (which
// has no independent notion of "aborted"); in that case we default to
// Committed, matching the ValueLog's role as ground truth for that recovery
// path.
func (db *DB) clogStatus(xid uint64) TxStatus {
	db.txMu.Lock()
	_, inProgress := db.activeXids[xid]
	db.txMu.Unlock()
	if inProgress {
		return TxInProgress
	}

	if db.ldb == nil {
		return TxCommitted
	}
	val, err := db.ldb.Get(encodeClogKey(xid), nil)
	if err != nil || len(val) == 0 {
		return TxCommitted
	}
	return TxStatus(val[0])
}

func (db *DB) isVisible(xmin uint64, snap Snapshot) bool {
	if xmin >= snap.Xmax || snap.contains(xmin) {
		return false
	}
	return db.clogStatus(xmin) == TxCommitted
}

// minActiveSnapshotXmax returns the vacuum horizon: the smallest snapshot
// boundary among currently open transactions (read-only or RW). Compaction
// must never reclaim a committed version that is still the newest version
// visible below this horizon.
func (db *DB) minActiveSnapshotXmax() uint64 {
	db.activeTxnsMu.Lock()
	defer db.activeTxnsMu.Unlock()
	if len(db.activeTxns) == 0 {
		return atomic.LoadUint64(&db.transactionID) + 1
	}
	min := uint64(math.MaxUint64)
	for _, xmax := range db.activeTxns {
		if xmax < min {
			min = xmax
		}
	}
	return min
}

// abortTransaction writes an ABORT WAL record (if the tx ever got a BEGIN),
// persists the aborted clog decision, and only then releases the
// transaction's key locks and bookkeeping. Safe to call more than once; a
// no-op after the first call for a given tx (guarded by tx.abortOnce).
//
// Ordering matters: xid must stay in db.activeXids (so clogStatus reports
// TxInProgress, not its "missing clog entry" TxCommitted fallback) and the
// key locks must stay held until the ABORT clog entry is durable. Otherwise
// a concurrent transaction could acquire the freed key lock and, via
// clogStatus's committed-by-default fallback for an xid with no durable
// clog entry yet, treat our about-to-be-aborted write as a committed base
// value -- silently laundering an aborted write into the committed history.
func (db *DB) abortTransaction(tx *Transaction) {
	if tx.xid == 0 {
		return
	}

	tx.abortOnce.Do(func() {
		// Claim the sole right to decide this xid's durable outcome. If
		// this fails, processCommitBatch already claimed it (it is
		// currently committing, or has already committed, this same tx) --
		// we must not write a conflicting ABORT record or touch its locks;
		// the committer is the sole source of truth for this xid now and
		// will release its locks itself. Without this, the liveness reaper
		// racing an in-flight Commit() could write both an ABORT and a
		// COMMIT record for the same xid, and could also iterate
		// tx.keyLocks concurrently with the owning goroutine's own
		// unsynchronized mutation of it.
		if !tx.claimDecision() {
			return
		}

		db.txMu.Lock()
		_, stillActive := db.activeXids[tx.xid]
		db.txMu.Unlock()
		if !stillActive {
			// Already resolved by another path (e.g. crash recovery already
			// decided this xid before this Transaction handle got a chance
			// to run its own abort).
			return
		}

		if _, err := db.appendRecord(WALRecordAbort, tx.xid, nil, nil); err != nil {
			// The WAL is the source of truth for whether this xid is
			// resolved. If we cannot durably record the ABORT decision but
			// still proceed to release locks/remove it from activeXids
			// below, a concurrent transaction could observe clogStatus's
			// "no entry found -> assume committed" fallback and silently
			// treat this aborted write as committed history. Crash instead,
			// mirroring processCommitBatch's symmetric panic on a failed
			// commit persist -- forcing a clog rebuild from the WAL on
			// restart rather than serving an inconsistent decision.
			panic("CRITICAL: ABORT record append failed. Database entering unrecoverable state to avoid laundering an aborted write into committed history: " + err.Error())
		}
		if db.ldb != nil {
			if err := db.ldb.Put(encodeClogKey(tx.xid), []byte{byte(TxAborted)}, nil); err != nil {
				panic("CRITICAL: aborted clog entry persist failed after WAL abort append: " + err.Error())
			}
		}

		db.txMu.Lock()
		delete(db.activeXids, tx.xid)
		delete(db.beginOpIDs, tx.xid)
		delete(db.txStartTimes, tx.xid)
		for k := range tx.keyLocks {
			if o, ok := db.keyLocks[k]; ok && o == tx.xid {
				delete(db.keyLocks, k)
			}
		}
		db.txMu.Unlock()
	})
}

// persistClogRebuild flushes the clog decisions reconstructed from the WAL
// during recovery (syncWALToValueLog) into the now-open LevelDB index.
func (db *DB) persistClogRebuild() error {
	if len(db.pendingClogRebuild) == 0 || db.ldb == nil {
		db.pendingClogRebuild = nil
		return nil
	}
	batch := new(leveldb.Batch)
	for xid, status := range db.pendingClogRebuild {
		final := status
		if final == TxInProgress {
			// Crashed mid-transaction: no COMMIT/ABORT record was ever written.
			final = TxAborted
		}
		batch.Put(encodeClogKey(xid), []byte{byte(final)})
	}
	db.pendingClogRebuild = nil
	if batch.Len() == 0 {
		return nil
	}
	return db.ldb.Write(batch, nil)
}

package stonedb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"turnstone/protocol"

	"turnstone/stonedb/index"
)

// Transaction represents a running transaction. Writes are applied eagerly
// (WAL + VLog + index) as soon as Put/Delete is called, Postgres-style;
// Commit only decides visibility (COMMIT WAL record + clog).
type Transaction struct {
	db     *DB
	update bool

	// RW-only state
	xid             uint64
	beginOpID       uint64
	beginErr        error
	keyLocks        map[string]struct{}   // keys this tx currently holds the write lock on
	dispositionSeen map[string]bool       // key -> "live" disposition as of our last write to it this tx
	ownPriorMeta    map[string]*EntryMeta // key -> our own previous write's meta this tx (for stale-byte accounting)
	staleBytes      map[uint32]int64      // fileID -> bytes made stale by this tx's writes
	keyDelta        int64                 // net live-key count delta contributed by this tx
	readSet         map[string]struct{}   // keys read, validated against clog at commit

	snapshot Snapshot
	// snapOpID is the operationID watermark captured atomically alongside
	// snapshot.Xmax (under db.txMu) at transaction start, for read-only
	// transactions. It always reflects the exact same instant as the
	// snapshot boundary, unlike a separately-timed re-read of db.operationID.
	snapOpID uint64
	// aborted is read/written from multiple goroutines (the owning
	// goroutine via write()/Get()/Commit(), and the background liveness
	// reaper via markAborted()), so it must be accessed atomically.
	aborted   int32
	finished  bool
	abortOnce sync.Once // guards db.abortTransaction's one-time WAL/clog/lock-release work

	// decided is the single arbitration point between the commit path
	// (processCommitBatch) and the abort path (abortTransaction, invoked by
	// Discard or the liveness reaper) for this xid's durable outcome.
	// Whichever side wins the CompareAndSwap(0, 1) is the sole writer of
	// the COMMIT/ABORT WAL record and clog entry; the loser must not touch
	// WAL/clog/locks for this xid at all. Without this, the reaper and the
	// committer could independently decide the same xid, producing both a
	// COMMIT and an ABORT record for it.
	decided int32

	currentSize int64 // accumulated size of keys/values + overhead, for MaxTxSize
}

// isAborted reports whether the transaction has been flagged aborted, safe
// to call concurrently from any goroutine.
func (tx *Transaction) isAborted() bool {
	return atomic.LoadInt32(&tx.aborted) == 1
}

// markAborted flags the transaction as unusable for further operations. The
// actual ABORT WAL record / clog write happens in db.abortTransaction, called
// either here inline (on conflict) or from Discard/the liveness reaper. Safe
// to call concurrently from any goroutine.
func (tx *Transaction) markAborted() {
	atomic.StoreInt32(&tx.aborted, 1)
}

// claimDecision atomically claims the right to durably decide (commit or
// abort) this transaction's xid. Only the first caller (across both the
// commit path and the abort path) gets true; every other caller must back
// off without writing any WAL/clog record or touching key locks for this
// xid, since the winner is now the sole source of truth for its outcome.
func (tx *Transaction) claimDecision() bool {
	return atomic.CompareAndSwapInt32(&tx.decided, 0, 1)
}

// Discard aborts (if RW and not already finished) and releases resources.
// Safe to call multiple times, and safe to call after a successful Commit
// (no-op in that case).
func (tx *Transaction) Discard() {
	if tx.finished {
		return
	}
	tx.finished = true

	tx.db.activeTxnsMu.Lock()
	delete(tx.db.activeTxns, tx)
	tx.db.activeTxnsMu.Unlock()

	if tx.update && tx.xid != 0 {
		tx.db.abortTransaction(tx)
	}
}

func (tx *Transaction) Put(key, value []byte) error {
	return tx.write(key, value, false)
}

func (tx *Transaction) Delete(key []byte) error {
	return tx.write(key, nil, true)
}

func (tx *Transaction) write(key, value []byte, isDelete bool) error {
	if !tx.update {
		return errors.New("cannot write in read-only transaction")
	}
	if tx.finished {
		return ErrTxnFinished
	}
	if tx.beginErr != nil {
		return tx.beginErr
	}
	if tx.isAborted() {
		return ErrWriteConflict
	}
	db := tx.db
	if atomic.LoadInt32(&db.isDiskFull) == 1 {
		return ErrDiskFull
	}
	if len(key) == 0 {
		return errors.New("empty key")
	}

	entrySize := int64(len(key) + len(value) + 9)
	if tx.currentSize+entrySize > int64(protocol.MaxTxSize) {
		return fmt.Errorf("transaction size exceeds limit %d", protocol.MaxTxSize)
	}

	keyStr := string(key)

	// First-writer-wins, NOWAIT: acquire (or confirm we already own) the key lock.
	db.txMu.Lock()
	if owner, locked := db.keyLocks[keyStr]; locked && owner != tx.xid {
		db.txMu.Unlock()
		atomic.AddUint64(&db.metricsConflicts, 1)
		tx.markAborted()
		return ErrWriteConflict
	}
	firstWriteToKey := false
	if _, owned := tx.keyLocks[keyStr]; !owned {
		db.keyLocks[keyStr] = tx.xid
		tx.keyLocks[keyStr] = struct{}{}
		firstWriteToKey = true
	}
	db.txMu.Unlock()

	// Snapshot the accounting state for this key so a failed WAL/VLog/index
	// write below can be rolled back instead of permanently skewing
	// tx.keyDelta/tx.dispositionSeen/tx.staleBytes relative to what was
	// actually durably written (a retried Put/Delete on the same key would
	// otherwise compute its own delta against corrupted baseline state).
	prevDelta := tx.keyDelta
	prevDispositionSeen, hadDisposition := tx.dispositionSeen[keyStr]
	var staleFid uint32
	var hadStaleFid bool
	var prevStaleVal int64

	if firstWriteToKey {
		// Blind-write hazard: someone else may have committed a newer version
		// of this key after our snapshot was taken (they must have finished,
		// since we now hold the lock).
		iter := db.ldb.NewIterator(nil, nil)
		meta, xmin, found := db.latestResolvedMeta(iter, key, tx.xid)
		iter.Release()
		if found && (xmin >= tx.snapshot.Xmax || tx.snapshot.contains(xmin)) {
			db.releaseKeyLockAndForget(tx, keyStr)
			atomic.AddUint64(&db.metricsConflicts, 1)
			tx.markAborted()
			return ErrWriteConflict
		}
		if meta != nil {
			staleFid, hadStaleFid = meta.FileID, true
			prevStaleVal = tx.staleBytes[staleFid]
		}
		tx.recordBaselineImpact(keyStr, key, meta, isDelete)
	} else {
		if pm, ok := tx.ownPriorMeta[keyStr]; ok && pm != nil {
			staleFid, hadStaleFid = pm.FileID, true
			prevStaleVal = tx.staleBytes[staleFid]
		}
		tx.recordOwnImpact(keyStr, key, isDelete)
	}

	rollbackImpact := func() {
		tx.keyDelta = prevDelta
		if hadDisposition {
			tx.dispositionSeen[keyStr] = prevDispositionSeen
		} else {
			delete(tx.dispositionSeen, keyStr)
		}
		if hadStaleFid {
			tx.staleBytes[staleFid] = prevStaleVal
		}
	}

	recType := WALRecordSet
	if isDelete {
		recType = WALRecordDelete
	}

	// The opID allocation, WAL append, and VLog append for this write must
	// happen as one atomic unit under writeSeqMu: WAL and VLog otherwise use
	// independent locks, so a preempted goroutine could let a later opID's
	// VLog write land before an earlier opID's. A crash in that window would
	// make the ValueLog's recovered high-water mark ("maxOp") look like it
	// already covers an opID whose entry was, in fact, never written --
	// permanently and silently losing that committed write on replay
	// (syncWALToValueLog only redoes records with OpID > maxOp).
	db.writeSeqMu.Lock()
	opID, err := db.appendRecord(recType, tx.xid, key, value)
	if err != nil {
		db.writeSeqMu.Unlock()
		rollbackImpact()
		return err
	}

	fileID, offset, err := db.valueLog.AppendEntries([]ValueLogEntry{{
		Key: key, Value: value, TransactionID: tx.xid, OperationID: opID, IsDelete: isDelete,
	}})
	db.writeSeqMu.Unlock()
	if err != nil {
		db.markCorrupt(err)
		rollbackImpact()
		return err
	}

	meta := &EntryMeta{FileID: fileID, ValueOffset: offset, ValueLen: uint32(len(value)), TransactionID: tx.xid, OperationID: opID, IsTombstone: isDelete}

	if err := db.ldb.Put(encodeIndexKey(key, tx.xid), meta.Encode(), nil); err != nil {
		db.markCorrupt(err)
		rollbackImpact()
		return err
	}

	tx.ownPriorMeta[keyStr] = meta
	tx.currentSize += entrySize
	return nil
}

// releaseKeyLockAndForget releases key's first-writer-wins lock (if owned by
// tx) and removes it from tx.keyLocks, all under a single db.txMu critical
// section. tx.keyLocks is also iterated by abortTransaction/
// releaseCommittedLocks under db.txMu from a different goroutine (the
// liveness reaper), so every mutation of it must go through db.txMu too --
// otherwise a plain unlocked delete() here can race with that iteration and
// crash the process with "concurrent map iteration and map write".
func (db *DB) releaseKeyLockAndForget(tx *Transaction, key string) {
	db.txMu.Lock()
	if owner, ok := db.keyLocks[key]; ok && owner == tx.xid {
		delete(db.keyLocks, key)
	}
	delete(tx.keyLocks, key)
	db.txMu.Unlock()
}

func (db *DB) markCorrupt(err error) {
	db.logger.Error("CRITICAL: storage write failed after WAL append. Database entering CORRUPT state.", "err", err)
	atomic.StoreInt32(&db.isCorrupt, 1)
}

// recordBaselineImpact accounts for the very first write to `key` within this
// transaction, comparing against whatever currently-resolved version exists
// in the database (independent of our own read snapshot -- this mirrors the
// pre-eager-write engine's "current DB truth" garbage/key-count accounting).
func (tx *Transaction) recordBaselineImpact(keyStr string, key []byte, meta *EntryMeta, isDelete bool) {
	wasLive := meta != nil && !meta.IsTombstone
	if meta != nil {
		size := int64(ValueLogHeaderSize) + int64(len(key)) + int64(meta.ValueLen)
		tx.staleBytes[meta.FileID] += size
	}
	if isDelete {
		if wasLive {
			tx.keyDelta--
		}
	} else if !wasLive {
		tx.keyDelta++
	}
	tx.dispositionSeen[keyStr] = !isDelete
}

// recordOwnImpact accounts for a second (or later) write to `key` within this
// same transaction, comparing against the running disposition left by our
// own previous write to it.
func (tx *Transaction) recordOwnImpact(keyStr string, key []byte, isDelete bool) {
	wasLive := tx.dispositionSeen[keyStr]
	if pm, ok := tx.ownPriorMeta[keyStr]; ok && pm != nil {
		size := int64(ValueLogHeaderSize) + int64(len(key)) + int64(pm.ValueLen)
		tx.staleBytes[pm.FileID] += size
	}
	if isDelete {
		if wasLive {
			tx.keyDelta--
		}
	} else if !wasLive {
		tx.keyDelta++
	}
	tx.dispositionSeen[keyStr] = !isDelete
}

// latestResolvedMeta walks index versions for key, newest-xid-first, skipping
// aborted entries and the excludeXid (our own in-flight writes, if any). It
// returns the first version found whose owning transaction is committed or
// still in-progress (in-progress can only happen for Get()'s use of this
// helper's sibling walk; for the write path lock exclusivity guarantees no
// other in-progress owner exists).
func (db *DB) latestResolvedMeta(iter index.Iterator, key []byte, excludeXid uint64) (*EntryMeta, uint64, bool) {
	seekKey := encodeIndexKey(key, math.MaxUint64)
	if !iter.Seek(seekKey) {
		return nil, 0, false
	}
	for iter.Valid() {
		foundKey := iter.Key()
		uKey, xmin, err := decodeIndexKey(foundKey)
		if err != nil || !bytes.Equal(uKey, key) {
			return nil, 0, false
		}
		if xmin == excludeXid {
			iter.Next()
			continue
		}
		status := db.clogStatusUnlocked(xmin)
		if status == TxAborted {
			iter.Next()
			continue
		}
		meta, err := decodeEntryMeta(iter.Value())
		if err != nil {
			return nil, 0, false
		}
		return meta, xmin, true
	}
	return nil, 0, false
}

// Get reads the version of key visible to this transaction's snapshot,
// skipping in-progress and aborted versions, with read-your-own-writes for
// RW transactions.
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	if tx.finished {
		return nil, ErrTxnFinished
	}
	if tx.isAborted() {
		return nil, ErrWriteConflict
	}

	if tx.update {
		tx.readSet[string(key)] = struct{}{}
	}

	db := tx.db
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	seekKey := encodeIndexKey(key, math.MaxUint64)
	if iter.Seek(seekKey) {
		for iter.Valid() {
			foundKey := iter.Key()
			uKey, xmin, err := decodeIndexKey(foundKey)
			if err != nil || !bytes.Equal(uKey, key) {
				break
			}

			visible := (tx.update && xmin == tx.xid) || db.isVisibleUnlocked(xmin, tx.snapshot)
			if visible {
				meta, err := decodeEntryMeta(iter.Value())
				if err != nil {
					db.logger.Error("Index meta corruption", "key", string(key), "err", err)
					return nil, fmt.Errorf("meta corrupt: %w", err)
				}
				if meta.IsTombstone {
					return nil, ErrKeyNotFound
				}
				val, err := db.valueLog.ReadValue(meta.FileID, meta.ValueOffset, meta.ValueLen)
				if err != nil {
					db.logger.Error("VLog read failure during Get", "key", string(key), "file_id", meta.FileID, "err", err)
				}
				return val, err
			}
			iter.Next()
		}
	}
	return nil, ErrKeyNotFound
}

// Commit validates the read set against the clog, appends a COMMIT record
// (group-fsynced together with other concurrently-committing transactions),
// marks the clog committed, and releases key locks. Empty read-write
// transactions still write BEGIN+COMMIT.
func (tx *Transaction) Commit() error {
	if !tx.update {
		tx.Discard()
		return nil
	}
	if tx.finished {
		return ErrTxnFinished
	}
	if tx.isAborted() || tx.beginErr != nil {
		tx.finished = true
		tx.db.activeTxnsMu.Lock()
		delete(tx.db.activeTxns, tx)
		tx.db.activeTxnsMu.Unlock()
		tx.db.abortTransaction(tx)
		if tx.beginErr != nil {
			return tx.beginErr
		}
		return ErrWriteConflict
	}
	if atomic.LoadInt32(&tx.db.isDiskFull) == 1 {
		tx.finished = true
		tx.db.activeTxnsMu.Lock()
		delete(tx.db.activeTxns, tx)
		tx.db.activeTxnsMu.Unlock()
		tx.db.abortTransaction(tx)
		return ErrDiskFull
	}

	// shutdownMu is a shutdown barrier: Close() takes the write lock (which
	// blocks until every RLock below has been released) before it closes
	// db.closeCh and lets runGroupCommits exit. That guarantees any request
	// we're about to enqueue into commitCh is guaranteed to still be
	// serviced -- without this barrier, Close() could stop draining
	// commitCh while our request sits in it, permanently stranding us on
	// <-req.resp (see runGroupCommits).
	tx.db.shutdownMu.RLock()
	if atomic.LoadInt32(&tx.db.closed) == 1 {
		tx.db.shutdownMu.RUnlock()
		tx.finished = true
		tx.db.activeTxnsMu.Lock()
		delete(tx.db.activeTxns, tx)
		tx.db.activeTxnsMu.Unlock()
		tx.db.abortTransaction(tx)
		return ErrDatabaseClosed
	}

	req := commitRequest{tx: tx, resp: make(chan error, 1)}
	tx.db.commitCh <- req
	err := <-req.resp
	tx.db.shutdownMu.RUnlock()

	tx.finished = true
	tx.db.activeTxnsMu.Lock()
	delete(tx.db.activeTxns, tx)
	tx.db.activeTxnsMu.Unlock()
	if err != nil && err != ErrWriteConflict {
		tx.db.logger.Error("Transaction commit failed", "err", err)
	}
	return err
}

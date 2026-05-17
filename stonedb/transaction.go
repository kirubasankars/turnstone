package stonedb

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"turnstone/protocol"

	"github.com/syndtr/goleveldb/leveldb/iterator"
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

	snapshot  Snapshot
	aborted   bool
	finished  bool
	abortOnce sync.Once // guards db.abortTransaction's one-time WAL/clog/lock-release work

	iter        iterator.Iterator // cached iterator for reads
	currentSize int64             // accumulated size of keys/values + overhead, for MaxTxSize
}

// markAborted flags the transaction as unusable for further operations. The
// actual ABORT WAL record / clog write happens in db.abortTransaction, called
// either here inline (on conflict) or from Discard/the liveness reaper.
func (tx *Transaction) markAborted() {
	tx.aborted = true
}

// Discard aborts (if RW and not already finished) and releases resources.
// Safe to call multiple times, and safe to call after a successful Commit
// (no-op in that case).
func (tx *Transaction) Discard() {
	if tx.finished {
		return
	}
	tx.finished = true

	if tx.iter != nil {
		tx.iter.Release()
		tx.iter = nil
	}

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
	if tx.aborted {
		return ErrWriteConflict
	}
	db := tx.db
	if db.isDiskFull == 1 {
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

	if firstWriteToKey {
		// Blind-write hazard: someone else may have committed a newer version
		// of this key after our snapshot was taken (they must have finished,
		// since we now hold the lock).
		iter := db.ldb.NewIterator(nil, nil)
		meta, xmin, found := db.latestResolvedMeta(iter, key, tx.xid)
		iter.Release()
		if found && (xmin >= tx.snapshot.Xmax || tx.snapshot.contains(xmin)) {
			db.releaseKeyLockLocked(keyStr, tx.xid)
			delete(tx.keyLocks, keyStr)
			atomic.AddUint64(&db.metricsConflicts, 1)
			tx.markAborted()
			return ErrWriteConflict
		}
		tx.recordBaselineImpact(keyStr, key, meta, isDelete)
	} else {
		tx.recordOwnImpact(keyStr, key, isDelete)
	}

	recType := WALRecordSet
	if isDelete {
		recType = WALRecordDelete
	}
	opID, err := db.appendRecord(recType, tx.xid, key, value)
	if err != nil {
		return err
	}

	fileID, offset, err := db.valueLog.AppendEntries([]ValueLogEntry{{
		Key: key, Value: value, TransactionID: tx.xid, OperationID: opID, IsDelete: isDelete,
	}})
	if err != nil {
		db.markCorrupt(err)
		return err
	}

	meta := &EntryMeta{FileID: fileID, ValueOffset: offset, ValueLen: uint32(len(value)), TransactionID: tx.xid, OperationID: opID, IsTombstone: isDelete}
	if err := db.ldb.Put(encodeIndexKey(key, tx.xid), meta.Encode(), nil); err != nil {
		db.markCorrupt(err)
		return err
	}

	tx.ownPriorMeta[keyStr] = meta
	tx.currentSize += entrySize

	// Invalidate the cached read iterator: goleveldb's iterator reflects a
	// point-in-time snapshot taken when it was created, so a cached iterator
	// from an earlier Get would be blind to the index entry we just wrote,
	// breaking read-your-own-writes for any key touched after the first Get.
	if tx.iter != nil {
		tx.iter.Release()
		tx.iter = nil
	}
	return nil
}

func (db *DB) releaseKeyLockLocked(key string, xid uint64) {
	db.txMu.Lock()
	if owner, ok := db.keyLocks[key]; ok && owner == xid {
		delete(db.keyLocks, key)
	}
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
func (db *DB) latestResolvedMeta(iter iterator.Iterator, key []byte, excludeXid uint64) (*EntryMeta, uint64, bool) {
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
		status := db.clogStatus(xmin)
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
	if tx.aborted {
		return nil, ErrWriteConflict
	}

	if tx.update {
		tx.readSet[string(key)] = struct{}{}
	}

	db := tx.db
	if tx.iter == nil {
		tx.iter = db.ldb.NewIterator(nil, nil)
	}

	seekKey := encodeIndexKey(key, math.MaxUint64)
	if tx.iter.Seek(seekKey) {
		for tx.iter.Valid() {
			foundKey := tx.iter.Key()
			uKey, xmin, err := decodeIndexKey(foundKey)
			if err != nil || !bytes.Equal(uKey, key) {
				break
			}

			visible := (tx.update && xmin == tx.xid) || db.isVisible(xmin, tx.snapshot)
			if visible {
				meta, err := decodeEntryMeta(tx.iter.Value())
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
			tx.iter.Next()
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
	if tx.aborted || tx.beginErr != nil {
		tx.finished = true
		tx.db.activeTxnsMu.Lock()
		delete(tx.db.activeTxns, tx)
		tx.db.activeTxnsMu.Unlock()
		tx.db.abortTransaction(tx)
		if tx.iter != nil {
			tx.iter.Release()
			tx.iter = nil
		}
		if tx.beginErr != nil {
			return tx.beginErr
		}
		return ErrWriteConflict
	}
	if tx.db.isDiskFull == 1 {
		tx.finished = true
		tx.db.activeTxnsMu.Lock()
		delete(tx.db.activeTxns, tx)
		tx.db.activeTxnsMu.Unlock()
		tx.db.abortTransaction(tx)
		return ErrDiskFull
	}

	req := commitRequest{tx: tx, resp: make(chan error, 1)}
	select {
	case tx.db.commitCh <- req:
	default:
		tx.db.commitCh <- req
	}
	err := <-req.resp

	tx.finished = true
	tx.db.activeTxnsMu.Lock()
	delete(tx.db.activeTxns, tx)
	tx.db.activeTxnsMu.Unlock()
	if tx.iter != nil {
		tx.iter.Release()
		tx.iter = nil
	}
	if err != nil && err != ErrWriteConflict {
		tx.db.logger.Error("Transaction commit failed", "err", err)
	}
	return err
}

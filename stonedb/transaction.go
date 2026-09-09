// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"turnstone/protocol"
)

type Transaction struct {
	db     *DB
	update bool

	xid             uint64
	beginOpID       uint64
	beginErr        error
	keyLocks        map[string]struct{}
	dispositionSeen map[string]bool
	ownPriorVer     map[string]*indexVersion
	staleBytes      map[int64]int64
	keyDelta        int64
	readSet         map[string]struct{}

	snapshot  Snapshot
	snapOpID  uint64
	aborted   int32
	finished  bool
	abortOnce sync.Once
	decided   int32

	currentSize int64
}

func (tx *Transaction) isAborted() bool {
	return atomic.LoadInt32(&tx.aborted) == 1
}

func (tx *Transaction) markAborted() {
	atomic.StoreInt32(&tx.aborted, 1)
}

func (tx *Transaction) claimDecision() bool {
	return atomic.CompareAndSwapInt32(&tx.decided, 0, 1)
}

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

	prevDelta := tx.keyDelta
	prevDispositionSeen, hadDisposition := tx.dispositionSeen[keyStr]
	var staleOff int64
	var hadStale bool
	var prevStaleVal int64

	if firstWriteToKey {
		ver, xmin, found := db.latestResolvedMeta(key, tx.xid)
		if found && (xmin >= tx.snapshot.Xmax || tx.snapshot.contains(xmin)) {
			db.releaseKeyLockAndForget(tx, keyStr)
			atomic.AddUint64(&db.metricsConflicts, 1)
			tx.markAborted()
			return ErrWriteConflict
		}
		if ver != nil {
			staleOff, hadStale = ver.offset, true
			prevStaleVal = tx.staleBytes[staleOff]
		}
		tx.recordBaselineImpact(keyStr, ver, isDelete)
	} else {
		if pv, ok := tx.ownPriorVer[keyStr]; ok && pv != nil {
			staleOff, hadStale = pv.offset, true
			prevStaleVal = tx.staleBytes[staleOff]
		}
		tx.recordOwnImpact(keyStr, isDelete)
	}

	rollbackImpact := func() {
		tx.keyDelta = prevDelta
		if hadDisposition {
			tx.dispositionSeen[keyStr] = prevDispositionSeen
		} else {
			delete(tx.dispositionSeen, keyStr)
		}
		if hadStale {
			tx.staleBytes[staleOff] = prevStaleVal
		}
	}

	recType := WALRecordSet
	if isDelete {
		recType = WALRecordDelete
	}

	opID, offset, err := db.appendRecordWithOffset(recType, tx.xid, key, value)
	if err != nil {
		rollbackImpact()
		return err
	}

	db.index.Put(key, indexVersion{
		offset: offset, valueLen: uint32(len(value)),
		xmin: tx.xid, opID: opID, tombstone: isDelete,
	})

	ver := &indexVersion{offset: offset, valueLen: uint32(len(value)), xmin: tx.xid, opID: opID, tombstone: isDelete}
	tx.ownPriorVer[keyStr] = ver
	tx.currentSize += entrySize
	return nil
}

func (db *DB) releaseKeyLockAndForget(tx *Transaction, key string) {
	db.txMu.Lock()
	if owner, ok := db.keyLocks[key]; ok && owner == tx.xid {
		delete(db.keyLocks, key)
	}
	delete(tx.keyLocks, key)
	db.txMu.Unlock()
}

func (tx *Transaction) recordBaselineImpact(keyStr string, ver *indexVersion, isDelete bool) {
	wasLive := ver != nil && !ver.tombstone
	if ver != nil {
		tx.staleBytes[ver.offset] += recordSpanSize(len(keyStr), int(ver.valueLen), WALRecordSet)
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

func (tx *Transaction) recordOwnImpact(keyStr string, isDelete bool) {
	wasLive := tx.dispositionSeen[keyStr]
	if pv, ok := tx.ownPriorVer[keyStr]; ok && pv != nil {
		tx.staleBytes[pv.offset] += recordSpanSize(len(keyStr), int(pv.valueLen), WALRecordSet)
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

	ver, ok := tx.db.index.GetVisible(key, tx.snapshot, tx.xid, tx.update, tx.db.isVisible)
	if !ok || ver == nil {
		return nil, ErrKeyNotFound
	}
	if ver.tombstone {
		return nil, ErrKeyNotFound
	}
	return tx.db.log.ReadValueAt(ver.offset, ver.valueLen)
}

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
	return err
}

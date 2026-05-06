package stonedb

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"turnstone/protocol"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// Transaction represents a running transaction.
type Transaction struct {
	db          *DB
	pendingOps  map[string]*PendingOp
	readSet     map[string]struct{} // Track keys read for conflict detection
	readTxID    uint64
	finished    bool
	update      bool
	iter        iterator.Iterator // Cached iterator for reads
	currentSize int64             // accumulated size of keys and values + overhead
}

// Discard cleans up the transaction resources and unregisters it.
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
}

func (tx *Transaction) Put(key, value []byte) error {
	if !tx.update {
		return errors.New("cannot write in read-only transaction")
	}
	if len(key) == 0 {
		return errors.New("empty key")
	}

	// Calculate size increase: KeyLen(4) + Key + ValLen(4) + Val + Type(1)
	entrySize := int64(len(key) + len(value) + 9)
	if tx.currentSize+entrySize > int64(protocol.MaxTxSize) {
		return fmt.Errorf("transaction size exceeds limit %d", protocol.MaxTxSize)
	}

	tx.pendingOps[string(key)] = &PendingOp{Value: append([]byte{}, value...), IsDelete: false}
	tx.currentSize += entrySize
	return nil
}

func (tx *Transaction) Delete(key []byte) error {
	if !tx.update {
		return errors.New("cannot delete in read-only transaction")
	}

	// Calculate size increase: KeyLen(4) + Key + ValLen(4) + Val(0) + Type(1)
	entrySize := int64(len(key) + 9)
	if tx.currentSize+entrySize > int64(protocol.MaxTxSize) {
		return fmt.Errorf("transaction size exceeds limit %d", protocol.MaxTxSize)
	}

	tx.pendingOps[string(key)] = &PendingOp{Value: nil, IsDelete: true}
	tx.currentSize += entrySize
	return nil
}

func (tx *Transaction) Get(key []byte) ([]byte, error) {
	// 1. Check local pending ops (Read-Your-Own-Writes)
	if op, ok := tx.pendingOps[string(key)]; ok {
		if op.IsDelete {
			return nil, ErrKeyNotFound
		}
		return append([]byte{}, op.Value...), nil
	}

	// 2. Track read for conflict detection
	if tx.update {
		tx.readSet[string(key)] = struct{}{}
	}

	// 3. Search Index
	if tx.iter == nil {
		tx.iter = tx.db.ldb.NewIterator(nil, nil)
	}

	seekKey := encodeIndexKey(key, math.MaxUint64)

	if tx.iter.Seek(seekKey) {
		for tx.iter.Valid() {
			foundKey := tx.iter.Key()
			uKey, version, err := decodeIndexKey(foundKey)
			if err != nil || !bytes.Equal(uKey, key) {
				break
			}

			// MVCC Check
			if version <= tx.readTxID {
				meta, err := decodeEntryMeta(tx.iter.Value())
				if err != nil {
					tx.db.logger.Error("Index meta corruption", "key", string(key), "err", err)
					return nil, fmt.Errorf("meta corrupt: %w", err)
				}
				if meta.IsTombstone {
					return nil, ErrKeyNotFound
				}

				val, err := tx.db.valueLog.ReadValue(meta.FileID, meta.ValueOffset, meta.ValueLen)
				if err != nil {
					tx.db.logger.Error("VLog read failure during Get", "key", string(key), "file_id", meta.FileID, "err", err)
				}
				return val, err
			}
			tx.iter.Next()
		}
	}
	return nil, ErrKeyNotFound
}

// Commit submits the transaction to the Group Commit pipeline.
func (tx *Transaction) Commit() error {
	defer tx.Discard()

	if !tx.update {
		return nil
	}
	if tx.finished {
		return ErrTxnFinished
	}
	if tx.db.isDiskFull == 1 {
		return ErrDiskFull
	}
	if len(tx.pendingOps) == 0 {
		return nil
	}

	req := commitRequest{
		tx:   tx,
		resp: make(chan error, 1),
	}

	// Use select to avoid blocking if the commit channel is full, though unexpected
	select {
	case tx.db.commitCh <- req:
	default:
		tx.db.logger.Warn("Commit channel full, blocking transaction")
		tx.db.commitCh <- req
	}

	err := <-req.resp
	if err != nil {
		// Log specific commit errors that aren't conflicts (conflicts are normal flow)
		if err != ErrWriteConflict {
			tx.db.logger.Error("Transaction commit failed", "err", err)
		}
	}
	return err
}

package stonedb

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// Transaction represents a running transaction (formerly Txn)
type Transaction struct {
	db         *DB
	pendingOps map[string]*PendingOp
	readSet    map[string]struct{} // Track keys read for conflict detection
	readTxID   uint64
	finished   bool
	update     bool
	iter       iterator.Iterator // Cached iterator for reads
}

// Discard cleans up the transaction resources and unregisters it.
// Must be called for both Commit and Rollback (usually via defer).
func (tx *Transaction) Discard() {
	if tx.finished {
		return
	}
	tx.finished = true

	// Release iterator if it exists
	if tx.iter != nil {
		tx.iter.Release()
		tx.iter = nil
	}

	// Unregister from active transactions (Fast O(1) delete)
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
	tx.pendingOps[string(key)] = &PendingOp{Value: append([]byte{}, value...), IsDelete: false}
	return nil
}

func (tx *Transaction) Delete(key []byte) error {
	if !tx.update {
		return errors.New("cannot delete in read-only transaction")
	}
	tx.pendingOps[string(key)] = &PendingOp{Value: nil, IsDelete: true}
	return nil
}

func (tx *Transaction) Get(key []byte) ([]byte, error) {
	// 1. Check local pending ops
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
	// Lazily initialize iterator for reuse within transaction
	if tx.iter == nil {
		tx.iter = tx.db.ldb.NewIterator(nil, nil)
	}

	seekKey := encodeIndexKey(key, math.MaxUint64)

	if tx.iter.Seek(seekKey) {
		for tx.iter.Valid() {
			foundKey := tx.iter.Key()
			uKey, version, err := decodeIndexKey(foundKey)
			if err != nil || !bytes.Equal(uKey, key) {
				// Different key or invalid, so we stop
				break
			}

			// MVCC: Visible version must be <= readTxID
			if version <= tx.readTxID {
				meta, err := decodeEntryMeta(tx.iter.Value())
				if err != nil {
					return nil, fmt.Errorf("meta corrupt: %w", err)
				}
				if meta.IsTombstone {
					return nil, ErrKeyNotFound
				}

				return tx.db.valueLog.ReadValue(meta.FileID, meta.ValueOffset, meta.ValueLen)
			}
			// If version > readTxID, check next version (older)
			tx.iter.Next()
		}
	}
	return nil, ErrKeyNotFound
}

// Commit submits the transaction to the Group Commit pipeline.
func (tx *Transaction) Commit() error {
	// Ensure cleanup happens even if we return early
	defer tx.Discard()

	if !tx.update {
		return nil
	}
	if tx.finished {
		return ErrTxnFinished
	}
	if len(tx.pendingOps) == 0 {
		return nil
	}

	// Submit to Group Commit Channel
	req := commitRequest{
		tx:   tx,
		resp: make(chan error, 1),
	}

	// Send request (blocking if channel full)
	tx.db.commitCh <- req

	// Wait for response
	return <-req.resp
}

// checkConflicts verifies that none of the keys read by this transaction
// have been modified by another transaction that committed after this transaction started.
func (tx *Transaction) checkConflicts() error {
	if len(tx.readSet) == 0 {
		return nil
	}

	iter := tx.db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for k := range tx.readSet {
		keyBytes := []byte(k)
		seekKey := encodeIndexKey(keyBytes, math.MaxUint64)

		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, version, err := decodeIndexKey(foundKey)

			if err == nil && bytes.Equal(uKey, keyBytes) {
				if version > tx.readTxID {
					return ErrWriteConflict
				}
			}
		}
	}
	return nil
}

// Removed prepareCommitBatches from here since it's now handled in db.go commitGroup

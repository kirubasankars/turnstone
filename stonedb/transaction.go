// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stonedb

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// Transaction represents a running transaction.
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
					return nil, fmt.Errorf("meta corrupt: %w", err)
				}
				if meta.IsTombstone {
					return nil, ErrKeyNotFound
				}

				val, err := tx.db.valueLog.ReadValue(meta.FileID, meta.ValueOffset, meta.ValueLen)
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
	if len(tx.pendingOps) == 0 {
		return nil
	}

	req := commitRequest{
		tx:   tx,
		resp: make(chan error, 1),
	}

	tx.db.commitCh <- req
	return <-req.resp
}

// Copyright 2025 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package stonedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
)

// commitBatch holds the state for a single Group Commit iteration.
type commitBatch struct {
	reqs         []commitRequest
	walPayloads  [][]byte
	combinedVLog []ValueLogEntry
	staleBytes   map[uint32]int64

	// IDs assigned to this batch
	maxTxID uint64
	maxOpID uint64

	// Persistence results needed for Index Update
	fileID     uint32
	baseOffset uint32
}

// processCommitBatch executes the 3-stage commit pipeline: Prepare -> Persist -> Apply.
func (db *DB) processCommitBatch(requests []commitRequest) {
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	// Stage 1: Validation & Preparation
	// Filters invalid transactions, checks conflicts, assigns IDs, serializes data.
	batch, err := db.prepareBatch(requests)
	if err != nil {
		// System-level error during preparation (rare)
		for _, req := range requests {
			req.resp <- err
		}
		return
	}

	// If all requests failed validation (e.g., conflicts), we are done.
	if len(batch.reqs) == 0 {
		return
	}

	// Stage 2: Persistence (IO)
	// Writes to WAL and ValueLog.
	if err := db.persistBatch(batch); err != nil {
		batch.failAll(err)
		return
	}

	// Stage 3: Application (Memory/Index)
	// Updates the LevelDB index and advances global clocks.
	if err := db.applyBatchIndex(batch); err != nil {
		batch.failAll(err)
		return
	}

	// Success
	batch.successAll()
}

// prepareBatch validates transactions, detects conflicts (inter-tx and intra-batch),
// and prepares the binary payloads for writing.
func (db *DB) prepareBatch(requests []commitRequest) (*commitBatch, error) {
	batch := &commitBatch{
		staleBytes: make(map[uint32]int64),
	}

	currentTxID := atomic.LoadUint64(&db.transactionID)
	currentOpID := atomic.LoadUint64(&db.operationID)

	// batchWrites tracks keys written by *accepted* transactions within this specific batch.
	// This prevents two concurrent transactions in the same batch from conflicting.
	batchWrites := make(map[string]uint64)

	for _, req := range requests {
		tx := req.tx

		// 1. Standard Conflict Detection (vs DB History)
		if err := tx.checkConflicts(); err != nil {
			req.resp <- err
			continue
		}

		// 2. Intra-Batch Conflict Detection (vs Pending Batch)
		if hasIntraBatchConflict(tx, batchWrites) {
			atomic.AddUint64(&db.metricsConflicts, 1)
			req.resp <- ErrWriteConflict
			continue
		}

		// Transaction Accepted: Assign IDs
		currentTxID++
		startOpID := currentOpID + 1
		currentOpID += uint64(len(tx.pendingOps))

		// Register writes for subsequent intra-batch checks
		for k := range tx.pendingOps {
			batchWrites[k] = currentTxID
		}

		// Serialize (moved to helper for readability)
		walData, vlogEntries := serializeTx(tx, currentTxID, startOpID)

		batch.walPayloads = append(batch.walPayloads, walData)
		batch.combinedVLog = append(batch.combinedVLog, vlogEntries...)
		batch.reqs = append(batch.reqs, req)
	}

	batch.maxTxID = currentTxID
	batch.maxOpID = currentOpID

	return batch, nil
}

// persistBatch writes the prepared data to the Write-Ahead Log and Value Log.
func (db *DB) persistBatch(batch *commitBatch) error {
	// 1. WAL Write (Grouped)
	if err := db.writeAheadLog.AppendBatches(batch.walPayloads); err != nil {
		return fmt.Errorf("wal write failed: %w", err)
	}
	// Metric: Count bytes written to WAL
	for _, p := range batch.walPayloads {
		atomic.AddUint64(&db.metricsBytesWritten, uint64(len(p)))
	}

	// 2. VLog Write (Grouped)
	fileID, offset, err := db.valueLog.AppendEntries(batch.combinedVLog)
	if err != nil {
		return fmt.Errorf("vlog write failed: %w", err)
	}
	// Metric: Count bytes written to VLog
	// VLog size = Header (29) + Key + Value per entry
	// (Actually AppendEntries return total bytes written would be better, but we can estimate or calc)
	// Just use WAL payload size as rough proxy or calculate properly?
	// Calculating properly:
	var vlogSize uint64
	for _, e := range batch.combinedVLog {
		vlogSize += uint64(29 + len(e.Key) + len(e.Value))
	}
	atomic.AddUint64(&db.metricsBytesWritten, vlogSize)

	// Capture VLog location for Index Update
	batch.fileID = fileID
	batch.baseOffset = offset

	return nil
}

// applyBatchIndex updates the in-memory index (LevelDB) to point to the new values
// and updates the global sequence clocks.
func (db *DB) applyBatchIndex(batch *commitBatch) error {
	// Calculate stale bytes (garbage) before updating index
	// This finds existing entries that are about to be overwritten.
	db.calculateStaleBytes(batch)

	// Update Index
	if err := db.UpdateIndexForEntries(batch.combinedVLog, batch.fileID, batch.baseOffset, batch.staleBytes); err != nil {
		return err
	}

	// Update Global Clocks
	atomic.StoreUint64(&db.transactionID, batch.maxTxID)
	atomic.StoreUint64(&db.operationID, batch.maxOpID)

	return nil
}

// Helpers

func hasIntraBatchConflict(tx *Transaction, batchWrites map[string]uint64) bool {
	// Check Read Set
	for k := range tx.readSet {
		if _, exists := batchWrites[k]; exists {
			return true
		}
	}
	// Check Write Set (Blind Writes)
	for k := range tx.pendingOps {
		if _, exists := batchWrites[k]; exists {
			return true
		}
	}
	return false
}

func serializeTx(tx *Transaction, txID, startOpID uint64) ([]byte, []ValueLogEntry) {
	var walBuf bytes.Buffer
	binary.Write(&walBuf, binary.BigEndian, txID)
	binary.Write(&walBuf, binary.BigEndian, startOpID)
	binary.Write(&walBuf, binary.BigEndian, uint32(len(tx.pendingOps)))

	var vlogEntries []ValueLogEntry
	opIdx := uint64(0)

	for k, op := range tx.pendingOps {
		key := []byte(k)
		entry := ValueLogEntry{
			Key:           key,
			Value:         op.Value,
			TransactionID: txID,
			OperationID:   startOpID + opIdx,
			IsDelete:      op.IsDelete,
		}
		vlogEntries = append(vlogEntries, entry)
		opIdx++

		// WAL serialization
		binary.Write(&walBuf, binary.BigEndian, uint32(len(key)))
		walBuf.Write(key)
		binary.Write(&walBuf, binary.BigEndian, uint32(len(op.Value)))
		walBuf.Write(op.Value)
		if op.IsDelete {
			walBuf.WriteByte(1)
		} else {
			walBuf.WriteByte(0)
		}
	}
	return walBuf.Bytes(), vlogEntries
}

func (b *commitBatch) failAll(err error) {
	for _, req := range b.reqs {
		req.resp <- err
	}
}

func (b *commitBatch) successAll() {
	for _, req := range b.reqs {
		req.resp <- nil
	}
}

func (db *DB) calculateStaleBytes(batch *commitBatch) {
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for _, entry := range batch.combinedVLog {
		seekKey := encodeIndexKey(entry.Key, math.MaxUint64)
		if iter.Seek(seekKey) {
			foundKey := iter.Key()
			uKey, _, err := decodeIndexKey(foundKey)
			if err == nil && bytes.Equal(uKey, entry.Key) {
				meta, err := decodeEntryMeta(iter.Value())
				if err == nil {
					size := int64(ValueLogHeaderSize) + int64(len(entry.Key)) + int64(meta.ValueLen)
					batch.staleBytes[meta.FileID] += size
				}
			}
		}
	}
}

package stonedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// testingPrepareBatchErr allows tests to inject a system-level error into prepareBatch.
// This variable is not mutex-protected and should only be set in serial tests.
var testingPrepareBatchErr error

// testingApplyBatchIndexErr allows tests to inject an error into applyBatchIndex.
var testingApplyBatchIndexErr error

// testingPersistBatchHook allows injecting logic after WAL write but before VLog write.
var testingPersistBatchHook func()

// commitBatch holds the state for a single Group Commit iteration.
type commitBatch struct {
	reqs         []commitRequest
	walPayloads  [][]byte
	combinedVLog []ValueLogEntry
	staleBytes   map[uint32]int64
	keyDelta     int64

	// IDs assigned to this batch
	maxTxID uint64
	maxOpID uint64

	// Persistence results needed for Index Update
	fileID     uint32
	baseOffset int64 // Changed to int64
}

// processCommitBatch executes the 3-stage commit pipeline: Prepare -> Persist -> Apply.
func (db *DB) processCommitBatch(requests []commitRequest) {
	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	start := time.Now()

	// Stage 1: Validation & Preparation
	batch, err := db.prepareBatch(requests)
	if err != nil {
		for _, req := range requests {
			req.resp <- err
		}
		return
	}

	if db.isDiskFull == 1 {
		batch.failAll(ErrDiskFull)
		return
	}

	// Double check corruption before proceeding
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		batch.failAll(errors.New("database is corrupt"))
		return
	}

	if len(batch.reqs) == 0 {
		return
	}

	// Stage 2: Persistence (IO)
	walBytesWritten, err := db.persistBatch(batch)
	if err != nil || walBytesWritten == 0 {
		// If WAL write fails, we mark the DB as corrupt immediately.
		// We do NOT attempt to proceed.
		atomic.StoreInt32(&db.isCorrupt, 1)

		db.logger.Error("CRITICAL: WAL Persistence failed. Database entering CORRUPT state.", "err", err)

		// Fail the batch. The client receives an error.
		// Future writes will be rejected by the gatekeeper check in prepareBatch.
		batch.failAll(err)
		return
	}

	// Stage 3: Application (Memory/Index)
	// Only proceed if WAL is guaranteed on disk (handled by strictSync in persistence layer).
	if err := db.applyBatchIndex(batch); err != nil {
		// If Index update fails (memory/logic error), we must crash or mark corrupt.
		// We cannot have the WAL say "Committed" but the Index say "Not Found".
		atomic.StoreInt32(&db.isCorrupt, 1)
		db.logger.Error("CRITICAL: Index application failed after WAL commit. Database CORRUPT.", "err", err)

		// Note: We intentionally do NOT fail the batch here because the data IS in the WAL.
		// It is durable. The Index is just broken. Recovery can fix the Index.
		// We crash to force that recovery.
		panic("CRITICAL: Index inconsistent with WAL. Crashing to trigger Index Rebuild.")
	}

	// WARN: Slow operations
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		db.logger.Warn("Slow group commit", "count", len(requests), "duration", duration)
	}

	// Success
	batch.successAll()
}

func (db *DB) attemptRollbackAndFail(batch *commitBatch, originalErr error, vlogOffset int64, walBytes int64) {
	// Crash-Only Architecture:
	// We attempt to rollback the disk state to ensure consistency.
	// If rollback fails due to I/O errors (e.g. disk busy, permissions), we try a few times.
	// If it still fails, we PANIC. This causes the process to exit.
	// On restart, the DB recovery sequence (WAL replay/truncation) will automatically fix the torn state.
	// We DO NOT mark the DB as "corrupt" and keep running, because that leaves it in a zombie state requiring manual intervention.

	// 1. Rollback VLog
	for i := 0; i < 10; i++ {
		if vErr := db.valueLog.Truncate(vlogOffset); vErr == nil {
			break
		} else {
			db.logger.Warn("VLog rollback failed, retrying...", "err", vErr, "attempt", i+1)
			time.Sleep(500 * time.Millisecond)
		}
		if i == 9 {
			db.logger.Error("CRITICAL: VLog Rollback failed after retries. Panicking to trigger recovery.")
			panic("CRITICAL: VLog Rollback Failed - Storage Inconsistent")
		}
	}

	// 2. Rollback WAL
	for i := 0; i < 10; i++ {
		if wErr := db.rollbackWAL(walBytes); wErr == nil {
			break
		} else {
			db.logger.Warn("WAL rollback failed, retrying...", "err", wErr, "attempt", i+1)
			time.Sleep(500 * time.Millisecond)
		}
		if i == 9 {
			db.logger.Error("CRITICAL: WAL Rollback failed after retries. Panicking to trigger recovery.")
			panic("CRITICAL: WAL Rollback Failed - Storage Inconsistent")
		}
	}

	// INFO: Successful recovery
	db.logger.Info("Safe rollback performed after index failure")
	batch.failAll(fmt.Errorf("internal error (safe rollback performed): %w", originalErr))
}

func (db *DB) prepareBatch(requests []commitRequest) (*commitBatch, error) {
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		return nil, errors.New("database is corrupt")
	}

	if testingPrepareBatchErr != nil {
		return nil, testingPrepareBatchErr
	}

	batch := &commitBatch{
		staleBytes: make(map[uint32]int64),
	}

	currentTxID := atomic.LoadUint64(&db.transactionID)
	currentOpID := atomic.LoadUint64(&db.operationID)
	batchWrites := make(map[string]uint64)

	for _, req := range requests {
		tx := req.tx

		if err := tx.checkConflicts(); err != nil {
			req.resp <- err
			continue
		}

		if hasIntraBatchConflict(tx, batchWrites) {
			atomic.AddUint64(&db.metricsConflicts, 1)
			req.resp <- ErrWriteConflict
			continue
		}

		currentTxID++
		startOpID := currentOpID + 1
		currentOpID += uint64(len(tx.pendingOps))

		for k := range tx.pendingOps {
			batchWrites[k] = currentTxID
		}

		walData, vlogEntries := serializeTx(tx, currentTxID, startOpID)

		batch.walPayloads = append(batch.walPayloads, walData)
		batch.combinedVLog = append(batch.combinedVLog, vlogEntries...)
		batch.reqs = append(batch.reqs, req)
	}

	batch.maxTxID = currentTxID
	batch.maxOpID = currentOpID

	return batch, nil
}

func (db *DB) persistBatch(batch *commitBatch) (int64, error) {
	var walBytesWritten int64
	for _, p := range batch.walPayloads {
		walBytesWritten += int64(WALHeaderSize + len(p))
	}

	if err := db.writeAheadLog.AppendBatches(batch.walPayloads); err != nil {
		return 0, fmt.Errorf("wal write failed: %w", err)
	}

	if testingPersistBatchHook != nil {
		testingPersistBatchHook()
	}

	fileID, offset, err := db.valueLog.AppendEntries(batch.combinedVLog)
	if err != nil {
		// Try immediate rollback if VLog write fails
		if rbErr := db.rollbackWAL(walBytesWritten); rbErr != nil {
			db.logger.Error("CRITICAL: Consistency violation in persistBatch. Panicking.", "wal_written", true, "vlog_err", err, "rollback_err", rbErr)
			panic(fmt.Sprintf("CRITICAL: vlog write failed AND rollback failed: %v", rbErr))
		}
		return 0, fmt.Errorf("vlog write failed (wal rolled back): %w", err)
	}

	batch.fileID = fileID
	batch.baseOffset = offset

	return walBytesWritten, nil
}

func (db *DB) rollbackWAL(bytesToRemove int64) error {
	wal := db.writeAheadLog
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.truncateTailLocked(bytesToRemove)
}

func (db *DB) applyBatchIndex(batch *commitBatch) error {
	if testingApplyBatchIndexErr != nil {
		return testingApplyBatchIndexErr
	}

	// Calculate impacts (stale bytes and key count delta)
	// We use the new consolidated method
	sb, delta := db.calculateBatchImpact(batch.combinedVLog)
	batch.staleBytes = sb
	batch.keyDelta = delta

	// Update Index
	if err := db.UpdateIndexForEntries(batch.combinedVLog, batch.fileID, batch.baseOffset, batch.staleBytes); err != nil {
		return err
	}

	// Update Key Count
	atomic.AddInt64(&db.keyCount, batch.keyDelta)

	// Update Global Clocks
	atomic.StoreUint64(&db.transactionID, batch.maxTxID)
	atomic.StoreUint64(&db.operationID, batch.maxOpID)

	return nil
}

func hasIntraBatchConflict(tx *Transaction, batchWrites map[string]uint64) bool {
	for k := range tx.readSet {
		if _, exists := batchWrites[k]; exists {
			return true
		}
	}
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

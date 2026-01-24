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
	baseOffset uint32
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

	vlogPreOffset := db.valueLog.writeOffset

	// Stage 2: Persistence (IO)
	walBytesWritten, err := db.persistBatch(batch)
	if err != nil || walBytesWritten == 0 {
		db.logger.Error("Batch persistence failed", "err", err, "wal_bytes", walBytesWritten)
		batch.failAll(err)
		return
	}

	// Stage 3: Application (Memory/Index)
	if err := db.applyBatchIndex(batch); err != nil {
		db.logger.Error("Index application failed, attempting rollback", "err", err)
		db.attemptRollbackAndFail(batch, err, vlogPreOffset, walBytesWritten)
		return
	}

	// Log slow commits (over 100ms)
	duration := time.Since(start)
	if duration > 100*time.Millisecond {
		db.logger.Warn("Slow group commit", "count", len(requests), "duration", duration)
	}

	// Success
	batch.successAll()
}

func (db *DB) attemptRollbackAndFail(batch *commitBatch, originalErr error, vlogOffset uint32, walBytes int64) {
	// Try VLog Truncate
	if vErr := db.valueLog.Truncate(vlogOffset); vErr != nil {
		// VLog truncate failed. This is bad but maybe effectively appended garbage if index wasn't updated.
		// However, if we can't seek back, we are in trouble for next writes.
		db.logger.Error("CRITICAL: Index failed and VLog rollback failed. Marking DB Corrupt.", "index_err", originalErr, "vlog_err", vErr)
		atomic.StoreInt32(&db.isCorrupt, 1)
		batch.failAll(fmt.Errorf("CRITICAL: Index failed and VLog rollback failed: %v", vErr))
		return
	}

	// Try WAL Rollback
	if wErr := db.rollbackWAL(walBytes); wErr != nil {
		// WAL rollback failed (e.g. rotation boundary violation).
		// We MUST mark the DB as corrupt to prevent further writes effectively relying on this WAL state
		// or if we are in an inconsistent state where WAL has data but VLog/Index doesn't.
		db.logger.Error("CRITICAL: Index failed, VLog rolled back, but WAL rollback failed. Marking DB Corrupt.", "index_err", originalErr, "wal_err", wErr)
		atomic.StoreInt32(&db.isCorrupt, 1)
		batch.failAll(fmt.Errorf("CRITICAL: WAL rollback failed: %v", wErr))
		return
	}

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
		if rbErr := db.rollbackWAL(walBytesWritten); rbErr != nil {
			db.logger.Error("CRITICAL: Consistency violation in persistBatch. Marking Corrupt.", "wal_written", true, "vlog_err", err, "rollback_err", rbErr)
			atomic.StoreInt32(&db.isCorrupt, 1) // Mark corrupt immediately
			return 0, fmt.Errorf("CRITICAL: vlog write failed AND rollback failed: %v | Original Err: %w", rbErr, err)
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

	currentSize := int64(wal.writeOffset)
	if currentSize < bytesToRemove {
		return fmt.Errorf("cannot rollback across file boundary (offset %d < remove %d)", currentSize, bytesToRemove)
	}

	newOffset := currentSize - bytesToRemove
	if err := wal.currentFile.Truncate(newOffset); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}
	if _, err := wal.currentFile.Seek(newOffset, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}
	wal.writeOffset = uint32(newOffset)
	if err := wal.currentFile.Sync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	return nil
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

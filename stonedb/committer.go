package stonedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync/atomic"
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

	if db.isDiskFull == 1 {
		batch.failAll(ErrDiskFull)
		return
	}

	// If all requests failed validation (e.g., conflicts), we are done.
	if len(batch.reqs) == 0 {
		return
	}

	// Capture VLog state BEFORE writing (for potential rollback)
	// Note: accessing valueLog.writeOffset requires locking or assumes commitMu protects it
	// Since commitMu is the only writer, reading it directly is safe here if VLog is internal.
	vlogPreOffset := db.valueLog.writeOffset

	// Stage 2: Persistence (IO)
	// Writes to WAL and ValueLog.
	walBytesWritten, err := db.persistBatch(batch)
	if err != nil || walBytesWritten == 0 {
		batch.failAll(err)
		return
	}

	// Stage 3: Application (Memory/Index)
	// Updates the LevelDB index and advances global clocks.
	if err := db.applyBatchIndex(batch); err != nil {
		// rollback WAL and Vlog
		db.attemptRollbackAndFail(batch, err, vlogPreOffset, walBytesWritten)
		return
	}

	// Success
	batch.successAll()
}

// Helper to handle the critical rollback logic
func (db *DB) attemptRollbackAndFail(batch *commitBatch, originalErr error, vlogOffset uint32, walBytes int64) {
	// Strategy: Undo actions in reverse order of operations.
	// Order was: WAL Write -> VLog Write.
	// Undo Order: VLog Truncate -> WAL Truncate.

	// 1. Rollback VLog
	if vErr := db.valueLog.Truncate(vlogOffset); vErr != nil {
		// FATAL: We have persisted data on disk that we cannot remove,
		// and we cannot update the index. The DB is now inconsistent.
		panic(fmt.Sprintf("CRITICAL: Index failed (%v) and VLog rollback failed (%v). Crashing to protect consistency.", originalErr, vErr))
	}

	// 2. Rollback WAL
	if wErr := db.rollbackWAL(walBytes); wErr != nil {
		// FATAL: VLog rolled back, but WAL stays. On restart, WAL will replay
		// data into VLog that we just deleted. Phantom writes will appear.
		panic(fmt.Sprintf("CRITICAL: Index failed (%v), VLog rolled back, but WAL rollback failed (%v). Crashing.", originalErr, wErr))
	}

	// 3. Rollback Successful
	// The system state is now exactly as it was before this batch started.
	// We can safely return the error to the client.
	batch.failAll(fmt.Errorf("internal error (safe rollback performed): %w", originalErr))
}

// prepareBatch validates transactions, detects conflicts (inter-tx and intra-batch),
// and prepares the binary payloads for writing.
func (db *DB) prepareBatch(requests []commitRequest) (*commitBatch, error) {
	// Hook for testing the error path
	if testingPrepareBatchErr != nil {
		return nil, testingPrepareBatchErr
	}

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
func (db *DB) persistBatch(batch *commitBatch) (int64, error) {
	// Calculate expected WAL bytes for potential rollback.
	// Each payload in walPayloads is wrapped in a frame with WALHeaderSize (8 bytes).
	var walBytesWritten int64
	for _, p := range batch.walPayloads {
		walBytesWritten += int64(WALHeaderSize + len(p))
	}

	// 1. WAL Write (Grouped)
	// If this returns nil, the transaction is physically durable on disk (fsync'd).
	if err := db.writeAheadLog.AppendBatches(batch.walPayloads); err != nil {
		return 0, fmt.Errorf("wal write failed: %w", err)
	}

	// HOOK: Point of no return checks.
	// Used to inject failures after WAL write but before VLog write to test rollback logic.
	if testingPersistBatchHook != nil {
		testingPersistBatchHook()
	}

	// 2. VLog Write (Grouped)
	// Write the actual values to the Value Log.
	fileID, offset, err := db.valueLog.AppendEntries(batch.combinedVLog)
	if err != nil {
		// Attempt to rollback the WAL write to maintain consistency.
		// If the VLog write fails, we must ensure the WAL entry is removed, otherwise
		// a restart would replay the transaction, causing a "Phantom Write" (data appearing
		// after client was told it failed).
		if rbErr := db.rollbackWAL(walBytesWritten); rbErr != nil {
			// Rollback failed (likely due to file rotation or I/O error).
			// We cannot guarantee consistency, so we must crash to force recovery from a known state.
			panic(fmt.Sprintf("CRITICAL: Consistency violation in persistBatch. WAL written, VLog failed, Rollback failed: %v. Orig Err: %v\n", rbErr, err))
		}

		// Rollback succeeded. The system state is clean (as if WAL write never happened).
		return 0, fmt.Errorf("vlog write failed (wal rolled back): %w", err)
	}

	// Capture VLog location for Index Update
	batch.fileID = fileID
	batch.baseOffset = offset

	return walBytesWritten, nil
}

// rollbackWAL attempts to truncate the active WAL file by the specified number of bytes.
// This is used to undo a successful WAL write if the subsequent VLog write fails.
func (db *DB) rollbackWAL(bytesToRemove int64) error {
	wal := db.writeAheadLog
	wal.mu.Lock()
	defer wal.mu.Unlock()

	currentSize := int64(wal.writeOffset)

	// Check if a rotation occurred during the write we are trying to undo.
	// If writeOffset is smaller than bytesToRemove, it means the file was rotated (or just started).
	// In this case, part of the batch is in a previous file, making simple truncation unsafe/complex.
	if currentSize < bytesToRemove {
		return fmt.Errorf("cannot rollback across file boundary (offset %d < remove %d)", currentSize, bytesToRemove)
	}

	newOffset := currentSize - bytesToRemove

	// 1. Truncate the file to remove the batch
	if err := wal.currentFile.Truncate(newOffset); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	// 2. Reset the file pointer
	if _, err := wal.currentFile.Seek(newOffset, 0); err != nil {
		return fmt.Errorf("seek failed: %w", err)
	}

	// 3. Update internal offset state
	wal.writeOffset = uint32(newOffset)

	// 4. Sync to ensure the truncation is durable
	if err := wal.currentFile.Sync(); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}

	return nil
}

// applyBatchIndex updates the in-memory index (LevelDB) to point to the new values
// and updates the global sequence clocks.
func (db *DB) applyBatchIndex(batch *commitBatch) error {
	// Hook for testing the error path
	if testingApplyBatchIndexErr != nil {
		return testingApplyBatchIndexErr
	}

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
	_ = binary.Write(&walBuf, binary.BigEndian, txID)
	_ = binary.Write(&walBuf, binary.BigEndian, startOpID)
	_ = binary.Write(&walBuf, binary.BigEndian, uint32(len(tx.pendingOps)))

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
		_ = binary.Write(&walBuf, binary.BigEndian, uint32(len(key)))
		walBuf.Write(key)
		_ = binary.Write(&walBuf, binary.BigEndian, uint32(len(op.Value)))
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

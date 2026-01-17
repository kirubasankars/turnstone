package stonedb

import (
	"bytes"
	"fmt"
)

// SnapshotEntry represents a single Key-Value pair from the database state.
type SnapshotEntry struct {
	Key   []byte
	Value []byte
}

// StreamSnapshot iterates over the entire active keyspace and invokes the callback.
// Returns the TxID and OpID (Log Sequence) at the time the snapshot completed.
func (db *DB) StreamSnapshot(fn func(batch []SnapshotEntry) error) (uint64, uint64, error) {
	// 1. Start a Read Transaction.
	tx := db.NewTransaction(false)
	defer tx.Discard()

	// 2. Lock to capture the high-water marks (TxID and OpID)
	db.mu.RLock()
	snapOpID := db.operationID
	snapTxID := db.transactionID
	db.mu.RUnlock()

	// 3. Create Iterator
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	var batch []SnapshotEntry
	batchSize := 0
	const maxBatchBytes = 1 * 1024 * 1024 // 1MB Batches

	var lastLogicalKey []byte

	// 4. Iterate keyspace
	for iter.Next() {
		idxKey := iter.Key()

		// Skip system keys
		if bytes.HasPrefix(idxKey, []byte("!sys!")) {
			continue
		}

		// Decode Index Key
		uKey, _, err := decodeIndexKey(idxKey)
		if err != nil {
			continue
		}

		// Dedup
		if bytes.Equal(uKey, lastLogicalKey) {
			continue
		}
		lastLogicalKey = append([]byte(nil), uKey...)

		// Decode Metadata
		meta, err := decodeEntryMeta(iter.Value())
		if err != nil {
			continue
		}

		// Skip tombstones
		if meta.IsTombstone {
			continue
		}

		// Fetch Value
		val, err := db.valueLog.ReadValue(meta.FileID, meta.ValueOffset, meta.ValueLen)
		if err != nil {
			fmt.Printf("Snapshot warning: missing value for key %s (FileID %d): %v\n", uKey, meta.FileID, err)
			continue
		}

		// Add to Batch
		batch = append(batch, SnapshotEntry{
			Key:   append([]byte(nil), uKey...),
			Value: val,
		})
		batchSize += len(uKey) + len(val)

		// Flush Batch
		if batchSize >= maxBatchBytes {
			if err := fn(batch); err != nil {
				return 0, 0, err
			}
			batch = batch[:0]
			batchSize = 0
		}
	}

	// Final Flush
	if len(batch) > 0 {
		if err := fn(batch); err != nil {
			return 0, 0, err
		}
	}

	return snapTxID, snapOpID, iter.Error()
}

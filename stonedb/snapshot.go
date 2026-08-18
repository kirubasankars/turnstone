package stonedb

import (
	"bytes"
	"time"
)

// SnapshotEntry represents a single Key-Value pair from the database state.
type SnapshotEntry struct {
	Key   []byte
	Value []byte
}

// StreamSnapshot iterates over the entire active keyspace and invokes the callback.
// Returns the TxID and OpID (Log Sequence) at the time the snapshot completed.
func (db *DB) StreamSnapshot(fn func(batch []SnapshotEntry) error) (uint64, uint64, error) {
	start := time.Now()
	// 1. Start a Read Transaction.
	tx := db.NewTransaction(false)
	defer tx.Discard()

	// 2. Derive the high-water marks (TxID and OpID) from the transaction's
	// own snapshot boundary, captured atomically under db.txMu at the exact
	// instant the snapshot was fixed (see NewTransaction). Re-reading
	// db.transactionID/db.operationID here separately -- as plain fields,
	// which is itself a data race since every writer uses sync/atomic --
	// could also observe commits that landed after the snapshot boundary,
	// making the returned watermark inconsistent with the data actually
	// streamed below.
	snapOpID := tx.snapOpID
	var snapTxID uint64
	if tx.snapshot.Xmax > 0 {
		snapTxID = tx.snapshot.Xmax - 1
	}

	db.logger.Info("Starting snapshot stream", "snap_tx_id", snapTxID, "snap_op_id", snapOpID)

	// 3. Create Iterator
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	var batch []SnapshotEntry
	batchSize := 0
	const maxBatchBytes = 1 * 1024 * 1024 // 1MB Batches

	var lastLogicalKey []byte
	resolvedForKey := false
	itemCount := 0

	// 4. Iterate keyspace. Versions of a given key appear consecutively,
	// newest-xid-first (see encodeIndexKey). Skip in-progress/aborted
	// versions and take the first committed-and-visible one per key.
	for iter.Next() {
		idxKey := iter.Key()

		// Skip system keys
		if bytes.HasPrefix(idxKey, []byte("!sys!")) {
			continue
		}

		// Decode Index Key
		uKey, xmin, err := decodeIndexKey(idxKey)
		if err != nil {
			continue
		}

		if !bytes.Equal(uKey, lastLogicalKey) {
			lastLogicalKey = append([]byte(nil), uKey...)
			resolvedForKey = false
		}
		if resolvedForKey {
			continue
		}

		if !tx.db.isVisible(xmin, tx.snapshot) {
			continue
		}
		resolvedForKey = true

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
			db.logger.Warn("Snapshot: missing value for key", "key", string(uKey), "file_id", meta.FileID, "err", err)
			continue
		}

		// Add to Batch
		batch = append(batch, SnapshotEntry{
			Key:   append([]byte(nil), uKey...),
			Value: val,
		})
		batchSize += len(uKey) + len(val)
		itemCount++

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

	if iter.Error() != nil {
		db.logger.Error("Snapshot iteration error", "err", iter.Error())
	} else {
		db.logger.Info("Snapshot stream complete", "items", itemCount, "duration", time.Since(start))
	}

	return snapTxID, snapOpID, iter.Error()
}

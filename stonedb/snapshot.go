package stonedb

import "time"

type SnapshotEntry struct {
	Key   []byte
	Value []byte
}

func (db *DB) StreamSnapshot(fn func(batch []SnapshotEntry) error) (uint64, uint64, error) {
	start := time.Now()
	tx := db.NewTransaction(false)
	defer tx.Discard()

	snapOpID := tx.snapOpID
	var snapTxID uint64
	if tx.snapshot.Xmax > 0 {
		snapTxID = tx.snapshot.Xmax - 1
	}

	db.logger.Info("Starting snapshot stream", "snap_tx_id", snapTxID, "snap_op_id", snapOpID)

	var batch []SnapshotEntry
	batchSize := 0
	const maxBatchBytes = 1 * 1024 * 1024
	itemCount := 0

	db.index.ForEachKey(func(key []byte, chain []indexVersion) {
		ver, ok := visibleVersionForKey(chain, tx.snapshot, tx.xid, tx.update, tx.db.isVisible)
		if !ok || ver == nil || ver.tombstone {
			return
		}
		val, err := db.log.ReadValueAt(ver.offset, ver.valueLen)
		if err != nil {
			db.logger.Warn("Snapshot: missing value", "key", string(key), "err", err)
			return
		}
		batch = append(batch, SnapshotEntry{Key: append([]byte(nil), key...), Value: val})
		batchSize += len(key) + len(val)
		itemCount++
		if batchSize >= maxBatchBytes {
			if err := fn(batch); err != nil {
				return
			}
			batch = batch[:0]
			batchSize = 0
		}
	})

	if len(batch) > 0 {
		if err := fn(batch); err != nil {
			return 0, 0, err
		}
	}

	db.logger.Info("Snapshot stream complete", "items", itemCount, "duration", time.Since(start))
	return snapTxID, snapOpID, nil
}

package stonedb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func (db *DB) recoverValueLog() error {
	maxTx, maxOp, err := db.valueLog.Recover()
	if err != nil {
		return err
	}
	db.transactionID = maxTx
	db.operationID = maxOp
	db.logger.Info("ValueLog recovered", "max_tx", maxTx, "max_op", maxOp)
	return nil
}

func (db *DB) syncWALToValueLog(truncateCorrupt bool) error {
	onTruncate := func() error {
		indexPath := filepath.Join(db.dir, "index")
		db.logger.Warn("WAL truncated due to corruption. Deleting LevelDB index to ensure consistency", "path", indexPath)
		return os.RemoveAll(indexPath)
	}

	return db.writeAheadLog.ReplaySinceTx(db.valueLog, db.transactionID, truncateCorrupt, func(entries []ValueLogEntry) {
		replayCount := 0
		for _, e := range entries {
			if e.TransactionID > db.transactionID {
				db.transactionID = e.TransactionID
			}
			if e.OperationID > db.operationID {
				db.operationID = e.OperationID
			}
			replayCount++
		}
		// Log batch replays minimally unless debug
		if replayCount > 0 {
			db.logger.Debug("Replayed WAL entries", "count", replayCount, "new_head_tx", db.transactionID)
		}
	}, onTruncate)
}

func (db *DB) isIndexConsistent() bool {
	if db.ldb == nil {
		return false
	}
	val, err := db.ldb.Get(sysTransactionIDKey, nil)
	if err == leveldb.ErrNotFound {
		return db.transactionID == 0
	}
	if err != nil || len(val) != 8 {
		return false
	}
	ldbTxID := binary.BigEndian.Uint64(val)
	return ldbTxID == db.transactionID
}

func (db *DB) RebuildIndexFromVLog() error {
	if db.ldb != nil {
		db.ldb.Close()
		db.ldb = nil
	}
	indexPath := filepath.Join(db.dir, "index")
	os.RemoveAll(indexPath)

	ldbOpts := &opt.Options{
		BlockCacheCapacity: 64 * 1024 * 1024,
		Compression:        opt.SnappyCompression,
	}
	var err error
	db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)
	if err != nil {
		return err
	}

	db.deletedBytesByFile = make(map[uint32]int64)

	batch := new(leveldb.Batch)
	batchCount := 0
	totalCount := 0

	err = db.valueLog.Replay(0, func(e ValueLogEntry, meta EntryMeta) error {
		encKey := encodeIndexKey(e.Key, meta.TransactionID)
		batch.Put(encKey, meta.Encode())

		batchCount++
		totalCount++
		if batchCount >= 1000 {
			if err := db.ldb.Write(batch, nil); err != nil {
				return err
			}
			batch.Reset()
			batchCount = 0
		}
		return nil
	})
	if err != nil {
		return err
	}

	if batch.Len() > 0 {
		if err := db.ldb.Write(batch, nil); err != nil {
			return err
		}
	}

	db.logger.Info("Index rebuilt from VLog", "total_entries", totalCount)

	// After rebuilding index, we must recalculate the KeyCount since we lost the persisted value
	count, err := db.scanKeyCount()
	if err != nil {
		return fmt.Errorf("failed to recount keys after rebuild: %w", err)
	}
	atomic.StoreInt64(&db.keyCount, count)

	return db.persistSequences()
}

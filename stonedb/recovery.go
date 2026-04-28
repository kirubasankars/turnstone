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
	// CHANGED: Reduced from INFO to DEBUG
	db.logger.Debug("ValueLog recovered", "max_tx", maxTx, "max_op", maxOp)
	return nil
}

// syncWALToValueLog replays WAL records newer than the VLog's recovered
// high-water mark, redoing any SET/DEL that didn't make it into the VLog
// before a crash, and reconstructing the commit log (BEGIN/COMMIT/ABORT) so
// it can be persisted into LevelDB once it is reopened (see
// persistClogRebuild). Any transaction that never reached COMMIT or ABORT by
// the end of the WAL is a crash victim and is resolved as aborted. It uses
// history to skip orphaned writes from stale timelines.
func (db *DB) syncWALToValueLog(truncateCorrupt bool, history []TimelineHistoryItem) error {
	onTruncate := func() error {
		indexPath := filepath.Join(db.dir, "index")
		db.logger.Warn("WAL truncated due to corruption. Deleting LevelDB index to ensure consistency", "path", indexPath)
		return os.RemoveAll(indexPath)
	}

	clogRebuild := make(map[uint64]TxStatus)
	replayCount := 0

	err := db.writeAheadLog.ReplaySinceTx(db.valueLog, db.operationID, history, truncateCorrupt, func(rec WALRecord) {
		if rec.XID > db.transactionID {
			db.transactionID = rec.XID
		}
		if rec.OpID > db.operationID {
			db.operationID = rec.OpID
		}
		switch rec.Type {
		case WALRecordBegin:
			clogRebuild[rec.XID] = TxInProgress
		case WALRecordCommit:
			clogRebuild[rec.XID] = TxCommitted
		case WALRecordAbort:
			clogRebuild[rec.XID] = TxAborted
		}
		replayCount++
	}, onTruncate)
	if err != nil {
		return err
	}

	if replayCount > 0 {
		db.logger.Debug("Replayed WAL records", "count", replayCount, "new_head_tx", db.transactionID, "new_head_op", db.operationID)
	}
	db.pendingClogRebuild = clogRebuild
	return nil
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
		BlockCacheCapacity: db.blockCacheSize,
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

	// CHANGED: Reduced from INFO to DEBUG
	db.logger.Debug("Index rebuilt from VLog", "total_entries", totalCount)

	// After rebuilding index, we must recalculate the KeyCount since we lost the persisted value
	count, err := db.scanKeyCount()
	if err != nil {
		return fmt.Errorf("failed to recount keys after rebuild: %w", err)
	}
	atomic.StoreInt64(&db.keyCount, count)

	return db.persistSequences()
}

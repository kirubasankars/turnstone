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
	return nil
}

func (db *DB) syncWALToValueLog(truncateCorrupt bool) error {
	onTruncate := func() error {
		indexPath := filepath.Join(db.dir, "index")
		fmt.Printf("WAL truncated due to corruption. Deleting LevelDB index at %s to ensure consistency.\n", indexPath)
		return os.RemoveAll(indexPath)
	}

	return db.writeAheadLog.ReplaySinceTx(db.valueLog, db.transactionID, truncateCorrupt, func(entries []ValueLogEntry) {
		for _, e := range entries {
			if e.TransactionID > db.transactionID {
				db.transactionID = e.TransactionID
			}
			if e.OperationID > db.operationID {
				db.operationID = e.OperationID
			}
		}
	}, onTruncate)
}

func (db *DB) isIndexConsistent() bool {
	// If LDB is not open, we can't check consistency
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

	// REPLAY FIX: Pass 0 to replay ALL entries from the beginning.
	// Passing db.transactionID (which is the latest) would skip everything.
	err = db.valueLog.Replay(0, func(e ValueLogEntry, meta EntryMeta) error {
		encKey := encodeIndexKey(e.Key, meta.TransactionID)
		batch.Put(encKey, meta.Encode())

		batchCount++
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

	return db.persistSequences()
}

func (db *DB) persistSequences() error {
	// Guard against nil LDB
	if db.ldb == nil {
		return nil
	}

	batch := new(leveldb.Batch)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, atomic.LoadUint64(&db.transactionID))
	batch.Put(sysTransactionIDKey, buf)

	buf2 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf2, atomic.LoadUint64(&db.operationID))
	batch.Put(sysOperationIDKey, buf2)

	return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
}

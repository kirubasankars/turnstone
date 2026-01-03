package engine

import (
	"math"

	"turnstone/protocol"
)

// SetVLogMaxFileSize allows tests to temporarily override the global var
func SetVLogMaxFileSize(size int) func() {
	orig := vlogMaxFileSize
	vlogMaxFileSize = size
	return func() {
		vlogMaxFileSize = orig
	}
}

// Flush manually triggers a memtable rotation to force disk persistence for testing.
// In the real system, this happens via background workers or Close().
func (db *StoneDB) Flush() error {
	doneCh := make(chan struct{})
	db.mu.Lock()
	db.rotateMemTableLocked(doneCh)
	db.mu.Unlock()
	<-doneCh
	return nil
}

// MinActiveTxID helper for testing transaction tracking
func (db *StoneDB) MinActiveTxID() uint64 {
	db.activeTxMu.Lock()
	defer db.activeTxMu.Unlock()
	if len(db.activeTxns) == 0 {
		return math.MaxUint64
	}
	min := uint64(math.MaxUint64)
	for id := range db.activeTxns {
		if id < min {
			min = id
		}
	}
	return min
}

// Has checks if a key exists in the underlying LevelDB index (for white-box testing)
func (db *StoneDB) HasIndex(key string, txID uint64) (bool, error) {
	return db.ldb.Has(encodeKey(key, txID), nil)
}

// Get extension for Transaction to support CAS/Optimistic Locking in tests.
// It populates the readCache to ensure ExpectedVersion logic works.
func (tx *Transaction) Get(key string) ([]byte, error) {
	// 1. Peek into LevelDB to find the current version (TxID)
	seekKey := encodeKey(key, tx.db.CurrentTxID())
	iter := tx.db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	if ok := iter.Seek(seekKey); !ok {
		return nil, protocol.ErrKeyNotFound
	}

	foundKey := iter.Key()
	// Verify key match (ignoring the 9-byte suffix)
	if len(foundKey) < 9 || string(foundKey[:len(foundKey)-9]) != key {
		return nil, protocol.ErrKeyNotFound
	}

	ptr := decodeValuePointer(iter.Value())

	if ptr.IsTombstone {
		return nil, protocol.ErrKeyNotFound
	}

	// 2. Populate Read Cache for OCC
	tx.readCache[key] = ptr.TxID

	// 3. Fetch actual value using main DB logic
	return tx.db.Get(key, tx.db.CurrentTxID())
}

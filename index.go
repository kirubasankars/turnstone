package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// IndexEntry represents a specific version of a key pointing to a location in the WAL.
type IndexEntry struct {
	Offset  int64
	Length  int32
	LSN     uint64
	Deleted bool
}

// IndexUpdate represents a pending write to the index used for batching.
type IndexUpdate struct {
	Key     string
	Offset  int64
	Length  int64
	LSN     uint64
	Deleted bool
}

// Index represents the abstract interface for the key storage backend.
type Index interface {
	Close() error
	PutCheckpoint(lsn uint64, offset int64) error
	GetCheckpoints() (map[uint64]int64, error)
	Len() int
	SizeBytes() int64
	Get(key string, readLSN uint64) (IndexEntry, bool)
	GetHead(key string) (IndexEntry, bool)
	GetLatest(key string) (IndexEntry, bool)
	Set(key string, offset int64, length int64, lsn uint64, deleted bool, minReadLSN uint64)
	SetBatch(updates []IndexUpdate) error
	UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool
	Remove(key string)
	OffloadColdKeys(minReadLSN uint64) (int, error)
	// PutState persists the recovery state: Next LSN, Next LogID, WAL Offset, and Approximate Key Count.
	PutState(nextLSN uint64, nextLogID uint64, offset int64, count int64) error
	// GetState retrieves the persisted recovery state and restores internal counters.
	GetState() (nextLSN uint64, nextLogID uint64, offset int64, count int64, err error)
}

func NewIndex(dir string) (Index, error) {
	return NewLevelDBIndex(dir)
}

// --- LevelDB Implementation ---

const (
	prefixIndex      = byte('i')
	prefixCheckpoint = byte('c')
	prefixState      = byte('s')
)

var bufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 256) },
}

func getBuf(size int) []byte {
	b := bufferPool.Get().([]byte)
	if cap(b) < size {
		b = make([]byte, size)
	}
	return b[:size]
}
func putBuf(b []byte) { bufferPool.Put(b) }

type LevelDBIndex struct {
	db          *leveldb.DB
	approxCount int64
}

func NewLevelDBIndex(dir string) (*LevelDBIndex, error) {
	dbPath := dir + "/index.ldb"
	opts := &opt.Options{
		Compression:            opt.NoCompression,
		BlockCacheCapacity:     64 * 1024 * 1024,
		OpenFilesCacheCapacity: 50,
		WriteBuffer:            64 * 1024 * 1024,
	}
	db, err := leveldb.OpenFile(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}
	return &LevelDBIndex{db: db}, nil
}

func (idx *LevelDBIndex) Close() error { return idx.db.Close() }

func (idx *LevelDBIndex) encodeKey(key string, lsn uint64) []byte {
	kLen := len(key)
	buf := getBuf(1 + kLen + 8)
	buf[0] = prefixIndex
	copy(buf[1:], key)
	binary.BigEndian.PutUint64(buf[1+kLen:], ^lsn) // Inverted LSN
	return buf
}

func (idx *LevelDBIndex) Get(key string, readLSN uint64) (IndexEntry, bool) {
	target := idx.encodeKey(key, readLSN)
	defer putBuf(target)

	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	if iter.Seek(target) {
		if isSameKey(iter.Key(), []byte(key)) {
			entry := decodeIndexVal(iter.Value())
			entry.LSN = decodeLSN(iter.Key())
			if entry.Deleted {
				return IndexEntry{}, false
			}
			return entry, true
		}
	}
	return IndexEntry{}, false
}

func (idx *LevelDBIndex) Set(key string, offset int64, length int64, lsn uint64, deleted bool, minReadLSN uint64) {
	dbKey := idx.encodeKey(key, lsn)
	defer putBuf(dbKey)

	var val [13]byte
	binary.BigEndian.PutUint64(val[0:8], uint64(offset))
	binary.BigEndian.PutUint32(val[8:12], uint32(length))
	if deleted {
		val[12] = 1
	}
	if err := idx.db.Put(dbKey, val[:], nil); err != nil {
		fmt.Fprintf(os.Stderr, "LevelDB Put Error: %v\n", err)
	}
	atomic.AddInt64(&idx.approxCount, 1)
}

func (idx *LevelDBIndex) SetBatch(updates []IndexUpdate) error {
	batch := new(leveldb.Batch)
	for _, u := range updates {
		dbKey := idx.encodeKey(u.Key, u.LSN)

		var val [13]byte
		binary.BigEndian.PutUint64(val[0:8], uint64(u.Offset))
		binary.BigEndian.PutUint32(val[8:12], uint32(u.Length))
		if u.Deleted {
			val[12] = 1
		}

		// LevelDB copies the key/value data, so it is safe to return dbKey to the pool immediately
		batch.Put(dbKey, val[:])
		putBuf(dbKey)
	}

	if err := idx.db.Write(batch, nil); err != nil {
		return err
	}
	atomic.AddInt64(&idx.approxCount, int64(len(updates)))
	return nil
}

func (idx *LevelDBIndex) GetHead(key string) (IndexEntry, bool) {
	// Head is the smallest inverted LSN (which is largest real LSN)
	return idx.Get(key, ^uint64(0))
}

func (idx *LevelDBIndex) GetLatest(key string) (IndexEntry, bool) {
	return idx.GetHead(key)
}

func (idx *LevelDBIndex) UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool {
	dbKey := idx.encodeKey(key, lsn)
	defer putBuf(dbKey)

	v, err := idx.db.Get(dbKey, nil)
	if err == nil && v != nil {
		deleted := v[12] == 1
		var val [13]byte
		binary.BigEndian.PutUint64(val[0:8], uint64(newOffset))
		binary.BigEndian.PutUint32(val[8:12], uint32(newLength))
		if deleted {
			val[12] = 1
		}
		err := idx.db.Put(dbKey, val[:], nil)
		return err == nil
	}
	return false
}

// Helpers for LevelDB
func isSameKey(dbKey, userKey []byte) bool {
	if len(dbKey) != 1+len(userKey)+8 {
		return false
	}
	return dbKey[0] == prefixIndex && bytes.Equal(dbKey[1:1+len(userKey)], userKey)
}

func decodeLSN(dbKey []byte) uint64 {
	return ^binary.BigEndian.Uint64(dbKey[len(dbKey)-8:])
}

func decodeIndexVal(val []byte) IndexEntry {
	return IndexEntry{
		Offset:  int64(binary.BigEndian.Uint64(val[0:8])),
		Length:  int32(binary.BigEndian.Uint32(val[8:12])),
		Deleted: val[12] == 1,
	}
}

func (idx *LevelDBIndex) Len() int         { return int(atomic.LoadInt64(&idx.approxCount)) }
func (idx *LevelDBIndex) SizeBytes() int64 { return 0 }
func (idx *LevelDBIndex) Remove(key string) {
	target := idx.encodeKey(key, ^uint64(0))
	defer putBuf(target)
	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()
	batch := new(leveldb.Batch)
	kBytes := []byte(key)
	if iter.Seek(target) {
		for iter.Valid() && isSameKey(iter.Key(), kBytes) {
			batch.Delete(append([]byte{}, iter.Key()...))
			iter.Next()
		}
	}
	idx.db.Write(batch, nil)
}

func (idx *LevelDBIndex) PutState(nextLSN uint64, nextLogID uint64, offset int64, count int64) error {
	key := []byte{prefixState}
	val := make([]byte, 32)
	binary.BigEndian.PutUint64(val[0:8], nextLSN)
	binary.BigEndian.PutUint64(val[8:16], nextLogID)
	binary.BigEndian.PutUint64(val[16:24], uint64(offset))
	binary.BigEndian.PutUint64(val[24:32], uint64(count))
	return idx.db.Put(key, val, nil)
}

func (idx *LevelDBIndex) GetState() (uint64, uint64, int64, int64, error) {
	key := []byte{prefixState}
	val, err := idx.db.Get(key, nil)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if len(val) != 32 {
		return 0, 0, 0, 0, fmt.Errorf("corrupted state length: %d", len(val))
	}

	lsn := binary.BigEndian.Uint64(val[0:8])
	logID := binary.BigEndian.Uint64(val[8:16])
	offset := int64(binary.BigEndian.Uint64(val[16:24]))
	count := int64(binary.BigEndian.Uint64(val[24:32]))

	// Restore the in-memory count
	atomic.StoreInt64(&idx.approxCount, count)

	return lsn, logID, offset, count, nil
}

func (idx *LevelDBIndex) PutCheckpoint(lsn uint64, offset int64) error {
	key := make([]byte, 9)
	key[0] = prefixCheckpoint
	binary.BigEndian.PutUint64(key[1:], lsn)
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(offset))
	return idx.db.Put(key, val, nil)
}

func (idx *LevelDBIndex) GetCheckpoints() (map[uint64]int64, error) {
	results := make(map[uint64]int64)
	iter := idx.db.NewIterator(util.BytesPrefix([]byte{prefixCheckpoint}), nil)
	defer iter.Release()
	for iter.Next() {
		k, v := iter.Key(), iter.Value()
		if len(k) == 9 && len(v) == 8 {
			results[binary.BigEndian.Uint64(k[1:])] = int64(binary.BigEndian.Uint64(v))
		}
	}
	return results, iter.Error()
}

func (idx *LevelDBIndex) OffloadColdKeys(minReadLSN uint64) (int, error) {
	iter := idx.db.NewIterator(util.BytesPrefix([]byte{prefixIndex}), nil)
	defer iter.Release()
	batch := new(leveldb.Batch)
	count, deleted := 0, 0
	var lastKey []byte
	keptOld := false

	for iter.Next() {
		k := iter.Key()
		userKey := k[1 : len(k)-8]
		lsn := decodeLSN(k)

		if !bytes.Equal(userKey, lastKey) {
			lastKey = append([]byte{}, userKey...)
			keptOld = false
		}

		if lsn >= minReadLSN {
			continue
		}
		if !keptOld {
			keptOld = true
			continue
		}
		// Prune
		batch.Delete(append([]byte{}, k...))
		count++
		deleted++
		if count >= 1000 {
			idx.db.Write(batch, nil)
			batch.Reset()
			count = 0
		}
	}
	if count > 0 {
		idx.db.Write(batch, nil)
	}
	return deleted, nil
}

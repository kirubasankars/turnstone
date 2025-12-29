package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"os"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// IndexEntry represents a specific version of a key pointing to a location in the WAL.
type IndexEntry struct {
	Offset  int64  // Byte offset in the WAL file.
	Length  int32  // Total length of the record (Header + Payload).
	LSN     uint64 // Logical Sequence Number for versioning and snapshot isolation.
	Deleted bool   // Tombstone flag.
}

// memoryEntrySize approximates the size of IndexEntry struct + slice overhead.
// int64 (8) + int32 (4) + uint64 (8) + bool (1) + padding ~= 24 bytes.
const memoryEntrySize = 24

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
	UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool
	Remove(key string)
	OffloadColdKeys(minReadLSN uint64) (int, error)

	// PutState persists the global store state (NextLSN and WAL Offset).
	PutState(nextLSN uint64, offset int64) error
	// GetState retrieves the persisted global store state.
	GetState() (nextLSN uint64, offset int64, err error)
}

// NewIndex initializes the storage based on the type.
func NewIndex(dir string, indexType string) (Index, error) {
	switch indexType {
	case "memory":
		return NewMemoryIndex(dir)
	case "sharded":
		// ShardedIndex is also an in-memory implementation, but partitioned
		// to reduce lock contention.
		return NewShardedIndex(dir)
	default:
		// Default to LevelDB
		return NewLevelDBIndex(dir)
	}
}

// --- Memory Implementation ---

type MemoryIndex struct {
	mu sync.RWMutex // Added for thread safety
	// data maps Key -> Slice of Versions (sorted by LSN ascending)
	data map[string][]IndexEntry
	// checkpoints maps LSN -> WAL Offset
	checkpoints map[uint64]int64
	// approxCount tracks total keys (unique keys)
	count int64
	// sizeBytes tracks the estimated memory usage of keys and entries
	sizeBytes int64
}

func NewMemoryIndex(dir string) (*MemoryIndex, error) {
	// Remove LevelDB folder if it exists to avoid confusion, or ignore it.
	// We don't persist index to disk in this mode (it rebuilds from WAL).
	return &MemoryIndex{
		data:        make(map[string][]IndexEntry),
		checkpoints: make(map[uint64]int64),
	}, nil
}

func (mi *MemoryIndex) Close() error {
	return nil
}

func (mi *MemoryIndex) PutCheckpoint(lsn uint64, offset int64) error {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.checkpoints[lsn] = offset
	return nil
}

func (mi *MemoryIndex) GetCheckpoints() (map[uint64]int64, error) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()
	return mi.checkpoints, nil
}

func (mi *MemoryIndex) Len() int {
	return int(atomic.LoadInt64(&mi.count))
}

func (mi *MemoryIndex) SizeBytes() int64 {
	return atomic.LoadInt64(&mi.sizeBytes)
}

func (mi *MemoryIndex) Get(key string, readLSN uint64) (IndexEntry, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()

	versions, ok := mi.data[key]
	if !ok || len(versions) == 0 {
		return IndexEntry{}, false
	}

	// Versions are sorted by LSN ascending.
	// We want the largest LSN <= readLSN.
	// Iterate backwards.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].LSN <= readLSN {
			if versions[i].Deleted {
				return IndexEntry{}, false
			}
			return versions[i], true
		}
	}
	return IndexEntry{}, false
}

func (mi *MemoryIndex) GetHead(key string) (IndexEntry, bool) {
	mi.mu.RLock()
	defer mi.mu.RUnlock()

	versions, ok := mi.data[key]
	if !ok || len(versions) == 0 {
		return IndexEntry{}, false
	}
	// Return the absolute latest version
	return versions[len(versions)-1], true
}

func (mi *MemoryIndex) GetLatest(key string) (IndexEntry, bool) {
	entry, ok := mi.GetHead(key)
	if !ok || entry.Deleted {
		return IndexEntry{}, false
	}
	return entry, true
}

func (mi *MemoryIndex) Set(key string, offset int64, length int64, lsn uint64, deleted bool, minReadLSN uint64) {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	entry := IndexEntry{
		Offset:  offset,
		Length:  int32(length),
		LSN:     lsn,
		Deleted: deleted,
	}

	if _, exists := mi.data[key]; !exists {
		atomic.AddInt64(&mi.count, 1)
		// Add size of key string
		atomic.AddInt64(&mi.sizeBytes, int64(len(key)))
	}
	mi.data[key] = append(mi.data[key], entry)
	// Add size of the new entry struct
	atomic.AddInt64(&mi.sizeBytes, memoryEntrySize)
}

func (mi *MemoryIndex) UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	versions, ok := mi.data[key]
	if !ok || len(versions) == 0 {
		return false
	}

	// Update the specific version matching LSN.
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].LSN == lsn {
			versions[i].Offset = newOffset
			versions[i].Length = int32(newLength)
			mi.data[key] = versions // In case slice header changed (unlikely here)
			return true
		}
	}
	return false
}

func (mi *MemoryIndex) Remove(key string) {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	// Admin removal.
	if versions, ok := mi.data[key]; ok {
		delete(mi.data, key)
		atomic.AddInt64(&mi.count, -1)
		// Subtract size of key + size of all entries
		usage := int64(len(key)) + int64(len(versions)*memoryEntrySize)
		atomic.AddInt64(&mi.sizeBytes, -usage)
	}
}

func (mi *MemoryIndex) OffloadColdKeys(minReadLSN uint64) (int, error) {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	pruned := 0
	for key, versions := range mi.data {
		if len(versions) <= 1 {
			continue
		}
		// Keep all versions >= minReadLSN.
		// Also keep at least one version < minReadLSN (the visible one).
		// Find the cutoff index.
		cutoff := -1
		for i, v := range versions {
			if v.LSN >= minReadLSN {
				cutoff = i
				break
			}
		}

		if cutoff > 0 {
			// We can prune everything before cutoff-1 (keep one older version for visibility)
			keepIdx := cutoff - 1
			if keepIdx > 0 {
				newVersions := make([]IndexEntry, len(versions)-keepIdx)
				copy(newVersions, versions[keepIdx:])
				mi.data[key] = newVersions
				pruned += keepIdx
				// Reduce tracked size by number of pruned entries
				atomic.AddInt64(&mi.sizeBytes, -int64(keepIdx*memoryEntrySize))
			}
		} else if cutoff == -1 {
			// All versions are old (< minReadLSN). Keep only the last one.
			if len(versions) > 1 {
				keepIdx := len(versions) - 1
				mi.data[key] = []IndexEntry{versions[keepIdx]}
				pruned += keepIdx
				// Reduce tracked size by number of pruned entries
				atomic.AddInt64(&mi.sizeBytes, -int64(keepIdx*memoryEntrySize))
			}
		}
	}
	return pruned, nil
}

// MemoryIndex does not persist state across restarts (rebuilds from WAL).
func (mi *MemoryIndex) PutState(nextLSN uint64, offset int64) error {
	return nil
}

func (mi *MemoryIndex) GetState() (uint64, int64, error) {
	return 0, 0, nil
}

// --- Sharded Map Implementation ---

const numShards = 128 // Power of 2 for efficient modulo

type shard struct {
	mu   sync.RWMutex
	data map[string][]IndexEntry
}

type ShardedIndex struct {
	shards [numShards]*shard
	seed   maphash.Seed

	ckptMu      sync.RWMutex
	checkpoints map[uint64]int64

	count     int64
	sizeBytes int64 // Global atomic size counter
}

func NewShardedIndex(dir string) (*ShardedIndex, error) {
	idx := &ShardedIndex{
		seed:        maphash.MakeSeed(),
		checkpoints: make(map[uint64]int64),
	}
	for i := 0; i < numShards; i++ {
		idx.shards[i] = &shard{
			data: make(map[string][]IndexEntry),
		}
	}
	return idx, nil
}

func (s *ShardedIndex) getShard(key string) *shard {
	var h maphash.Hash
	h.SetSeed(s.seed)
	h.WriteString(key)
	return s.shards[h.Sum64()%uint64(numShards)]
}

func (s *ShardedIndex) Close() error {
	return nil
}

func (s *ShardedIndex) PutCheckpoint(lsn uint64, offset int64) error {
	s.ckptMu.Lock()
	defer s.ckptMu.Unlock()
	s.checkpoints[lsn] = offset
	return nil
}

func (s *ShardedIndex) GetCheckpoints() (map[uint64]int64, error) {
	s.ckptMu.RLock()
	defer s.ckptMu.RUnlock()
	return s.checkpoints, nil
}

func (s *ShardedIndex) Len() int {
	return int(atomic.LoadInt64(&s.count))
}

func (s *ShardedIndex) SizeBytes() int64 {
	return atomic.LoadInt64(&s.sizeBytes)
}

func (s *ShardedIndex) Get(key string, readLSN uint64) (IndexEntry, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	versions, ok := sh.data[key]
	if !ok || len(versions) == 0 {
		return IndexEntry{}, false
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].LSN <= readLSN {
			if versions[i].Deleted {
				return IndexEntry{}, false
			}
			return versions[i], true
		}
	}
	return IndexEntry{}, false
}

func (s *ShardedIndex) GetHead(key string) (IndexEntry, bool) {
	sh := s.getShard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	versions, ok := sh.data[key]
	if !ok || len(versions) == 0 {
		return IndexEntry{}, false
	}
	return versions[len(versions)-1], true
}

func (s *ShardedIndex) GetLatest(key string) (IndexEntry, bool) {
	entry, ok := s.GetHead(key)
	if !ok || entry.Deleted {
		return IndexEntry{}, false
	}
	return entry, true
}

func (s *ShardedIndex) Set(key string, offset int64, length int64, lsn uint64, deleted bool, minReadLSN uint64) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	entry := IndexEntry{
		Offset:  offset,
		Length:  int32(length),
		LSN:     lsn,
		Deleted: deleted,
	}

	if _, exists := sh.data[key]; !exists {
		atomic.AddInt64(&s.count, 1)
		atomic.AddInt64(&s.sizeBytes, int64(len(key)))
	}
	sh.data[key] = append(sh.data[key], entry)
	atomic.AddInt64(&s.sizeBytes, memoryEntrySize)
}

func (s *ShardedIndex) UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	versions, ok := sh.data[key]
	if !ok || len(versions) == 0 {
		return false
	}

	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].LSN == lsn {
			versions[i].Offset = newOffset
			versions[i].Length = int32(newLength)
			sh.data[key] = versions
			return true
		}
	}
	return false
}

func (s *ShardedIndex) Remove(key string) {
	sh := s.getShard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if versions, ok := sh.data[key]; ok {
		delete(sh.data, key)
		atomic.AddInt64(&s.count, -1)
		usage := int64(len(key)) + int64(len(versions)*memoryEntrySize)
		atomic.AddInt64(&s.sizeBytes, -usage)
	}
}

func (s *ShardedIndex) OffloadColdKeys(minReadLSN uint64) (int, error) {
	pruned := 0
	for i := 0; i < numShards; i++ {
		sh := s.shards[i]
		sh.mu.Lock()
		for key, versions := range sh.data {
			if len(versions) <= 1 {
				continue
			}
			cutoff := -1
			for idx, v := range versions {
				if v.LSN >= minReadLSN {
					cutoff = idx
					break
				}
			}

			if cutoff > 0 {
				keepIdx := cutoff - 1
				if keepIdx > 0 {
					newVersions := make([]IndexEntry, len(versions)-keepIdx)
					copy(newVersions, versions[keepIdx:])
					sh.data[key] = newVersions
					pruned += keepIdx
					atomic.AddInt64(&s.sizeBytes, -int64(keepIdx*memoryEntrySize))
				}
			} else if cutoff == -1 {
				if len(versions) > 1 {
					keepIdx := len(versions) - 1
					sh.data[key] = []IndexEntry{versions[keepIdx]}
					pruned += keepIdx
					atomic.AddInt64(&s.sizeBytes, -int64(keepIdx*memoryEntrySize))
				}
			}
		}
		sh.mu.Unlock()
	}
	return pruned, nil
}

// ShardedIndex does not persist state across restarts.
func (s *ShardedIndex) PutState(nextLSN uint64, offset int64) error {
	return nil
}

func (s *ShardedIndex) GetState() (uint64, int64, error) {
	return 0, 0, nil
}

// --- LevelDB Implementation ---

// Estimated overhead per entry is not tracked in Pure LevelDB mode
const entrySizeOverhead = 0

// Key Prefixes to segregate data in the flat LevelDB keyspace
const (
	prefixIndex      = byte('i') // Key Prefix: 'i' + key + LSN
	prefixCheckpoint = byte('c') // Key Prefix: 'c' + big-endian LSN
	prefixState      = byte('s') // Key Prefix: 's' (Single key for global state)
)

// bufferPool reuses byte slices to reduce GC pressure for LevelDB key construction.
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Default size good for typical keys + prefix + LSN (e.g. 256 bytes)
		return make([]byte, 256)
	},
}

func getBuf(size int) []byte {
	b := bufferPool.Get().([]byte)
	if cap(b) < size {
		b = make([]byte, size)
	}
	return b[:size]
}

func putBuf(b []byte) {
	bufferPool.Put(b)
}

type LevelDBIndex struct {
	db *leveldb.DB

	// approxCount tracks the number of Set operations.
	// Since we don't read-before-write for performance, we can't track
	// the exact number of unique keys (overwrites vs new keys).
	approxCount int64
}

func NewLevelDBIndex(dir string) (*LevelDBIndex, error) {
	dbPath := dir + "/index.ldb"

	// WIPE ON STARTUP REMOVED:
	// We no longer remove the existing DB on startup.
	// This enables the potential to skip full WAL replay if the store logic is updated to support it.
	// Current store logic will still replay WAL and overwrite existing entries, which is safe but redundant.

	// Open LevelDB
	// NoCompression is used because the WAL offsets are random-ish and small,
	// and speed is prioritized over space for the index.
	opts := &opt.Options{
		Compression:        opt.NoCompression,
		BlockCacheCapacity: 64 * 1024 * 1024, // 64MB Cache
		// ROBUSTNESS: Limit open files to prevent mmap exhaustion on restricted systems (like ulimit -n 1024)
		OpenFilesCacheCapacity: 50,
		// OPTIMIZATION: Increase WriteBuffer to speed up the massive bulk load during recovery
		WriteBuffer: 64 * 1024 * 1024, // 64MB
	}
	db, err := leveldb.OpenFile(dbPath, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb: %w", err)
	}

	return &LevelDBIndex{
		db:          db,
		approxCount: 0,
	}, nil
}

func (idx *LevelDBIndex) Close() error {
	return idx.db.Close()
}

func (idx *LevelDBIndex) PutCheckpoint(lsn uint64, offset int64) error {
	key := make([]byte, 1+8)
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
		k := iter.Key()
		v := iter.Value()

		if len(k) != 9 || len(v) != 8 {
			continue
		}

		lsn := binary.BigEndian.Uint64(k[1:]) // Skip prefix 'c'
		off := binary.BigEndian.Uint64(v)
		results[lsn] = int64(off)
	}

	return results, iter.Error()
}

func (idx *LevelDBIndex) Len() int {
	return int(atomic.LoadInt64(&idx.approxCount))
}

func (idx *LevelDBIndex) SizeBytes() int64 {
	// LevelDB size tracking is offloaded to disk statistics or ignored for in-memory limit purposes
	// as it manages its own memory (BlockCache).
	return 0
}

func (idx *LevelDBIndex) Get(key string, readLSN uint64) (IndexEntry, bool) {
	// Construct Target Key: prefix + key + ^readLSN
	// Inverting LSN makes keys sort in DESCENDING LSN order.
	// Seek will find the smallest inverted LSN >= target, which corresponds
	// to the largest real LSN <= readLSN.
	kLen := len(key)
	targetSize := 1 + kLen + 8
	target := getBuf(targetSize)
	defer putBuf(target)

	target[0] = prefixIndex
	copy(target[1:], key)
	binary.BigEndian.PutUint64(target[1+kLen:], ^readLSN)

	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	// Simple Seek. No Prev() needed.
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

func (idx *LevelDBIndex) GetHead(key string) (IndexEntry, bool) {
	// To find the HEAD (newest version), we seek the prefix.
	// Since keys are [Prefix][Key][^LSN], and ^LSN sorts descending,
	// the first entry for [Prefix][Key] is the Head.
	kLen := len(key)
	targetSize := 1 + kLen
	target := getBuf(targetSize)
	defer putBuf(target)

	target[0] = prefixIndex
	copy(target[1:], key)

	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	if iter.Seek(target) {
		if isSameKey(iter.Key(), []byte(key)) {
			entry := decodeIndexVal(iter.Value())
			entry.LSN = decodeLSN(iter.Key())
			return entry, true
		}
	}
	return IndexEntry{}, false
}

func (idx *LevelDBIndex) GetLatest(key string) (IndexEntry, bool) {
	entry, found := idx.GetHead(key)
	if !found {
		return IndexEntry{}, false
	}
	if entry.Deleted {
		return IndexEntry{}, false
	}
	return entry, true
}

func (idx *LevelDBIndex) Set(key string, offset int64, length int64, lsn uint64, deleted bool, minReadLSN uint64) {
	kLen := len(key)
	dbKeySize := 1 + kLen + 8
	dbKey := getBuf(dbKeySize)
	defer putBuf(dbKey)

	dbKey[0] = prefixIndex
	copy(dbKey[1:], key)
	// STORE AS INVERTED LSN
	binary.BigEndian.PutUint64(dbKey[1+kLen:], ^lsn)

	// Use stack-allocated array for value (small constant size) to avoid malloc
	var val [13]byte
	binary.BigEndian.PutUint64(val[0:8], uint64(offset))
	binary.BigEndian.PutUint32(val[8:12], uint32(length))
	if deleted {
		val[12] = 1
	} else {
		val[12] = 0
	}

	// Capture error instead of ignoring it
	if err := idx.db.Put(dbKey, val[:], nil); err != nil {
		fmt.Fprintf(os.Stderr, "LevelDB Put Error: %v\n", err)
	}
	atomic.AddInt64(&idx.approxCount, 1)
}

func (idx *LevelDBIndex) UpdateHead(key string, newOffset int64, newLength int64, lsn uint64) bool {
	kLen := len(key)
	dbKeySize := 1 + kLen + 8
	dbKey := getBuf(dbKeySize)
	defer putBuf(dbKey)

	dbKey[0] = prefixIndex
	copy(dbKey[1:], key)
	// STORE AS INVERTED LSN
	binary.BigEndian.PutUint64(dbKey[1+kLen:], ^lsn)

	v, err := idx.db.Get(dbKey, nil)
	if err == nil && v != nil {
		deleted := v[12] == 1

		var val [13]byte
		binary.BigEndian.PutUint64(val[0:8], uint64(newOffset))
		binary.BigEndian.PutUint32(val[8:12], uint32(newLength))
		if deleted {
			val[12] = 1
		} else {
			val[12] = 0
		}

		err := idx.db.Put(dbKey, val[:], nil)
		return err == nil
	}
	return false
}

func (idx *LevelDBIndex) Remove(key string) {
	kLen := len(key)
	targetSize := 1 + kLen
	target := getBuf(targetSize)
	defer putBuf(target)

	target[0] = prefixIndex
	copy(target[1:], key)

	iter := idx.db.NewIterator(nil, nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	kBytes := []byte(key) // We still need the raw key bytes for comparison

	if iter.Seek(target) {
		for iter.Valid() {
			if !isSameKey(iter.Key(), kBytes) {
				break
			}
			// Must copy iterator key because it is valid only until Next
			keyToDelete := make([]byte, len(iter.Key()))
			copy(keyToDelete, iter.Key())
			batch.Delete(keyToDelete)
			iter.Next()
		}
	}
	_ = idx.db.Write(batch, nil)
}

func (idx *LevelDBIndex) OffloadColdKeys(minReadLSN uint64) (int, error) {
	iter := idx.db.NewIterator(util.BytesPrefix([]byte{prefixIndex}), nil)
	defer iter.Release()

	batch := new(leveldb.Batch)
	batchSize := 0
	const MaxBatchSize = 1000
	deletedCount := 0

	var lastUserKey []byte
	keptVersionBelowMin := false

	for iter.Next() {
		key := iter.Key()

		// Key format: [prefixIndex (1)] [UserKey (N)] [InvertedLSN (8)]
		if len(key) < 9 {
			continue
		}

		// Extract UserKey and LSN
		userKey := key[1 : len(key)-8]
		// LSN is stored inverted (^lsn) for descending sort order in LevelDB
		invertedLSN := binary.BigEndian.Uint64(key[len(key)-8:])
		lsn := ^invertedLSN

		// Check if we switched to a new user key
		// Note: The iterator sorts by Key bytes, so all versions of a key are contiguous.
		if !bytes.Equal(userKey, lastUserKey) {
			lastUserKey = make([]byte, len(userKey))
			copy(lastUserKey, userKey)
			keptVersionBelowMin = false
		}

		// 1. Keep recent versions (>= minReadLSN)
		if lsn >= minReadLSN {
			continue
		}

		// 2. Keep the LATEST version that is < minReadLSN
		// Because keys are sorted by LSN descending (due to inverted storage),
		// the first version we encounter that is < minReadLSN is the newest one in that range.
		if !keptVersionBelowMin {
			keptVersionBelowMin = true
			continue
		}

		// 3. Prune older versions
		// We copy the key because the iterator's buffer is transient
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		batch.Delete(keyCopy)
		batchSize++
		deletedCount++

		if batchSize >= MaxBatchSize {
			if err := idx.db.Write(batch, nil); err != nil {
				return deletedCount, fmt.Errorf("failed to write batch cleanup: %w", err)
			}
			batch.Reset()
			batchSize = 0
		}
	}

	if batchSize > 0 {
		if err := idx.db.Write(batch, nil); err != nil {
			return deletedCount, fmt.Errorf("failed to write final batch cleanup: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return deletedCount, fmt.Errorf("iterator error during cleanup: %w", err)
	}

	// Trigger manual compaction to reclaim disk space immediately
	if deletedCount > 0 {
		if err := idx.db.CompactRange(util.Range{Start: nil, Limit: nil}); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to trigger manual compaction: %v\n", err)
		}
	}

	return deletedCount, nil
}

func (idx *LevelDBIndex) PutState(nextLSN uint64, offset int64) error {
	key := []byte{prefixState}
	val := make([]byte, 16)
	binary.BigEndian.PutUint64(val[0:8], nextLSN)
	binary.BigEndian.PutUint64(val[8:16], uint64(offset))
	return idx.db.Put(key, val, nil)
}

func (idx *LevelDBIndex) GetState() (uint64, int64, error) {
	key := []byte{prefixState}
	val, err := idx.db.Get(key, nil)
	if err != nil {
		return 0, 0, err
	}
	if len(val) != 16 {
		return 0, 0, fmt.Errorf("corrupted state")
	}
	nextLSN := binary.BigEndian.Uint64(val[0:8])
	offset := binary.BigEndian.Uint64(val[8:16])
	return nextLSN, int64(offset), nil
}

// --- Helpers ---

func isSameKey(dbKey []byte, userKey []byte) bool {
	if len(dbKey) != 1+len(userKey)+8 {
		return false
	}
	if dbKey[0] != prefixIndex {
		return false
	}
	return bytes.Equal(dbKey[1:1+len(userKey)], userKey)
}

func decodeLSN(dbKey []byte) uint64 {
	if len(dbKey) < 8 {
		return 0
	}
	// INVERT BACK
	return ^binary.BigEndian.Uint64(dbKey[len(dbKey)-8:])
}

func decodeIndexVal(val []byte) IndexEntry {
	if len(val) < 13 {
		return IndexEntry{}
	}
	return IndexEntry{
		Offset:  int64(binary.BigEndian.Uint64(val[0:8])),
		Length:  int32(binary.BigEndian.Uint32(val[8:12])),
		Deleted: val[12] == 1,
	}
}

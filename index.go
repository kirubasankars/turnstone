package main

import (
	"encoding/binary"
	"sort"

	"go.etcd.io/bbolt"
)

// IndexEntry represents a version of a key pointing to a location in the WAL.
type IndexEntry uint64

const indexEntryDeletedMask uint64 = 1 << 63

func packIndexEntry(offset int64, deleted bool) IndexEntry {
	v := uint64(offset)
	if deleted {
		v |= indexEntryDeletedMask
	}
	return IndexEntry(v)
}

func (e IndexEntry) Offset() int64 {
	return int64(uint64(e) &^ indexEntryDeletedMask)
}

func (e IndexEntry) Deleted() bool {
	return (uint64(e) & indexEntryDeletedMask) != 0
}

// memState holds the in-memory part of the index.
// It groups the key history and the pair lengths together to ensure atomic rotation.
type memState struct {
	keys        map[string][]IndexEntry
	pairLengths map[int64]int64
}

func newMemState() *memState {
	return &memState{
		keys:        make(map[string][]IndexEntry),
		pairLengths: make(map[int64]int64),
	}
}

// Index implements a tiered Hybrid MVCC Index.
type Index struct {
	active   *memState // Hot writes go here
	flushing *memState // Immutable snapshot for background flush

	db *bbolt.DB // Persistent storage
}

func NewIndex(db *bbolt.DB) *Index {
	return &Index{
		active:   newMemState(),
		flushing: nil,
		db:       db,
	}
}

// Len returns the total count of keys (In-Memory + On-Disk).
// Note: This is an approximation as it sums stats.
func (idx *Index) Len() int {
	count := len(idx.active.keys)
	if idx.flushing != nil {
		count += len(idx.flushing.keys)
	}

	_ = idx.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketData))
		if b != nil {
			count += b.Stats().KeyN
		}
		return nil
	})

	return count
}

func (idx *Index) Get(key string, readVersion int64) (IndexEntry, bool) {
	// 1. Check Active (Hot)
	if ent, ok := idx.searchHistory(idx.active.keys[key], readVersion); ok {
		return ent, true
	}

	// 2. Check Flushing (Warm)
	if idx.flushing != nil {
		if ent, ok := idx.searchHistory(idx.flushing.keys[key], readVersion); ok {
			return ent, true
		}
	}

	// 3. Check BoltDB (Cold)
	var ent IndexEntry
	found := false
	_ = idx.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketData))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v != nil {
			history := idx.decodeHistory(v)
			ent, found = idx.searchHistory(history, readVersion)
		}
		return nil
	})

	return ent, found
}

func (idx *Index) GetLatest(key string) (IndexEntry, bool) {
	if hist := idx.active.keys[key]; len(hist) > 0 {
		return idx.checkLatest(hist)
	}
	if idx.flushing != nil {
		if hist := idx.flushing.keys[key]; len(hist) > 0 {
			return idx.checkLatest(hist)
		}
	}
	var ent IndexEntry
	found := false
	_ = idx.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketData))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if len(v) < 4 {
			return nil
		}

		if len(v) >= 12 { // 4 bytes count + at least 1 entry (8 bytes)
			lastOffset := len(v) - 8
			val := binary.LittleEndian.Uint64(v[lastOffset:])
			ent = IndexEntry(val)

			if !ent.Deleted() {
				found = true
			}
		}
		return nil
	})
	return ent, found
}

func (idx *Index) checkLatest(history []IndexEntry) (IndexEntry, bool) {
	last := history[len(history)-1]
	if last.Deleted() {
		return 0, false
	}
	return last, true
}

func (idx *Index) searchHistory(history []IndexEntry, readVersion int64) (IndexEntry, bool) {
	if len(history) == 0 {
		return 0, false
	}
	for i := len(history) - 1; i >= 0; i-- {
		entry := history[i]
		if entry.Offset() < readVersion {
			if entry.Deleted() {
				return 0, false
			}
			return entry, true
		}
	}
	return 0, false
}

func (idx *Index) Set(key string, offset int64, length int64, deleted bool, minReadVersion int64) {
	// 1. Update Key Index
	entry := packIndexEntry(offset, deleted)
	idx.active.keys[key] = append(idx.active.keys[key], entry)

	// Prune in-memory history based on minReadVersion to save RAM
	history := idx.active.keys[key]
	if len(history) > 1 {
		pivot := -1
		for i := 0; i < len(history)-1; i++ {
			if history[i+1].Offset() < minReadVersion {
				pivot = i
			} else {
				break
			}
		}

		if pivot >= 0 {
			remain := history[pivot+1:]
			newHist := make([]IndexEntry, len(remain))
			copy(newHist, remain)
			idx.active.keys[key] = newHist
		}
	}

	// 2. Update Pair Length Index (for Sync safety)
	idx.active.pairLengths[offset] = length
}

func (idx *Index) Rotate() bool {
	if len(idx.active.keys) == 0 && len(idx.active.pairLengths) == 0 {
		return false
	}
	if idx.flushing != nil {
		return false // Previous flush not finished
	}

	// Atomic swap
	idx.flushing = idx.active
	idx.active = newMemState()

	return true
}

func (idx *Index) FlushToBolt(minReadVersion int64) error {
	// If nothing to flush, return
	if idx.flushing == nil {
		return nil
	}

	keys := make([]string, 0, len(idx.flushing.keys))
	for k := range idx.flushing.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	err := idx.db.Update(func(tx *bbolt.Tx) error {
		// 1. Flush Keys
		b, err := tx.CreateBucketIfNotExists([]byte(BoltBucketData))
		if err != nil {
			return err
		}
		b.FillPercent = 0.9 // Optimize for sequential writes

		meta, err := tx.CreateBucketIfNotExists([]byte(BoltBucketMeta))
		if err != nil {
			return err
		}

		var maxOffset int64

		for _, key := range keys {
			memHist := idx.flushing.keys[key]
			keyBytes := []byte(key)

			var fullHist []IndexEntry
			v := b.Get(keyBytes)
			if v != nil {
				fullHist = idx.decodeHistory(v)
			}
			fullHist = append(fullHist, memHist...)

			prunedHist := idx.pruneHistory(fullHist, minReadVersion)

			shouldDelete := false
			if len(prunedHist) == 0 {
				shouldDelete = true
			} else {
				lastEntry := prunedHist[len(prunedHist)-1]
				if lastEntry.Deleted() && lastEntry.Offset() < minReadVersion {
					shouldDelete = true
				}
			}

			if shouldDelete {
				if err := b.Delete(keyBytes); err != nil {
					return err
				}
			} else {
				encoded := idx.encodeHistory(prunedHist)
				if err := b.Put(keyBytes, encoded); err != nil {
					return err
				}
			}

			if len(memHist) > 0 {
				last := memHist[len(memHist)-1].Offset()
				if last > maxOffset {
					maxOffset = last
				}
			}
		}

		if maxOffset > 0 {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, uint64(maxOffset))
			if err := meta.Put([]byte(KeyLastOffset), buf); err != nil {
				return err
			}
		}

		// 2. Flush Pair Lengths
		if len(idx.flushing.pairLengths) > 0 {
			bPair, err := tx.CreateBucketIfNotExists([]byte(BoltBucketPair))
			if err != nil {
				return err
			}
			bPair.FillPercent = 0.9

			// Sort by offset for sequential writes (critical for BoltDB performance)
			offsets := make([]int64, 0, len(idx.flushing.pairLengths))
			for off := range idx.flushing.pairLengths {
				offsets = append(offsets, off)
			}
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

			for _, off := range offsets {
				length := idx.flushing.pairLengths[off]

				kBuf := make([]byte, 8)
				vBuf := make([]byte, 8)

				binary.BigEndian.PutUint64(kBuf, uint64(off))
				binary.BigEndian.PutUint64(vBuf, uint64(length))

				if err := bPair.Put(kBuf, vBuf); err != nil {
					return err
				}
			}
		}

		return nil
	})

	return err
}

func (idx *Index) FinishFlush() {
	idx.flushing = nil
}

func (idx *Index) pruneHistory(hist []IndexEntry, minVer int64) []IndexEntry {
	if len(hist) <= 1 {
		return hist
	}
	pivot := -1
	for i := 0; i < len(hist)-1; i++ {
		if hist[i+1].Offset() < minVer {
			pivot = i
		} else {
			break
		}
	}
	if pivot >= 0 {
		return hist[pivot+1:]
	}
	return hist
}

func (idx *Index) encodeHistory(hist []IndexEntry) []byte {
	// Protocol: [Count uint32] [Entry uint64]...
	size := 4 + len(hist)*8
	buf := make([]byte, size)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(hist)))

	for i, entry := range hist {
		offset := 4 + i*8
		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(entry))
	}
	return buf
}

func (idx *Index) decodeHistory(data []byte) []IndexEntry {
	if len(data) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(data[0:4])

	// Basic safety check for buffer size vs declared count
	if len(data) < 4+int(count)*8 {
		return nil
	}

	hist := make([]IndexEntry, count)
	for i := 0; i < int(count); i++ {
		offset := 4 + i*8
		packed := binary.LittleEndian.Uint64(data[offset : offset+8])
		hist[i] = IndexEntry(packed)
	}
	return hist
}

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

// Index implements a tiered Hybrid MVCC Index.
type Index struct {
	active   map[string][]IndexEntry // Hot writes go here
	flushing map[string][]IndexEntry // Immutable snapshot for background flush

	// Pair Length Maps (Offset -> Length)
	// Key is the Offset of the pair.
	// Value is the total Length of the pair (Header + Key + Value).
	activePairLengths   map[int64]int64
	flushingPairLengths map[int64]int64

	db *bbolt.DB // Persistent storage
}

func NewIndex(db *bbolt.DB) *Index {
	return &Index{
		active:            make(map[string][]IndexEntry),
		flushing:          nil,
		activePairLengths: make(map[int64]int64),
		db:                db,
	}
}

// Len returns the total count of keys (In-Memory + On-Disk).
func (idx *Index) Len() int {
	count := len(idx.active)
	if idx.flushing != nil {
		count += len(idx.flushing)
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
	if ent, ok := idx.searchHistory(idx.active[key], readVersion); ok {
		return ent, true
	}

	// 2. Check Flushing (Warm)
	if idx.flushing != nil {
		if ent, ok := idx.searchHistory(idx.flushing[key], readVersion); ok {
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
	if hist := idx.active[key]; len(hist) > 0 {
		return idx.checkLatest(hist)
	}
	if idx.flushing != nil {
		if hist := idx.flushing[key]; len(hist) > 0 {
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
	idx.active[key] = append(idx.active[key], entry)

	history := idx.active[key]
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
			idx.active[key] = newHist
		}
	}

	// 2. Update Pair Length Index (for Sync safety)
	idx.activePairLengths[offset] = length
}

func (idx *Index) Rotate() bool {
	if len(idx.active) == 0 && len(idx.activePairLengths) == 0 {
		return false
	}
	if idx.flushing != nil {
		return false
	}
	idx.flushing = idx.active
	idx.active = make(map[string][]IndexEntry)

	idx.flushingPairLengths = idx.activePairLengths
	idx.activePairLengths = make(map[int64]int64)

	return true
}

func (idx *Index) FlushToBolt(minReadVersion int64) error {
	// If nothing to flush (neither keys nor pairs), return
	if idx.flushing == nil && idx.flushingPairLengths == nil {
		return nil
	}

	keys := make([]string, 0, len(idx.flushing))
	for k := range idx.flushing {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	err := idx.db.Update(func(tx *bbolt.Tx) error {
		// 1. Flush Keys
		if idx.flushing != nil {
			b, err := tx.CreateBucketIfNotExists([]byte(BoltBucketData))
			if err != nil {
				return err
			}
			b.FillPercent = 0.9
			meta, err := tx.CreateBucketIfNotExists([]byte(BoltBucketMeta))
			if err != nil {
				return err
			}

			var maxOffset int64

			for _, key := range keys {
				memHist := idx.flushing[key]
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
		}

		// 2. Flush Pair Lengths
		if idx.flushingPairLengths != nil {
			bPair, err := tx.CreateBucketIfNotExists([]byte(BoltBucketPair))
			if err != nil {
				return err
			}
			bPair.FillPercent = 0.9

			// Sort by offset for sequential writes
			offsets := make([]int64, 0, len(idx.flushingPairLengths))
			for off := range idx.flushingPairLengths {
				offsets = append(offsets, off)
			}
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })

			for _, off := range offsets {
				length := idx.flushingPairLengths[off]

				// SAFETY: Allocate new buffers for each entry to avoid any reuse issues.
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
	idx.flushingPairLengths = nil
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

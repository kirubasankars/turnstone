package main

import (
	"bytes"
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
	db       *bbolt.DB               // Persistent storage
}

func NewIndex(db *bbolt.DB) *Index {
	return &Index{
		active:   make(map[string][]IndexEntry),
		flushing: nil,
		db:       db,
	}
}

// Len returns the total count of keys (In-Memory + On-Disk).
func (idx *Index) Len() int {
	count := len(idx.active)
	if idx.flushing != nil {
		count += len(idx.flushing)
	}

	idx.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketData))
		if b != nil {
			// Bucket Stats() is fast (O(1)) as it reads metadata.
			// It counts the total number of keys in the B+Tree.
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
	idx.db.View(func(tx *bbolt.Tx) error {
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
	idx.db.View(func(tx *bbolt.Tx) error {
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

func (idx *Index) Set(key string, offset int64, deleted bool, minReadVersion int64) {
	entry := packIndexEntry(offset, deleted)
	idx.active[key] = append(idx.active[key], entry)

	history := idx.active[key]
	if len(history) <= 1 {
		return
	}

	pivot := -1
	for i := range len(history) - 1 {
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

func (idx *Index) Rotate() bool {
	if len(idx.active) == 0 {
		return false
	}
	if idx.flushing != nil {
		return false
	}
	idx.flushing = idx.active
	idx.active = make(map[string][]IndexEntry)
	return true
}

func (idx *Index) FlushToBolt(minReadVersion int64) error {
	if idx.flushing == nil {
		return nil
	}

	keys := make([]string, 0, len(idx.flushing))
	for k := range idx.flushing {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	err := idx.db.Update(func(tx *bbolt.Tx) error {
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
			var buf [8]byte
			binary.BigEndian.PutUint64(buf[:], uint64(maxOffset))
			if err := meta.Put([]byte(KeyLastOffset), buf[:]); err != nil {
				return err
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
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(hist)))
	for _, entry := range hist {
		binary.Write(buf, binary.LittleEndian, uint64(entry))
	}
	return buf.Bytes()
}

func (idx *Index) decodeHistory(data []byte) []IndexEntry {
	if len(data) < 4 {
		return nil
	}
	r := bytes.NewReader(data)
	var count uint32
	binary.Read(r, binary.LittleEndian, &count)
	hist := make([]IndexEntry, count)
	for i := 0; i < int(count); i++ {
		var packed uint64
		binary.Read(r, binary.LittleEndian, &packed)
		hist[i] = IndexEntry(packed)
	}
	return hist
}

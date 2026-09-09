// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"

	"turnstone/stonedb/btree"
)

const indexVersionSize = 29 // offset(8)+valueLen(4)+xmin(8)+opID(8)+tombstone(1)

// Index is an MVCC index backed by a memory-mapped B+ tree (stonedb/btree).
type Index struct {
	tree *btree.Tree
}

// OpenIndex opens (or recreates) the B+ tree index under dbDir/index.
// The log replay on Open rebuilds index contents from scratch.
func OpenIndex(dbDir string) (*Index, error) {
	indexDir := filepath.Join(dbDir, "index")
	if err := os.RemoveAll(indexDir); err != nil {
		return nil, err
	}
	tree, err := btree.Open(indexDir)
	if err != nil {
		return nil, err
	}
	return &Index{tree: tree}, nil
}

func (idx *Index) Close() error {
	if idx.tree == nil {
		return nil
	}
	return idx.tree.Close()
}

func (idx *Index) Put(key []byte, v indexVersion) {
	_ = idx.tree.Put(encodeIndexKey(key, v.xmin), encodeIndexVersion(v))
}

func (idx *Index) DropXid(xid uint64) {
	var toDelete [][]byte
	idx.scanEntries(func(uk []byte, xmin uint64, idxKey []byte, _ indexVersion) {
		if xmin == xid {
			toDelete = append(toDelete, append([]byte(nil), idxKey...))
		}
	})
	for _, k := range toDelete {
		_ = idx.tree.Delete(k)
	}
}

func (idx *Index) LatestResolved(key []byte, excludeXid uint64, clog func(uint64) TxStatus) (*indexVersion, uint64, bool) {
	var found *indexVersion
	var foundXmin uint64
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if v.xmin == excludeXid {
			return true
		}
		if clog(v.xmin) == TxAborted {
			return true
		}
		cp := v
		found = &cp
		foundXmin = v.xmin
		return false
	})
	if found == nil {
		return nil, 0, false
	}
	return found, foundXmin, true
}

func (idx *Index) GetVisible(key []byte, snap Snapshot, myXid uint64, update bool, visible func(uint64, Snapshot) bool) (*indexVersion, bool) {
	var found *indexVersion
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if update && v.xmin == myXid {
			cp := v
			found = &cp
			return false
		}
		if visible(v.xmin, snap) {
			cp := v
			found = &cp
			return false
		}
		return true
	})
	if found == nil {
		return nil, false
	}
	return found, true
}

func (idx *Index) HasNewerCommitted(key []byte, excludeXid uint64, snap Snapshot, clog func(uint64) TxStatus) bool {
	found := false
	idx.walkKeyVersions(key, func(v indexVersion) bool {
		if v.xmin == excludeXid {
			return true
		}
		if clog(v.xmin) != TxCommitted {
			return true
		}
		found = v.xmin >= snap.Xmax || snap.contains(v.xmin)
		return false
	})
	return found
}

func (idx *Index) ForEachKey(fn func(key []byte, chain []indexVersion)) {
	it := idx.tree.NewIterator(nil)
	defer it.Release()
	if !it.First() {
		return
	}

	var curKey []byte
	var chain []indexVersion
	flush := func() {
		if curKey != nil && len(chain) > 0 {
			fn(curKey, chain)
		}
	}

	for {
		uk, xmin, err := decodeIndexKey(it.Key())
		if err != nil {
			break
		}
		ver, err := decodeIndexVersion(it.Value())
		if err != nil {
			break
		}
		ver.xmin = xmin
		if curKey != nil && !bytes.Equal(curKey, uk) {
			flush()
			chain = chain[:0]
		}
		curKey = append(curKey[:0], uk...)
		chain = append(chain, ver)
		if !it.Next() {
			break
		}
	}
	flush()
}

func (idx *Index) RemoveVersion(key []byte, xmin uint64) {
	_ = idx.tree.Delete(encodeIndexKey(key, xmin))
}

func (idx *Index) LiveKeyCount(clog func(uint64) TxStatus) int64 {
	var count int64
	idx.ForEachKey(func(_ []byte, chain []indexVersion) {
		for _, v := range chain {
			if clog(v.xmin) == TxCommitted && !v.tombstone {
				count++
				break
			}
		}
	})
	return count
}

func (idx *Index) collectDeadVersions(horizon uint64, clog func(uint64) TxStatus) []struct {
	key string
	ver indexVersion
} {
	var dead []struct {
		key string
		ver indexVersion
	}

	idx.ForEachKey(func(uk []byte, chain []indexVersion) {
		var newestCommitted *indexVersion
		for i := range chain {
			v := chain[i]
			if clog(v.xmin) == TxCommitted {
				cp := v
				newestCommitted = &cp
				break
			}
		}

		k := string(uk)
		for _, v := range chain {
			if clog(v.xmin) == TxInProgress {
				continue
			}
			if clog(v.xmin) == TxAborted {
				dead = append(dead, struct {
					key string
					ver indexVersion
				}{k, v})
				continue
			}
			isNewest := newestCommitted != nil && v.offset == newestCommitted.offset
			if isNewest {
				continue
			}
			if v.xmin < horizon {
				dead = append(dead, struct {
					key string
					ver indexVersion
				}{k, v})
			}
		}
	})
	return dead
}

func (idx *Index) RemoveDead(dead []struct {
	key string
	ver indexVersion
}) {
	for _, d := range dead {
		_ = idx.tree.Delete(encodeIndexKey([]byte(d.key), d.ver.xmin))
	}
}

func (idx *Index) HasLiveRefAtOffset(offset int64) bool {
	found := false
	idx.scanEntries(func(_ []byte, _ uint64, _ []byte, v indexVersion) {
		if v.offset == offset {
			found = true
		}
	})
	return found
}

func (idx *Index) KeysEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

func (idx *Index) CountKeys() int {
	n := 0
	idx.ForEachKey(func(_ []byte, _ []indexVersion) {
		n++
	})
	return n
}

func (idx *Index) walkKeyVersions(key []byte, fn func(indexVersion) bool) {
	rng := userKeyRange(key)
	it := idx.tree.NewIterator(rng)
	defer it.Release()
	if !it.First() {
		return
	}
	for it.Valid() {
		uk, xmin, err := decodeIndexKey(it.Key())
		if err != nil || !bytes.Equal(uk, key) {
			break
		}
		ver, err := decodeIndexVersion(it.Value())
		if err != nil {
			break
		}
		ver.xmin = xmin
		if !fn(ver) {
			break
		}
		if !it.Next() {
			break
		}
	}
}

func (idx *Index) scanEntries(fn func(userKey []byte, xmin uint64, idxKey []byte, ver indexVersion)) {
	it := idx.tree.NewIterator(nil)
	defer it.Release()
	if !it.First() {
		return
	}
	for it.Valid() {
		idxKey := append([]byte(nil), it.Key()...)
		uk, xmin, err := decodeIndexKey(idxKey)
		if err != nil {
			break
		}
		ver, err := decodeIndexVersion(it.Value())
		if err != nil {
			break
		}
		ver.xmin = xmin
		fn(uk, xmin, idxKey, ver)
		if !it.Next() {
			break
		}
	}
}

func userKeyRange(userKey []byte) *btree.Range {
	start := encodeIndexKey(userKey, math.MaxUint64)
	limit := indexKeyUpperBound(userKey)
	if limit == nil {
		return &btree.Range{Start: start}
	}
	return &btree.Range{Start: start, Limit: limit}
}

func indexKeyUpperBound(userKey []byte) []byte {
	if len(userKey) == 0 {
		return []byte{0x00}
	}
	next := make([]byte, len(userKey))
	copy(next, userKey)
	for i := len(next) - 1; i >= 0; i-- {
		next[i]++
		if next[i] != 0 {
			return append(next, 0x00)
		}
	}
	return nil
}

func encodeIndexKey(key []byte, xmin uint64) []byte {
	out := make([]byte, len(key)+9)
	copy(out, key)
	out[len(key)] = 0x00
	binary.BigEndian.PutUint64(out[len(key)+1:], math.MaxUint64-xmin)
	return out
}

func decodeIndexKey(data []byte) ([]byte, uint64, error) {
	if len(data) < 9 {
		return nil, 0, errors.New("invalid index key length")
	}
	userKey := data[:len(data)-9]
	invTs := binary.BigEndian.Uint64(data[len(data)-8:])
	return userKey, math.MaxUint64 - invTs, nil
}

func encodeIndexVersion(v indexVersion) []byte {
	buf := make([]byte, indexVersionSize)
	binary.BigEndian.PutUint64(buf[0:], uint64(v.offset))
	binary.BigEndian.PutUint32(buf[8:], v.valueLen)
	binary.BigEndian.PutUint64(buf[12:], v.xmin)
	binary.BigEndian.PutUint64(buf[20:], v.opID)
	if v.tombstone {
		buf[28] = 1
	}
	return buf
}

func decodeIndexVersion(data []byte) (indexVersion, error) {
	if len(data) < indexVersionSize {
		return indexVersion{}, errors.New("invalid index version length")
	}
	return indexVersion{
		offset:    int64(binary.BigEndian.Uint64(data[0:])),
		valueLen:  binary.BigEndian.Uint32(data[8:]),
		xmin:      binary.BigEndian.Uint64(data[12:]),
		opID:      binary.BigEndian.Uint64(data[20:]),
		tombstone: data[28] == 1,
	}, nil
}

func alignRange(start, end, blockSize int64) (int64, int64) {
	if blockSize <= 0 {
		blockSize = 4096
	}
	alignedStart := (start + blockSize - 1) / blockSize * blockSize
	alignedEnd := end / blockSize * blockSize
	if alignedEnd <= alignedStart {
		return 0, 0
	}
	return alignedStart, alignedEnd
}

func recordSpanSize(keyLen, valLen int, recType WALRecordType) int64 {
	var body int
	switch recType {
	case WALRecordSet:
		body = 4 + keyLen + 4 + valLen
	case WALRecordDelete:
		body = 4 + keyLen
	}
	return frameSize(LogRecordHeaderSize + body)
}

// visibleVersionForKey returns the first visible version for key in snapshot.
func visibleVersionForKey(chain []indexVersion, snap Snapshot, myXid uint64, update bool, isVisible func(uint64, Snapshot) bool) (*indexVersion, bool) {
	for _, v := range chain {
		if update && v.xmin == myXid {
			cp := v
			return &cp, true
		}
		if isVisible(v.xmin, snap) {
			cp := v
			return &cp, true
		}
	}
	return nil, false
}

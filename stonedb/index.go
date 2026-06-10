package stonedb

import (
	"bytes"
	"sync"
)

// MemIndex is an in-memory MVCC index: key -> version chain (newest xmin first).
type MemIndex struct {
	mu       sync.RWMutex
	versions map[string][]indexVersion
}

func NewMemIndex() *MemIndex {
	return &MemIndex{versions: make(map[string][]indexVersion)}
}

func (idx *MemIndex) Put(key []byte, v indexVersion) {
	keyStr := string(key)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	chain := idx.versions[keyStr]
	idx.versions[keyStr] = append([]indexVersion{v}, chain...)
}

func (idx *MemIndex) DropXid(xid uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for k, chain := range idx.versions {
		filtered := chain[:0]
		for _, v := range chain {
			if v.xmin != xid {
				filtered = append(filtered, v)
			}
		}
		if len(filtered) == 0 {
			delete(idx.versions, k)
		} else {
			idx.versions[k] = filtered
		}
	}
}

func (idx *MemIndex) LatestResolved(key []byte, excludeXid uint64, clog func(uint64) TxStatus) (*indexVersion, uint64, bool) {
	keyStr := string(key)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, v := range idx.versions[keyStr] {
		if v.xmin == excludeXid {
			continue
		}
		st := clog(v.xmin)
		if st == TxAborted {
			continue
		}
		ver := v
		return &ver, v.xmin, true
	}
	return nil, 0, false
}

func (idx *MemIndex) GetVisible(key []byte, snap Snapshot, myXid uint64, update bool, visible func(uint64, Snapshot) bool) (*indexVersion, bool) {
	keyStr := string(key)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, v := range idx.versions[keyStr] {
		if update && v.xmin == myXid {
			ver := v
			return &ver, true
		}
		if visible(v.xmin, snap) {
			ver := v
			return &ver, true
		}
	}
	return nil, false
}

func (idx *MemIndex) HasNewerCommitted(key []byte, excludeXid uint64, snap Snapshot, clog func(uint64) TxStatus) bool {
	keyStr := string(key)
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, v := range idx.versions[keyStr] {
		if v.xmin == excludeXid {
			continue
		}
		if clog(v.xmin) != TxCommitted {
			continue
		}
		return v.xmin >= snap.Xmax || snap.contains(v.xmin)
	}
	return false
}

func (idx *MemIndex) ForEachKey(fn func(key []byte, chain []indexVersion)) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for k, chain := range idx.versions {
		cp := append([]indexVersion(nil), chain...)
		fn([]byte(k), cp)
	}
}

func (idx *MemIndex) RemoveVersion(key []byte, xmin uint64) {
	keyStr := string(key)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	chain := idx.versions[keyStr]
	filtered := chain[:0]
	for _, v := range chain {
		if v.xmin != xmin {
			filtered = append(filtered, v)
		}
	}
	if len(filtered) == 0 {
		delete(idx.versions, keyStr)
	} else {
		idx.versions[keyStr] = filtered
	}
}

func (idx *MemIndex) LiveKeyCount(clog func(uint64) TxStatus) int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var count int64
	for _, chain := range idx.versions {
		for _, v := range chain {
			if clog(v.xmin) == TxCommitted && !v.tombstone {
				count++
				break
			}
		}
	}
	return count
}

// collectDeadVersions returns index versions that can be dropped from the
// hashmap and their on-disk spans eligible for punch (subject to external floors).
func (idx *MemIndex) collectDeadVersions(horizon uint64, clog func(uint64) TxStatus) []struct {
	key string
	ver indexVersion
} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var dead []struct {
		key string
		ver indexVersion
	}

	for k, chain := range idx.versions {
		var newestCommitted *indexVersion
		for i := range chain {
			v := chain[i]
			if clog(v.xmin) == TxCommitted {
				if newestCommitted == nil {
					cp := v
					newestCommitted = &cp
				}
				break
			}
		}

		for i, v := range chain {
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
			// committed
			isNewest := newestCommitted != nil && v.offset == newestCommitted.offset
			if isNewest {
				continue
			}
			if v.xmin < horizon {
				dead = append(dead, struct {
					key string
					ver indexVersion
				}{k, v})
				continue
			}
			_ = i
		}
	}
	return dead
}

func (idx *MemIndex) RemoveDead(dead []struct {
	key string
	ver indexVersion
}) {
	if len(dead) == 0 {
		return
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, d := range dead {
		chain := idx.versions[d.key]
		filtered := chain[:0]
		for _, v := range chain {
			if v.offset != d.ver.offset || v.xmin != d.ver.xmin {
				filtered = append(filtered, v)
			}
		}
		if len(filtered) == 0 {
			delete(idx.versions, d.key)
		} else {
			idx.versions[d.key] = filtered
		}
	}
}

func (idx *MemIndex) HasLiveRefAtOffset(offset int64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, chain := range idx.versions {
		for _, v := range chain {
			if v.offset == offset {
				return true
			}
		}
	}
	return false
}

func (idx *MemIndex) KeysEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
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

func (idx *MemIndex) CountKeys() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.versions)
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


// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package segindex

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"turnstone/stonedb/mmapfile"
)

const (
	numSegments   = 256
	initialSlots  = 1024
	headerSize    = mmapfile.PageSize
	versionSize   = 29
	versionNodeSz = versionSize + 8 // next pointer
	magic         = uint64(0x5447534547) // "TGSEG"
	formatVersion = uint32(1)
)

// Version is one MVCC index entry pointing at a log record.
type Version struct {
	Offset    int64
	ValueLen  uint32
	Xmin      uint64
	OpID      uint64
	Tombstone bool
}

// SegmentedIndex is a sharded mmap hash index with per-segment locking.
type SegmentedIndex struct {
	dir      string
	segments [numSegments]*segment
}

// Open creates or opens numSegments mmap segment files under dir.
func Open(dir string) (*SegmentedIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	idx := &SegmentedIndex{dir: dir}
	for i := 0; i < numSegments; i++ {
		path := filepath.Join(dir, fmt.Sprintf("seg-%03d.bin", i))
		seg, err := openSegment(path)
		if err != nil {
			idx.Close()
			return nil, err
		}
		idx.segments[i] = seg
	}
	return idx, nil
}

func (idx *SegmentedIndex) Close() error {
	var first error
	for _, seg := range idx.segments {
		if seg == nil {
			continue
		}
		if err := seg.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (idx *SegmentedIndex) segmentFor(key []byte) *segment {
	return idx.segments[int(hashKey(key)&255)]
}

func (idx *SegmentedIndex) Put(key []byte, ver Version) {
	seg := idx.segmentFor(key)
	seg.put(key, ver)
}

func (idx *SegmentedIndex) WalkVersions(key []byte, fn func(Version) bool) {
	seg := idx.segmentFor(key)
	seg.walkVersions(key, fn)
}

func (idx *SegmentedIndex) DropXid(xid uint64) {
	for _, seg := range idx.segments {
		if seg != nil {
			seg.dropXid(xid)
		}
	}
}

func (idx *SegmentedIndex) ForEachKey(fn func(key []byte, chain []Version)) {
	for _, seg := range idx.segments {
		if seg != nil {
			seg.forEachKey(fn)
		}
	}
}

func (idx *SegmentedIndex) RemoveVersion(key []byte, xmin uint64) {
	seg := idx.segmentFor(key)
	seg.removeVersion(key, xmin)
}

func (idx *SegmentedIndex) HasLiveRefAtOffset(offset int64) bool {
	for _, seg := range idx.segments {
		if seg != nil && seg.hasOffset(offset) {
			return true
		}
	}
	return false
}

type segment struct {
	mu sync.RWMutex
	mf *mmapfile.File
}

func openSegment(path string) (*segment, error) {
	tableBytes := int64(initialSlots * 8)
	minSize := int64(headerSize) + tableBytes + mmapfile.PageSize
	mf, err := mmapfile.Open(path, minSize)
	if err != nil {
		return nil, err
	}
	seg := &segment{mf: mf}
	if readU64(mf.Data(), hdrMagicOff) != magic {
		seg.initNew(initialSlots)
	}
	return seg, nil
}

func (s *segment) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mf == nil {
		return nil
	}
	err := s.mf.Close()
	s.mf = nil
	return err
}

const (
	hdrMagicOff     = 0
	hdrVersionOff   = 8
	hdrSlotCountOff = 12
	hdrKeyCountOff  = 16
	hdrTableOffOff  = 24
	hdrArenaOffOff  = 32
	hdrArenaUsedOff = 40
)

func (s *segment) initNew(slotCount uint32) {
	data := s.mf.Data()
	writeU64(data, hdrMagicOff, magic)
	writeU32(data, hdrVersionOff, formatVersion)
	writeU32(data, hdrSlotCountOff, slotCount)
	writeU32(data, hdrKeyCountOff, 0)
	tableOff := uint64(headerSize)
	arenaOff := tableOff + uint64(slotCount)*8
	writeU64(data, hdrTableOffOff, tableOff)
	writeU64(data, hdrArenaOffOff, arenaOff)
	writeU64(data, hdrArenaUsedOff, 0)
}

func (s *segment) slotCount() uint32 {
	return readU32(s.mf.Data(), hdrSlotCountOff)
}

func (s *segment) keyCount() uint32 {
	return readU32(s.mf.Data(), hdrKeyCountOff)
}

func (s *segment) setKeyCount(n uint32) {
	writeU32(s.mf.Data(), hdrKeyCountOff, n)
}

func (s *segment) tableOff() uint64 {
	return readU64(s.mf.Data(), hdrTableOffOff)
}

func (s *segment) arenaOff() uint64 {
	return readU64(s.mf.Data(), hdrArenaOffOff)
}

func (s *segment) arenaUsed() uint64 {
	return readU64(s.mf.Data(), hdrArenaUsedOff)
}

func (s *segment) setArenaUsed(n uint64) {
	writeU64(s.mf.Data(), hdrArenaUsedOff, n)
}

func (s *segment) alloc(size int) (uint64, error) {
	off := s.arenaOff() + s.arenaUsed()
	need := int64(off) + int64(size)
	if need > int64(len(s.mf.Data())) {
		if err := s.mf.Grow(need); err != nil {
			return 0, err
		}
	}
	s.setArenaUsed(s.arenaUsed() + uint64(size))
	return off, nil
}

func (s *segment) slotIndex(key []byte) uint32 {
	return uint32(hashKey(key) % uint64(s.slotCount()))
}

func (s *segment) findKeyRecord(key []byte) (uint64, bool) {
	slots := s.slotCount()
	start := s.slotIndex(key)
	data := s.mf.Data()
	table := int(s.tableOff())
	for i := uint32(0); i < slots; i++ {
		slot := (start + i) % slots
		off := readU64(data, table+int(slot)*8)
		if off == 0 {
			return 0, false
		}
		if s.keyAt(off, key) {
			return off, true
		}
	}
	return 0, false
}

func (s *segment) findOrCreateKeyRecord(key []byte) (uint64, error) {
	if off, ok := s.findKeyRecord(key); ok {
		return off, nil
	}
	slots := s.slotCount()
	start := s.slotIndex(key)
	data := s.mf.Data()
	table := int(s.tableOff())
	recSize := 12 + len(key)
	off, err := s.alloc(recSize)
	if err != nil {
		return 0, err
	}
	data = s.mf.Data()
	writeU32(data, int(off), uint32(len(key)))
	writeU64(data, int(off)+4, 0)
	copy(data[int(off)+12:], key)

	for i := uint32(0); i < slots; i++ {
		slot := (start + i) % slots
		slotOff := table + int(slot)*8
		if readU64(data, slotOff) == 0 {
			writeU64(data, slotOff, off)
			s.setKeyCount(s.keyCount() + 1)
			return off, nil
		}
	}
	return 0, fmt.Errorf("segment hash table full")
}

func (s *segment) keyAt(recOff uint64, key []byte) bool {
	data := s.mf.Data()
	if int(recOff)+12 > len(data) {
		return false
	}
	kLen := readU32(data, int(recOff))
	if int(kLen) != len(key) {
		return false
	}
	if int(recOff)+12+int(kLen) > len(data) {
		return false
	}
	return string(data[int(recOff)+12:int(recOff)+12+int(kLen)]) == string(key)
}

func (s *segment) readKey(recOff uint64) []byte {
	data := s.mf.Data()
	kLen := readU32(data, int(recOff))
	out := make([]byte, kLen)
	copy(out, data[int(recOff)+12:int(recOff)+12+int(kLen)])
	return out
}

func (s *segment) versionHead(recOff uint64) uint64 {
	return readU64(s.mf.Data(), int(recOff)+4)
}

func (s *segment) setVersionHead(recOff, head uint64) {
	writeU64(s.mf.Data(), int(recOff)+4, head)
}

func (s *segment) put(key []byte, ver Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mf == nil {
		return
	}

	recOff, err := s.findOrCreateKeyRecord(key)
	if err != nil {
		panic("segindex: " + err.Error())
	}
	nodeOff, err := s.alloc(versionNodeSz)
	if err != nil {
		panic("segindex: alloc version: " + err.Error())
	}
	writeVersion(s.mf.Data(), int(nodeOff), ver)
	writeU64(s.mf.Data(), int(nodeOff)+versionSize, s.versionHead(recOff))
	s.setVersionHead(recOff, nodeOff)
}

func (s *segment) walkVersions(key []byte, fn func(Version) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mf == nil {
		return
	}

	recOff, ok := s.findKeyRecord(key)
	if !ok {
		return
	}
	node := s.versionHead(recOff)
	data := s.mf.Data()
	for node != 0 {
		if int(node)+versionNodeSz > len(data) {
			break
		}
		ver := readVersion(data, int(node))
		if !fn(ver) {
			break
		}
		node = readU64(data, int(node)+versionSize)
	}
}

func (s *segment) forEachKey(fn func(key []byte, chain []Version)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mf == nil {
		return
	}

	data := s.mf.Data()
	slots := s.slotCount()
	table := int(s.tableOff())
	for slot := uint32(0); slot < slots; slot++ {
		recOff := readU64(data, table+int(slot)*8)
		if recOff == 0 {
			continue
		}
		key := s.readKey(recOff)
		var chain []Version
		node := s.versionHead(recOff)
		for node != 0 {
			if int(node)+versionNodeSz > len(data) {
				break
			}
			chain = append(chain, readVersion(data, int(node)))
			node = readU64(data, int(node)+versionSize)
		}
		if len(chain) > 0 {
			fn(key, chain)
		}
	}
}

func (s *segment) dropXid(xid uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mf == nil {
		return
	}

	slots := s.slotCount()
	table := int(s.tableOff())
	for slot := uint32(0); slot < slots; slot++ {
		data := s.mf.Data()
		slotOff := table + int(slot)*8
		recOff := readU64(data, slotOff)
		if recOff == 0 {
			continue
		}
		newHead, empty := s.filterChain(recOff, func(v Version) bool { return v.Xmin != xid })
		if empty {
			writeU64(s.mf.Data(), slotOff, 0)
			s.setKeyCount(s.keyCount() - 1)
		} else {
			s.setVersionHead(recOff, newHead)
		}
	}
}

func (s *segment) removeVersion(key []byte, xmin uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mf == nil {
		return
	}

	recOff, ok := s.findKeyRecord(key)
	if !ok {
		return
	}
	data := s.mf.Data()
	slots := s.slotCount()
	table := int(s.tableOff())
	var slotIdx int = -1
	start := s.slotIndex(key)
	for i := uint32(0); i < slots; i++ {
		slot := (start + i) % slots
		if readU64(data, table+int(slot)*8) == recOff {
			slotIdx = table + int(slot)*8
			break
		}
	}
	newHead, empty := s.filterChain(recOff, func(v Version) bool { return v.Xmin != xmin })
	if empty {
		if slotIdx >= 0 {
			writeU64(s.mf.Data(), slotIdx, 0)
		}
		s.setKeyCount(s.keyCount() - 1)
	} else {
		s.setVersionHead(recOff, newHead)
	}
}

func (s *segment) filterChain(recOff uint64, keep func(Version) bool) (uint64, bool) {
	data := s.mf.Data()
	head := s.versionHead(recOff)
	var kept []Version
	for node := head; node != 0; node = readU64(data, int(node)+versionSize) {
		if int(node)+versionNodeSz > len(data) {
			break
		}
		v := readVersion(data, int(node))
		if keep(v) {
			kept = append(kept, v)
		}
	}
	if len(kept) == 0 {
		return 0, true
	}
	var newHead uint64
	for i := len(kept) - 1; i >= 0; i-- {
		nodeOff, err := s.alloc(versionNodeSz)
		if err != nil {
			panic("segindex: filterChain alloc: " + err.Error())
		}
		data = s.mf.Data()
		writeVersion(data, int(nodeOff), kept[i])
		writeU64(data, int(nodeOff)+versionSize, newHead)
		newHead = nodeOff
	}
	return newHead, false
}

func (s *segment) hasOffset(target int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.mf == nil {
		return false
	}

	found := false
	s.forEachKeyLocked(func(_ []byte, chain []Version) {
		if found {
			return
		}
		for _, v := range chain {
			if v.Offset == target {
				found = true
				return
			}
		}
	})
	return found
}

func (s *segment) forEachKeyLocked(fn func(key []byte, chain []Version)) {
	data := s.mf.Data()
	slots := s.slotCount()
	table := int(s.tableOff())
	for slot := uint32(0); slot < slots; slot++ {
		recOff := readU64(data, table+int(slot)*8)
		if recOff == 0 {
			continue
		}
		key := s.readKey(recOff)
		var chain []Version
		node := s.versionHead(recOff)
		for node != 0 {
			if int(node)+versionNodeSz > len(data) {
				break
			}
			chain = append(chain, readVersion(data, int(node)))
			node = readU64(data, int(node)+versionSize)
		}
		if len(chain) > 0 {
			fn(key, chain)
		}
	}
}

func hashKey(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

func writeVersion(buf []byte, off int, v Version) {
	binary.BigEndian.PutUint64(buf[off:], uint64(v.Offset))
	binary.BigEndian.PutUint32(buf[off+8:], v.ValueLen)
	binary.BigEndian.PutUint64(buf[off+12:], v.Xmin)
	binary.BigEndian.PutUint64(buf[off+20:], v.OpID)
	if v.Tombstone {
		buf[off+28] = 1
	} else {
		buf[off+28] = 0
	}
}

func readVersion(buf []byte, off int) Version {
	return Version{
		Offset:    int64(binary.BigEndian.Uint64(buf[off:])),
		ValueLen:  binary.BigEndian.Uint32(buf[off+8:]),
		Xmin:      binary.BigEndian.Uint64(buf[off+12:]),
		OpID:      binary.BigEndian.Uint64(buf[off+20:]),
		Tombstone: buf[off+28] == 1,
	}
}

func readU64(buf []byte, off int) uint64 {
	return binary.BigEndian.Uint64(buf[off:])
}

func writeU64(buf []byte, off int, v uint64) {
	binary.BigEndian.PutUint64(buf[off:], v)
}

func readU32(buf []byte, off int) uint32 {
	return binary.BigEndian.Uint32(buf[off:])
}

func writeU32(buf []byte, off int, v uint32) {
	binary.BigEndian.PutUint32(buf[off:], v)
}

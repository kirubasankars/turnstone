// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package segindex

import (
	"encoding/binary"
	"fmt"
	"sync"
)

const (
	numSegments      = 256
	initialSlots     = 1024
	maxLoadFactorNum = 3 // grow when keyCount*4 > slotCount*3
	maxLoadFactorDen = 4
	headerSize       = 4096
	versionSize      = 21
	versionNodeSz    = versionSize + 8      // next pointer
	magic            = uint64(0x5447534547) // "TGSEG"
	formatVersion    = uint32(1)
)

// Version is one MVCC index entry pointing at a log record.
type Version struct {
	Offset    int64
	ValueLen  uint32
	Xmin      uint64
	Tombstone bool
}

// SegmentedIndex is a sharded in-memory hash index with per-segment locking.
type SegmentedIndex struct {
	segments [numSegments]*segment
}

// Open creates numSegments heap-backed index segments.
func Open() *SegmentedIndex {
	idx := &SegmentedIndex{}
	for i := 0; i < numSegments; i++ {
		idx.segments[i] = newSegment()
	}
	return idx
}

func (idx *SegmentedIndex) Close() error {
	for _, seg := range idx.segments {
		if seg != nil {
			seg.close()
		}
	}
	return nil
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

type segment struct {
	mu   sync.RWMutex
	data []byte
}

func newSegment() *segment {
	tableBytes := int64(initialSlots * 8)
	minSize := int64(headerSize) + tableBytes + headerSize
	s := &segment{data: make([]byte, minSize)}
	s.initNew(initialSlots)
	return s
}

func (s *segment) close() {
	s.mu.Lock()
	s.data = nil
	s.mu.Unlock()
}

func (s *segment) grow(minSize int64) {
	if minSize <= int64(len(s.data)) {
		return
	}
	n := len(s.data)
	if n == 0 {
		n = int(minSize)
	}
	for int64(n) < minSize {
		n *= 2
	}
	out := make([]byte, n)
	copy(out, s.data)
	s.data = out
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
	data := s.data
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
	return readU32(s.data, hdrSlotCountOff)
}

func (s *segment) keyCount() uint32 {
	return readU32(s.data, hdrKeyCountOff)
}

func (s *segment) setKeyCount(n uint32) {
	writeU32(s.data, hdrKeyCountOff, n)
}

func (s *segment) tableOff() uint64 {
	return readU64(s.data, hdrTableOffOff)
}

func (s *segment) arenaOff() uint64 {
	return readU64(s.data, hdrArenaOffOff)
}

func (s *segment) arenaUsed() uint64 {
	return readU64(s.data, hdrArenaUsedOff)
}

func (s *segment) setArenaUsed(n uint64) {
	writeU64(s.data, hdrArenaUsedOff, n)
}

func (s *segment) alloc(size int) (uint64, error) {
	off := s.arenaOff() + s.arenaUsed()
	need := int64(off) + int64(size)
	if need > int64(len(s.data)) {
		s.grow(need)
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
	data := s.data
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

func (s *segment) tableLoadHigh() bool {
	slots := s.slotCount()
	if slots == 0 {
		return true
	}
	return uint64(s.keyCount()+1)*uint64(maxLoadFactorDen) > uint64(slots)*uint64(maxLoadFactorNum)
}

func (s *segment) bumpArenaChainRefs(recOff uint64, delta uint64) {
	head := s.versionHead(recOff)
	if head == 0 {
		return
	}
	newHead := head + delta
	s.setVersionHead(recOff, newHead)
	node := newHead
	data := s.data
	for node != 0 {
		nextOff := readU64(data, int(node)+versionSize)
		if nextOff != 0 {
			writeU64(data, int(node)+versionSize, nextOff+delta)
			node = nextOff + delta
		} else {
			node = 0
		}
	}
}

func readKeyFromArena(buf []byte, recOff, arenaStart uint64) []byte {
	rel := int(recOff - arenaStart)
	if rel+12 > len(buf) {
		return nil
	}
	kLen := readU32(buf, rel)
	out := make([]byte, kLen)
	copy(out, buf[rel+12:rel+12+int(kLen)])
	return out
}

func (s *segment) growHashTable() error {
	oldSlots := s.slotCount()
	newSlots := oldSlots * 2
	if newSlots <= oldSlots {
		return fmt.Errorf("segment hash table slot overflow")
	}
	tableStart := s.tableOff()
	arenaStart := s.arenaOff()
	used := s.arenaUsed()
	newTableBytes := uint64(newSlots) * 8
	newArenaStart := tableStart + newTableBytes
	need := int64(newArenaStart + used)
	s.grow(need)
	data := s.data
	arenaSnap := make([]byte, used)
	if used > 0 {
		copy(arenaSnap, data[int(arenaStart):int(arenaStart+used)])
	}
	oldTableInt := int(tableStart)
	delta := newArenaStart - arenaStart
	type entry struct {
		key       []byte
		newRecOff uint64
	}
	var entries []entry
	for slot := uint32(0); slot < oldSlots; slot++ {
		recOff := readU64(data, oldTableInt+int(slot)*8)
		if recOff == 0 {
			continue
		}
		key := readKeyFromArena(arenaSnap, recOff, arenaStart)
		if key == nil {
			return fmt.Errorf("segment hash table grow: bad key record")
		}
		entries = append(entries, entry{
			key:       key,
			newRecOff: recOff - arenaStart + newArenaStart,
		})
	}
	newTable := int(tableStart)
	for i := uint32(0); i < newSlots; i++ {
		writeU64(data, newTable+int(i)*8, 0)
	}
	if used > 0 {
		copy(data[int(newArenaStart):int(newArenaStart+used)], arenaSnap)
	}
	for _, e := range entries {
		s.bumpArenaChainRefs(e.newRecOff, delta)
		start := uint32(hashKey(e.key) % uint64(newSlots))
		inserted := false
		for i := uint32(0); i < newSlots; i++ {
			slotOff := newTable + int((start+i)%newSlots)*8
			if readU64(data, slotOff) == 0 {
				writeU64(data, slotOff, e.newRecOff)
				inserted = true
				break
			}
		}
		if !inserted {
			return fmt.Errorf("segment hash table rehash failed")
		}
	}
	writeU32(data, hdrSlotCountOff, newSlots)
	writeU64(data, hdrArenaOffOff, newArenaStart)
	return nil
}

func (s *segment) insertKeySlot(key []byte, recOff uint64) error {
	slots := s.slotCount()
	start := s.slotIndex(key)
	data := s.data
	table := int(s.tableOff())
	for i := uint32(0); i < slots; i++ {
		slot := (start + i) % slots
		slotOff := table + int(slot)*8
		if readU64(data, slotOff) == 0 {
			writeU64(data, slotOff, recOff)
			s.setKeyCount(s.keyCount() + 1)
			return nil
		}
	}
	return fmt.Errorf("segment hash table full")
}

func (s *segment) findOrCreateKeyRecord(key []byte) (uint64, error) {
	if off, ok := s.findKeyRecord(key); ok {
		return off, nil
	}
	recSize := 12 + len(key)
	for {
		if s.tableLoadHigh() {
			if err := s.growHashTable(); err != nil {
				return 0, err
			}
		}
		off, err := s.alloc(recSize)
		if err != nil {
			return 0, err
		}
		data := s.data
		writeU32(data, int(off), uint32(len(key)))
		writeU64(data, int(off)+4, 0)
		copy(data[int(off)+12:], key)

		if err := s.insertKeySlot(key, off); err == nil {
			return off, nil
		}
		arenaBefore := s.arenaOff()
		if err := s.growHashTable(); err != nil {
			return 0, err
		}
		off += s.arenaOff() - arenaBefore // recOff shifts with arena relocation
		if err := s.insertKeySlot(key, off); err != nil {
			return 0, err
		}
		return off, nil
	}
}

func (s *segment) keyAt(recOff uint64, key []byte) bool {
	data := s.data
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
	data := s.data
	kLen := readU32(data, int(recOff))
	out := make([]byte, kLen)
	copy(out, data[int(recOff)+12:int(recOff)+12+int(kLen)])
	return out
}

func (s *segment) versionHead(recOff uint64) uint64 {
	return readU64(s.data, int(recOff)+4)
}

func (s *segment) setVersionHead(recOff, head uint64) {
	writeU64(s.data, int(recOff)+4, head)
}

func (s *segment) put(key []byte, ver Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
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
	writeVersion(s.data, int(nodeOff), ver)
	writeU64(s.data, int(nodeOff)+versionSize, s.versionHead(recOff))
	s.setVersionHead(recOff, nodeOff)
}

func (s *segment) walkVersions(key []byte, fn func(Version) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.data == nil {
		return
	}

	recOff, ok := s.findKeyRecord(key)
	if !ok {
		return
	}
	node := s.versionHead(recOff)
	data := s.data
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
	if s.data == nil {
		return
	}

	data := s.data
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
	if s.data == nil {
		return
	}

	slots := s.slotCount()
	table := int(s.tableOff())
	for slot := uint32(0); slot < slots; slot++ {
		data := s.data
		slotOff := table + int(slot)*8
		recOff := readU64(data, slotOff)
		if recOff == 0 {
			continue
		}
		newHead, empty := s.filterChain(recOff, func(v Version) bool { return v.Xmin != xid })
		if empty {
			writeU64(s.data, slotOff, 0)
			s.setKeyCount(s.keyCount() - 1)
		} else {
			s.setVersionHead(recOff, newHead)
		}
	}
}

func (s *segment) filterChain(recOff uint64, keep func(Version) bool) (uint64, bool) {
	data := s.data
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
		data = s.data
		writeVersion(data, int(nodeOff), kept[i])
		writeU64(data, int(nodeOff)+versionSize, newHead)
		newHead = nodeOff
	}
	return newHead, false
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
	if v.Tombstone {
		buf[off+20] = 1
	} else {
		buf[off+20] = 0
	}
}

func readVersion(buf []byte, off int) Version {
	return Version{
		Offset:    int64(binary.BigEndian.Uint64(buf[off:])),
		ValueLen:  binary.BigEndian.Uint32(buf[off+8:]),
		Xmin:      binary.BigEndian.Uint64(buf[off+12:]),
		Tombstone: buf[off+20] == 1,
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

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "fmt"

const (
	arenaPageSize = 64 << 10
	pageNextOff   = 0
	pageUsedOff   = 4
	pageHdrSize   = 8
)

func (s *shard) arenaBytes() []byte {
	if s == nil || s.arena == nil {
		return nil
	}
	return s.arena.data
}

func (s *shard) allocatedBytes() int64 {
	var n int64
	if s.buf != nil {
		n += int64(len(s.buf.data))
	}
	if s.arena != nil {
		n += int64(len(s.arena.data))
	}
	return n
}

func (s *shard) arenaHead() uint32 {
	return uint32(readU64(s.metaData(), hdrArenaOffOff))
}

func (s *shard) setArenaHead(off uint32) {
	writeU64(s.metaData(), hdrArenaOffOff, uint64(off))
}

func (s *shard) arenaTail() uint32 {
	return readU32(s.metaData(), hdrArenaTailOff)
}

func (s *shard) setArenaTail(off uint32) {
	writeU32(s.metaData(), hdrArenaTailOff, off)
}

func (s *shard) initArena() error {
	lock := s.mlock
	buf, err := newShardBufferLocked(arenaPageSize, lock)
	if err != nil {
		return err
	}
	s.arena = buf
	data := buf.data
	writeU32(data, pageNextOff, 0)
	writeU32(data, pageUsedOff, 0)
	s.setArenaHead(0)
	s.setArenaTail(0)
	s.setArenaUsed(0)
	return nil
}

func (s *shard) pagePayloadCap(pageOff uint32) int {
	data := s.arenaBytes()
	if data == nil {
		return 0
	}
	next := readU32(data, int(pageOff)+pageNextOff)
	end := len(data)
	if next != 0 && int(next) <= end {
		end = int(next)
	}
	cap := end - int(pageOff) - pageHdrSize
	if cap < 0 {
		return 0
	}
	return cap
}

func (s *shard) addArenaPage(minPayload int) error {
	if minPayload < 0 {
		minPayload = 0
	}
	pageBytes := int64(arenaPageSize)
	need := int64(pageHdrSize + minPayload)
	if need > pageBytes {
		pageBytes = bufferAllocSize(need)
	}
	oldLen := int64(0)
	if s.arena != nil {
		oldLen = int64(len(s.arena.data))
	}
	newLen := oldLen + pageBytes
	if newLen > maxShardBytes || newLen > int64(maxSlotOffset)+1 {
		return ErrSlotOffset
	}
	if s.arena == nil {
		return fmt.Errorf("hashindex: arena is closed")
	}
	if err := s.growArenaTo(newLen); err != nil {
		return err
	}
	data := s.arenaBytes()
	newOff := uint32(oldLen)
	if oldLen > 0 {
		writeU32(data, int(s.arenaTail())+pageNextOff, newOff)
	} else {
		s.setArenaHead(newOff)
	}
	writeU32(data, int(newOff)+pageNextOff, 0)
	writeU32(data, int(newOff)+pageUsedOff, 0)
	s.setArenaTail(newOff)
	return nil
}

func (s *shard) growArenaTo(minSize int64) error {
	if s.arena == nil || s.arena.data == nil {
		return fmt.Errorf("hashindex: arena is closed")
	}
	if minSize > maxShardBytes {
		return ErrSlotOffset
	}
	oldLen := int64(len(s.arena.data))
	if minSize <= oldLen {
		return nil
	}
	planned := bufferAllocSize(minSize)
	if planned > maxShardBytes {
		return ErrSlotOffset
	}
	if s.parent != nil {
		if err := s.parent.accountDelta(planned - oldLen); err != nil {
			return err
		}
	}
	if err := growBufferExact(s.arena, minSize); err != nil {
		if s.parent != nil {
			_ = s.parent.accountDelta(-(planned - oldLen))
		}
		return err
	}
	if adj := int64(len(s.arena.data)) - planned; adj != 0 && s.parent != nil {
		if err := s.parent.accountDelta(adj); err != nil {
			return err
		}
	}
	return nil
}

func (s *shard) alloc(size int) (uint64, error) {
	if size <= 0 {
		return 0, fmt.Errorf("hashindex: invalid alloc %d", size)
	}
	if s.arena == nil || s.arena.data == nil {
		return 0, fmt.Errorf("hashindex: arena is closed")
	}
	if int64(size) > maxShardBytes {
		return 0, ErrSlotOffset
	}

	try := func() (uint64, bool) {
		tail := s.arenaTail()
		data := s.arenaBytes()
		used := readU32(data, int(tail)+pageUsedOff)
		if int(used)+size > s.pagePayloadCap(tail) {
			return 0, false
		}
		off := uint64(tail) + uint64(pageHdrSize) + uint64(used)
		writeU32(data, int(tail)+pageUsedOff, used+uint32(size))
		s.setArenaUsed(s.arenaUsed() + uint64(size))
		return off, true
	}

	if off, ok := try(); ok {
		return off, nil
	}
	if err := s.addArenaPage(size); err != nil {
		return 0, err
	}
	off, ok := try()
	if !ok {
		return 0, fmt.Errorf("hashindex: arena page too small for %d", size)
	}
	return off, nil
}

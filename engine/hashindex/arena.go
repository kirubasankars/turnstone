// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "fmt"

// shardBuffer holds a shard's backing store. On Unix it is mmap(MAP_ANON);
// on other platforms it falls back to a heap []byte with the same API.
type shardBuffer struct {
	data        []byte
	mmapBacking []byte // non-nil on Unix: full mapping passed to Munmap on release
	locked      bool
}

func (b *shardBuffer) bytes() []byte {
	if b == nil {
		return nil
	}
	return b.data
}

func (b *shardBuffer) len() int {
	if b == nil {
		return 0
	}
	return len(b.data)
}

func (b *shardBuffer) close() {
	if b == nil {
		return
	}
	releaseShardBuffer(b)
	b.data = nil
	b.mmapBacking = nil
	b.locked = false
}

func (s *shard) shardData() []byte {
	if s == nil || s.buf == nil {
		return nil
	}
	return s.buf.data
}

func (s *shard) isClosed() bool {
	return s.buf == nil || s.buf.data == nil
}

func (s *shard) grow(minSize int64) error {
	if s.isClosed() {
		return fmt.Errorf("hashindex: shard is closed")
	}
	if minSize > maxShardBytes {
		return ErrSlotOffset
	}
	if s.parent == nil {
		if plannedBufferSize(int64(len(s.buf.data)), minSize) > maxShardBytes {
			return ErrSlotOffset
		}
		return growShardBuffer(s.buf, minSize)
	}
	oldLen := int64(len(s.buf.data))
	if minSize <= oldLen {
		return nil
	}
	planned := plannedBufferSize(oldLen, minSize)
	if planned > maxShardBytes {
		return ErrSlotOffset
	}
	delta := planned - oldLen
	if err := s.parent.accountDelta(delta); err != nil {
		return err
	}
	if err := growShardBuffer(s.buf, minSize); err != nil {
		_ = s.parent.accountDelta(-delta)
		return err
	}
	if adj := int64(len(s.buf.data)) - planned; adj != 0 {
		if err := s.parent.accountDelta(adj); err != nil {
			return err
		}
	}
	return nil
}

func (s *shard) replaceBuffer(next *shardBuffer) error {
	if next == nil {
		panic("hashindex: nil replacement buffer")
	}
	oldLen := int64(0)
	if s.buf != nil {
		oldLen = int64(len(s.buf.data))
	}
	newLen := int64(len(next.data))
	if err := s.parent.accountDelta(newLen - oldLen); err != nil {
		return err
	}
	old := s.buf
	s.buf = next
	if old != nil {
		old.close()
	}
	return nil
}

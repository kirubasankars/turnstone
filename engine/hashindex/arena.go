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
	return growShardBuffer(s.buf, minSize)
}

func (s *shard) replaceBuffer(next *shardBuffer) {
	if next == nil {
		panic("hashindex: nil replacement buffer")
	}
	old := s.buf
	s.buf = next
	if old != nil {
		old.close()
	}
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"errors"
	"sync"
	"sync/atomic"
)

var errSharedBuffersBusy = errors.New("shared buffers: no unpinned page")

const (
	sharedBufferPageSize = 8192
	// DefaultSharedBuffersBytes is the process-wide WAL page-pool default
	// (split across databases at server start).
	DefaultSharedBuffersBytes = 64 << 20
	defaultSharedBuffersBytes = DefaultSharedBuffersBytes
	sharedBufferMinPages      = 16
	sharedBufferClockMaxUsage = 5
)

type bufferTag struct {
	id   uint32
	page uint32
}

type bufDesc struct {
	tag   bufferTag
	valid bool
	usage uint32
	pins  uint32
}

// sharedBuffers is a PostgreSQL-style page pool for WAL-backed values.
// Pages are filled from the segment mmap (or pread) and shared by GETs.
type sharedBuffers struct {
	descs   []bufDesc
	backing []byte
	lookup  map[bufferTag]int
	clock   int
	mu      sync.Mutex
	hits    uint64
	misses  uint64
	evicts  uint64
}

func newSharedBuffers(bytes int64) *sharedBuffers {
	n := int(bytes / sharedBufferPageSize)
	if n < sharedBufferMinPages {
		n = sharedBufferMinPages
	}
	return &sharedBuffers{
		descs:   make([]bufDesc, n),
		backing: make([]byte, n*sharedBufferPageSize),
		lookup:  make(map[bufferTag]int, n),
	}
}

func (b *sharedBuffers) page(i int) []byte {
	off := i * sharedBufferPageSize
	return b.backing[off : off+sharedBufferPageSize]
}

func (b *sharedBuffers) evictLocked() (int, bool) {
	n := len(b.descs)
	for steps := 0; steps < n*sharedBufferClockMaxUsage+n; steps++ {
		i := b.clock
		b.clock = (b.clock + 1) % n
		d := &b.descs[i]
		if d.pins > 0 {
			continue
		}
		if d.usage > 0 {
			d.usage--
			continue
		}
		return i, true
	}
	return 0, false
}

func (b *sharedBuffers) pin(tag bufferTag, fill func(dst []byte) error) (int, []byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if i, ok := b.lookup[tag]; ok && b.descs[i].valid {
		b.descs[i].pins++
		if b.descs[i].usage < sharedBufferClockMaxUsage {
			b.descs[i].usage++
		}
		atomic.AddUint64(&b.hits, 1)
		return i, b.page(i), nil
	}
	i, ok := b.evictLocked()
	if !ok {
		return 0, nil, errSharedBuffersBusy
	}
	if old := b.descs[i]; old.valid {
		delete(b.lookup, old.tag)
		atomic.AddUint64(&b.evicts, 1)
	}
	if err := fill(b.page(i)); err != nil {
		b.descs[i] = bufDesc{}
		return 0, nil, err
	}
	b.descs[i] = bufDesc{tag: tag, valid: true, usage: 1, pins: 1}
	b.lookup[tag] = i
	atomic.AddUint64(&b.misses, 1)
	return i, b.page(i), nil
}

func (b *sharedBuffers) unpin(i int) {
	if b == nil {
		return
	}
	b.mu.Lock()
	if i >= 0 && i < len(b.descs) && b.descs[i].pins > 0 {
		b.descs[i].pins--
	}
	b.mu.Unlock()
}

func (b *sharedBuffers) applyWrite(segID uint32, local int64, data []byte) {
	if b == nil || len(data) == 0 || local < 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	pos := local
	remaining := data
	for len(remaining) > 0 {
		page := uint32(pos / sharedBufferPageSize)
		off := int(pos % sharedBufferPageSize)
		tag := bufferTag{id: segID, page: page}
		if i, ok := b.lookup[tag]; ok && b.descs[i].valid {
			n := copy(b.page(i)[off:], remaining)
			remaining = remaining[n:]
			pos += int64(n)
			continue
		}
		n := sharedBufferPageSize - off
		if n > len(remaining) {
			n = len(remaining)
		}
		remaining = remaining[n:]
		pos += int64(n)
	}
}

func (b *sharedBuffers) invalidateSegment(id uint32) {
	if b == nil {
		return
	}
	b.mu.Lock()
	for tag, i := range b.lookup {
		if tag.id == id {
			delete(b.lookup, tag)
			b.descs[i].valid = false
		}
	}
	b.mu.Unlock()
}

func (b *sharedBuffers) clear() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.lookup = make(map[bufferTag]int, len(b.descs))
	for i := range b.descs {
		b.descs[i] = bufDesc{}
	}
	b.mu.Unlock()
}

func (b *sharedBuffers) Hits() uint64 {
	if b == nil {
		return 0
	}
	return atomic.LoadUint64(&b.hits)
}

func (b *sharedBuffers) Misses() uint64 {
	if b == nil {
		return 0
	}
	return atomic.LoadUint64(&b.misses)
}

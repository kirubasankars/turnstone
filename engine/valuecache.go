// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"sync"
	"sync/atomic"
)

// Decoded-value L1 in front of shared_buffers / WAL mmap.
const (
	valueCacheShards       = 64
	defaultValueCacheBytes = 64 << 20
	valueCacheMaxEntry     = 1 << 20
)

type cacheShard struct {
	mu    sync.Mutex
	items map[int64][]byte
	bytes int64
	max   int64
}

type valueCache struct {
	shards [valueCacheShards]cacheShard
	hits   uint64
	misses uint64
}

func newValueCache(maxBytes int64) *valueCache {
	if maxBytes < valueCacheShards {
		maxBytes = valueCacheShards
	}
	per := maxBytes / valueCacheShards
	c := &valueCache{}
	for i := range c.shards {
		c.shards[i] = cacheShard{
			items: make(map[int64][]byte),
			max:   per,
		}
	}
	return c
}

func (c *valueCache) shard(offset int64) *cacheShard {
	return &c.shards[uint64(offset)%valueCacheShards]
}

func (c *valueCache) get(offset int64) ([]byte, bool) {
	s := c.shard(offset)
	s.mu.Lock()
	val, ok := s.items[offset]
	s.mu.Unlock()
	if !ok {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}
	atomic.AddUint64(&c.hits, 1)
	return append([]byte(nil), val...), true
}

func (c *valueCache) put(offset int64, val []byte) {
	if len(val) == 0 || len(val) > valueCacheMaxEntry {
		return
	}
	s := c.shard(offset)
	s.mu.Lock()
	if old, ok := s.items[offset]; ok {
		s.bytes -= int64(len(old))
		delete(s.items, offset)
	}
	need := int64(len(val))
	for s.bytes+need > s.max && len(s.items) > 0 {
		for k, v := range s.items {
			s.bytes -= int64(len(v))
			delete(s.items, k)
			break
		}
	}
	if s.bytes+need <= s.max {
		s.items[offset] = append([]byte(nil), val...)
		s.bytes += need
	}
	s.mu.Unlock()
}

func (c *valueCache) clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.items = make(map[int64][]byte)
		s.bytes = 0
		s.mu.Unlock()
	}
}

func (db *DB) cacheValue(offset int64, val []byte) {
	if db.valueCache == nil {
		return
	}
	db.valueCache.put(offset, val)
}

func (db *DB) cachedValue(offset int64) ([]byte, bool) {
	if db.valueCache == nil {
		return nil, false
	}
	return db.valueCache.get(offset)
}

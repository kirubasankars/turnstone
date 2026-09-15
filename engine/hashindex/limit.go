// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "sync/atomic"

func (idx *Index) SetMaxArenaBytes(n int64) {
	atomic.StoreInt64(&idx.maxArenaBytes, n)
}

func (idx *Index) MaxArenaBytes() int64 {
	return atomic.LoadInt64(&idx.maxArenaBytes)
}

func (idx *Index) SetEnforceLimit(enforce bool) {
	if enforce {
		atomic.StoreInt32(&idx.enforceLimit, 1)
	} else {
		atomic.StoreInt32(&idx.enforceLimit, 0)
	}
}

func (idx *Index) UsedBytes() int64 {
	return atomic.LoadInt64(&idx.usedBytes)
}

func (idx *Index) RecalcUsedBytes() {
	var total int64
	for _, s := range idx.shards {
		if s == nil {
			continue
		}
		s.mu.RLock()
		if s.buf != nil {
			total += int64(len(s.buf.data))
		}
		s.mu.RUnlock()
	}
	atomic.StoreInt64(&idx.usedBytes, total)
}

func (idx *Index) accountDelta(delta int64) error {
	if delta == 0 {
		return nil
	}
	if delta < 0 {
		atomic.AddInt64(&idx.usedBytes, delta)
		return nil
	}
	if atomic.LoadInt32(&idx.enforceLimit) == 0 {
		atomic.AddInt64(&idx.usedBytes, delta)
		return nil
	}
	limit := atomic.LoadInt64(&idx.maxArenaBytes)
	if limit <= 0 {
		atomic.AddInt64(&idx.usedBytes, delta)
		return nil
	}
	for {
		used := atomic.LoadInt64(&idx.usedBytes)
		next := used + delta
		if next > limit {
			return ErrArenaLimit
		}
		if atomic.CompareAndSwapInt64(&idx.usedBytes, used, next) {
			return nil
		}
	}
}

func plannedBufferSize(currentLen, minSize int64) int64 {
	n := currentLen
	if n == 0 {
		n = minSize
	}
	for n < minSize {
		n *= 2
	}
	return bufferAllocSize(n)
}

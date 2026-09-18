// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "sync/atomic"

// SharedBudget is a process-wide cap on hash-index shard buffer bytes.
// All indexes that attach to the same budget compete for Max bytes.
type SharedBudget struct {
	max  int64
	used int64
}

// NewSharedBudget returns a shared cap, or nil when max <= 0 (unlimited).
func NewSharedBudget(max int64) *SharedBudget {
	if max <= 0 {
		return nil
	}
	return &SharedBudget{max: max}
}

func (b *SharedBudget) Max() int64 {
	if b == nil {
		return 0
	}
	return b.max
}

func (b *SharedBudget) Used() int64 {
	if b == nil {
		return 0
	}
	return atomic.LoadInt64(&b.used)
}

func (b *SharedBudget) reserve(delta int64) error {
	if b == nil || delta == 0 {
		return nil
	}
	if delta < 0 {
		atomic.AddInt64(&b.used, delta)
		return nil
	}
	for {
		used := atomic.LoadInt64(&b.used)
		next := used + delta
		if next > b.max {
			return ErrArenaLimit
		}
		if atomic.CompareAndSwapInt64(&b.used, used, next) {
			return nil
		}
	}
}

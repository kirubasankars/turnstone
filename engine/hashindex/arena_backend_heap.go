// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"fmt"

	"turnstone/internal/mlock"
)

func heapNewShardBuffer(size int64, lock bool) (*shardBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("hashindex: invalid buffer size %d", size)
	}
	data := make([]byte, size)
	if lock {
		if err := mlock.Lock(data); err != nil {
			return nil, fmt.Errorf("mlock shard buffer: %w", err)
		}
	}
	return &shardBuffer{data: data, locked: lock}, nil
}

func heapGrowShardBuffer(b *shardBuffer, minSize int64) error {
	if b == nil || b.data == nil {
		return fmt.Errorf("hashindex: buffer is closed")
	}
	if minSize <= int64(len(b.data)) {
		return nil
	}
	n := len(b.data)
	if n == 0 {
		n = int(minSize)
	}
	for int64(n) < minSize {
		n *= 2
	}
	out := make([]byte, n)
	copy(out, b.data)
	if b.locked {
		if err := mlock.Lock(out); err != nil {
			return fmt.Errorf("mlock shard buffer: %w", err)
		}
		mlock.Unlock(b.data)
	}
	b.data = out
	return nil
}

func heapReleaseShardBuffer(b *shardBuffer) {
	if b == nil {
		return
	}
	if b.locked {
		mlock.Unlock(b.data)
		b.locked = false
	}
	b.data = nil
}

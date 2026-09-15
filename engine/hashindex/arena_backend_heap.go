// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "fmt"

func heapNewShardBuffer(size int64) (*shardBuffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("hashindex: invalid buffer size %d", size)
	}
	return &shardBuffer{data: make([]byte, size)}, nil
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
	b.data = out
	return nil
}

func heapReleaseShardBuffer(b *shardBuffer) {
	if b == nil {
		return
	}
	b.data = nil
}

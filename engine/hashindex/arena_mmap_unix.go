// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package hashindex

import (
	"fmt"
	"syscall"
)

var shardBufferPageSize = syscall.Getpagesize()

func alignShardBufferSize(n int64) int64 {
	if n <= 0 {
		return int64(shardBufferPageSize)
	}
	ps := int64(shardBufferPageSize)
	return (n + ps - 1) &^ (ps - 1)
}

func newShardBuffer(size int64) (*shardBuffer, error) {
	mappedSize := alignShardBufferSize(size)
	mapped, err := syscall.Mmap(-1, 0, int(mappedSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("mmap shard buffer: %w", err)
	}
	slice := mapped[:mappedSize]
	return &shardBuffer{
		data:        slice,
		mmapBacking: slice,
	}, nil
}

func growShardBuffer(b *shardBuffer, minSize int64) error {
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
	next, err := newShardBuffer(int64(n))
	if err != nil {
		return err
	}
	copy(next.data, b.data)
	releaseShardBuffer(b)
	b.data = next.data
	b.mmapBacking = next.mmapBacking
	next.data = nil
	next.mmapBacking = nil
	return nil
}

func releaseShardBuffer(b *shardBuffer) {
	if b == nil || b.mmapBacking == nil {
		return
	}
	_ = syscall.Munmap(b.mmapBacking)
	b.mmapBacking = nil
}

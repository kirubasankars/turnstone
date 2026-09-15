// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

//go:build unix

package hashindex

import (
	"fmt"
	"testing"
)

func BenchmarkIndexPut_ArenaCompare(b *testing.B) {
	keys := shardKeysForBench(2000, 0)
	b.Run("Mmap", func(b *testing.B) {
		benchIndexPut(b, keys)
	})
	b.Run("Heap", func(b *testing.B) {
		restore := useHeapShardBuffers()
		defer restore()
		benchIndexPut(b, keys)
	})
}

func BenchmarkIndexPutManyShards_ArenaCompare(b *testing.B) {
	keys := make([][]byte, 5000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-many-%d", i))
	}
	b.Run("Mmap", func(b *testing.B) {
		benchIndexPut(b, keys)
	})
	b.Run("Heap", func(b *testing.B) {
		restore := useHeapShardBuffers()
		defer restore()
		benchIndexPut(b, keys)
	})
}

func BenchmarkShardGrow_ArenaCompare(b *testing.B) {
	keys := shardKeysForBench(2500, 0)
	b.Run("Mmap", func(b *testing.B) {
		benchIndexGrowShard(b, keys)
	})
	b.Run("Heap", func(b *testing.B) {
		restore := useHeapShardBuffers()
		defer restore()
		benchIndexGrowShard(b, keys)
	})
}

func BenchmarkCompactShard_ArenaCompare(b *testing.B) {
	b.Run("Mmap", func(b *testing.B) {
		benchCompactShard(b)
	})
	b.Run("Heap", func(b *testing.B) {
		restore := useHeapShardBuffers()
		defer restore()
		benchCompactShard(b)
	})
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"fmt"
	"testing"
)

func shardKeysForBench(n int, shard byte) [][]byte {
	keys := make([][]byte, 0, n)
	for i := 0; len(keys) < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%d", i))
		if hashKey(key)&255 == uint64(shard) {
			keys = append(keys, key)
		}
	}
	return keys
}

func benchIndexPut(b *testing.B, keys [][]byte) {
	idx := New()
	defer idx.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		idx.Put(key, Version{Offset: int64(i), ValueLen: 64, Xmin: uint64(i + 1)})
	}
}

func benchIndexGrowShard(b *testing.B, keys [][]byte) {
	idx := New()
	defer idx.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		idx.Put(key, Version{Offset: int64(i), ValueLen: 32, Xmin: uint64(i + 1)})
	}
}

func fragmentKeyForCompact(idx *Index, key []byte) {
	idx.Put(key, Version{Offset: 100, ValueLen: 64, Xmin: 1})
	for xid := uint64(2); xid <= 8; xid++ {
		idx.Put(key, Version{Offset: int64(xid * 10), ValueLen: 64, Xmin: xid})
		idx.DropXid(xid)
	}
}

func benchCompactShard(b *testing.B) {
	idx := New()
	defer idx.Close()

	key := []byte("compact-bench-key")
	shardIdx := int(hashKey(key) & 255)
	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) == 0 {
			return nil
		}
		return chain[:1]
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fragmentKeyForCompact(idx, key)
		if _, err := idx.CompactShard(shardIdx, filter); err != nil {
			b.Fatal(err)
		}
	}
}

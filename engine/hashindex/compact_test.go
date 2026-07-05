// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"testing"
)

func TestCompactShard_DropsOldVersionsAndShrinksArena(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("k")
	for i := 1; i <= 5; i++ {
		idx.Put(key, Version{Offset: int64(i * 10), ValueLen: 1, Xmin: uint64(i)})
	}

	shardIdx := int(hashKey(key) & 255)
	before := idx.Stats().Shards[shardIdx].ArenaUsed
	if before == 0 {
		t.Fatal("expected non-zero arena before compact")
	}

	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) == 0 {
			return nil
		}
		return chain[:1]
	}
	afterStats, err := idx.CompactShard(shardIdx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if afterStats.ArenaUsed >= before {
		t.Fatalf("expected arena shrink, before=%d after=%d", before, afterStats.ArenaUsed)
	}

	var chain []Version
	idx.WalkVersions(key, func(v Version) bool {
		chain = append(chain, v)
		return true
	})
	if len(chain) != 1 || chain[0].Xmin != 5 {
		t.Fatalf("expected single newest version, got %+v", chain)
	}
}

func TestCompactShard_RemovesKeyWhenFilterEmpty(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("drop-me")
	idx.Put(key, Version{Offset: 1, Xmin: 1})

	shardIdx := int(hashKey(key) & 255)
	filter := func(_ []byte, _ []Version) []Version { return nil }
	if _, err := idx.CompactShard(shardIdx, filter); err != nil {
		t.Fatal(err)
	}

	seen := false
	idx.ForEachKey(func(k []byte, chain []Version) {
		if string(k) == string(key) {
			seen = true
		}
	})
	if seen {
		t.Fatal("expected key removed from index")
	}
}

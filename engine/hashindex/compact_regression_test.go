// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"fmt"
	"testing"
)

func TestCompactAll_RegressionPreservesMultipleKeys(t *testing.T) {
	idx := New()
	defer idx.Close()

	keys := []string{"alpha", "beta", "gamma"}
	for i, k := range keys {
		idx.Put([]byte(k), Version{Offset: int64(i + 1), ValueLen: 1, Xmin: uint64(i + 1)})
	}

	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) == 0 {
			return nil
		}
		return chain[:1]
	}
	if _, err := idx.CompactAll(filter); err != nil {
		t.Fatal(err)
	}

	seen := map[string]int{}
	idx.ForEachKey(func(k []byte, chain []Version) {
		seen[string(k)] = len(chain)
	})
	if len(seen) != len(keys) {
		t.Fatalf("expected %d keys, got %v", len(keys), seen)
	}
	for _, k := range keys {
		if seen[k] != 1 {
			t.Fatalf("key %s chain len=%d want 1", k, seen[k])
		}
	}
}

func TestCompactShard_RegressionMaintainsNewestFirstOrder(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("order-key")
	for i := 1; i <= 4; i++ {
		idx.Put(key, Version{Offset: int64(i * 10), Xmin: uint64(i)})
	}

	shardIdx := int(hashKey(key) & 255)
	filter := func(_ []byte, chain []Version) []Version {
		return chain
	}
	if _, err := idx.CompactShard(shardIdx, filter); err != nil {
		t.Fatal(err)
	}

	var chain []Version
	idx.WalkVersions(key, func(v Version) bool {
		chain = append(chain, v)
		return true
	})
	if len(chain) != 4 {
		t.Fatalf("expected 4 versions, got %d", len(chain))
	}
	for i := 0; i < len(chain)-1; i++ {
		if chain[i].Xmin <= chain[i+1].Xmin {
			t.Fatalf("expected newest-first order, got %+v", chain)
		}
	}
}

func TestCompactShard_RegressionLiveBytesEqualsArenaUsed(t *testing.T) {
	idx := New()
	defer idx.Close()

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		idx.Put(key, Version{Offset: int64(i), Xmin: uint64(i + 1)})
	}

	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) == 0 {
			return nil
		}
		return chain[:1]
	}
	stats, err := idx.CompactAll(filter)
	if err != nil {
		t.Fatal(err)
	}

	var totalArena, totalLive uint64
	for i := range stats.Shards {
		st := stats.Shards[i]
		if st.KeyCount == 0 {
			continue
		}
		totalArena += st.ArenaUsed
		totalLive += st.LiveBytes
		if st.ArenaUsed != st.LiveBytes {
			t.Fatalf("shard %d arena=%d live=%d", i, st.ArenaUsed, st.LiveBytes)
		}
	}
	if totalArena == 0 {
		t.Fatal("expected non-zero total arena after compact")
	}
}

func TestCompactShard_RegressionAfterAbortFragmentation(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("abort-key")
	idx.Put(key, Version{Offset: 100, Xmin: 1})
	for xid := uint64(2); xid <= 10; xid++ {
		idx.Put(key, Version{Offset: int64(xid * 10), Xmin: xid})
		idx.DropXid(xid)
	}

	shardIdx := int(hashKey(key) & 255)
	before := idx.Stats().Shards[shardIdx].ArenaUsed

	filter := func(_ []byte, chain []Version) []Version {
		return chain
	}
	afterStats, err := idx.CompactShard(shardIdx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if afterStats.ArenaUsed >= before {
		t.Fatalf("expected shrink after abort fragmentation, before=%d after=%d", before, afterStats.ArenaUsed)
	}

	var chain []Version
	idx.WalkVersions(key, func(v Version) bool {
		chain = append(chain, v)
		return true
	})
	if len(chain) != 1 || chain[0].Xmin != 1 {
		t.Fatalf("expected single xmin=1 version, got %+v", chain)
	}
}

func TestCompactShard_RegressionPartialChainPrune(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("partial")
	// Put oldest xmin first so the chain is newest-first by xmin (3, 2, 1).
	idx.Put(key, Version{Offset: 100, Xmin: 1})
	idx.Put(key, Version{Offset: 200, Xmin: 2})
	idx.Put(key, Version{Offset: 300, Xmin: 3})

	shardIdx := int(hashKey(key) & 255)
	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) < 2 {
			return chain
		}
		return chain[:2]
	}
	if _, err := idx.CompactShard(shardIdx, filter); err != nil {
		t.Fatal(err)
	}

	var xmins []uint64
	idx.WalkVersions(key, func(v Version) bool {
		xmins = append(xmins, v.Xmin)
		return true
	})
	if len(xmins) != 2 || xmins[0] != 3 || xmins[1] != 2 {
		t.Fatalf("expected xmins [3 2], got %v", xmins)
	}
}

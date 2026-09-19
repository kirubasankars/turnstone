// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutAndWalkVersions(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("alpha")
	idx.Put(key, Version{Offset: 10, ValueLen: 3, Xmin: 1})
	idx.Put(key, Version{Offset: 20, ValueLen: 3, Xmin: 2})

	var chain []Version
	idx.WalkVersions(key, func(v Version) bool {
		chain = append(chain, v)
		return true
	})
	if len(chain) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(chain))
	}
	if chain[0].Xmin != 2 || chain[1].Xmin != 1 {
		t.Fatalf("expected newest-first chain, got %+v", chain)
	}
}

func walkVersions(idx *Index, key []byte) []Version {
	var chain []Version
	idx.WalkVersions(key, func(v Version) bool {
		chain = append(chain, v)
		return true
	})
	return chain
}

// collidingKeys returns two distinct keys that hash to the same shard and the
// same home slot of a fresh table (initialSlots). The second key linear-probes
// to the next empty slot after the first is inserted.
func collidingKeys(t *testing.T) (a, b []byte) {
	t.Helper()
	slots := uint64(initialSlots)
	for i := 0; i < 200000; i++ {
		ka := []byte(fmt.Sprintf("probe-%d", i))
		ha := hashKey(ka)
		for j := i + 1; j < i+8000; j++ {
			kb := []byte(fmt.Sprintf("probe-%d", j))
			hb := hashKey(kb)
			if ha&255 != hb&255 {
				continue
			}
			if ha%slots == hb%slots {
				return ka, kb
			}
		}
	}
	t.Fatal("no colliding keys")
	return nil, nil
}

func homeSlotOccupied(idx *Index, key []byte) bool {
	seg := idx.shardFor(key)
	seg.mu.RLock()
	defer seg.mu.RUnlock()
	if seg.isClosed() {
		return false
	}
	data := seg.shardData()
	table := int(seg.tableOff())
	slot := seg.slotIndex(key)
	return readSlot(data, table, slot) != 0
}

func TestForEachKeyAndDropXid(t *testing.T) {
	idx := New()
	defer idx.Close()

	idx.Put([]byte("a"), Version{Offset: 1, Xmin: 1})
	idx.Put([]byte("b"), Version{Offset: 2, Xmin: 2})
	idx.Put([]byte("a"), Version{Offset: 3, Xmin: 3})

	count := 0
	idx.ForEachKey(func(_ []byte, chain []Version) {
		count++
		if len(chain) == 0 {
			t.Fatal("empty chain")
		}
	})
	if count != 2 {
		t.Fatalf("expected 2 keys, got %d", count)
	}

	idx.DropXid(2)
	found := false
	idx.ForEachKey(func(key []byte, chain []Version) {
		if string(key) == "b" {
			found = true
			if len(chain) != 0 {
				t.Fatalf("expected b removed, chain=%+v", chain)
			}
		}
	})
	if found {
		t.Fatal("key b should be gone after DropXid")
	}
}

func TestDropXid_KeepsSlotSoCollidingKeyStaysFindable(t *testing.T) {
	idx := New()
	defer idx.Close()

	a, b := collidingKeys(t)
	if err := idx.Put(a, Version{Offset: 1, Xmin: 1}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(b, Version{Offset: 2, Xmin: 2}); err != nil {
		t.Fatal(err)
	}
	if got := walkVersions(idx, b); len(got) != 1 || got[0].Xmin != 2 {
		t.Fatalf("setup: B chain=%+v", got)
	}

	if err := idx.DropXid(1); err != nil {
		t.Fatal(err)
	}

	if !homeSlotOccupied(idx, a) {
		t.Fatal("DropXid cleared A's home slot; B's probe chain is broken")
	}
	if got := walkVersions(idx, a); len(got) != 0 {
		t.Fatalf("A should have an empty chain after DropXid, got %+v", got)
	}
	if got := walkVersions(idx, b); len(got) != 1 || got[0].Offset != 2 || got[0].Xmin != 2 {
		t.Fatalf("B must stay findable after DropXid emptied A, got %+v", got)
	}

	seenB := false
	idx.ForEachKey(func(key []byte, chain []Version) {
		if string(key) == string(b) {
			seenB = true
			if len(chain) != 1 || chain[0].Xmin != 2 {
				t.Fatalf("ForEachKey B chain=%+v", chain)
			}
		}
		if string(key) == string(a) {
			t.Fatalf("ForEachKey should omit empty A, chain=%+v", chain)
		}
	})
	if !seenB {
		t.Fatal("ForEachKey lost B")
	}
}

func TestDropXid_ReputEmptiedKeyDoesNotOrphanNeighbor(t *testing.T) {
	idx := New()
	defer idx.Close()

	a, b := collidingKeys(t)
	if err := idx.Put(a, Version{Offset: 1, Xmin: 1}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(b, Version{Offset: 2, Xmin: 2}); err != nil {
		t.Fatal(err)
	}
	if err := idx.DropXid(1); err != nil {
		t.Fatal(err)
	}

	if err := idx.Put(a, Version{Offset: 3, Xmin: 3}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(b, Version{Offset: 4, Xmin: 4}); err != nil {
		t.Fatal(err)
	}

	if got := walkVersions(idx, a); len(got) != 1 || got[0].Xmin != 3 {
		t.Fatalf("A after reput=%+v", got)
	}
	if got := walkVersions(idx, b); len(got) != 2 || got[0].Xmin != 4 || got[1].Xmin != 2 {
		t.Fatalf("B after reput=%+v", got)
	}

	seen := 0
	idx.ForEachKey(func(key []byte, chain []Version) {
		switch string(key) {
		case string(a), string(b):
			seen++
		}
		if len(chain) == 0 {
			t.Fatal("empty chain after reput")
		}
	})
	if seen != 2 {
		t.Fatalf("expected 2 live keys after reput, got %d", seen)
	}
}

func TestHashTableGrowSameShard(t *testing.T) {
	idx := New()
	defer idx.Close()

	var keys [][]byte
	for i := 0; len(keys) < 1500; i++ {
		k := []byte(fmt.Sprintf("grow-key-%d", i))
		if hashKey(k)&255 == 0 {
			keys = append(keys, k)
		}
	}
	t.Logf("generated %d keys for shard 0", len(keys))

	for i, key := range keys {
		idx.Put(key, Version{Offset: int64(i), Xmin: uint64(i + 1)})
	}

	seen := 0
	idx.ForEachKey(func(_ []byte, chain []Version) {
		seen++
	})
	t.Logf("forEach seen %d keys", seen)
	if seen != len(keys) {
		t.Fatalf("expected %d keys after grow, got %d", len(keys), seen)
	}

	seg := idx.shards[0]
	seg.mu.RLock()
	tableBytes := uint64(len(seg.buf.data)) - uint64(headerSize)
	slots := seg.slotCount()
	seg.mu.RUnlock()
	if slots <= initialSlots {
		t.Fatalf("expected slot grow beyond %d, got %d", initialSlots, slots)
	}
	if tableBytes != uint64(slots)*slotWidth {
		t.Fatalf("after grow: table bytes %d want %d", tableBytes, uint64(slots)*slotWidth)
	}
}

func TestHashTableGrowManyKeys(t *testing.T) {
	idx := New()
	defer idx.Close()

	const n = 5000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("grow-key-%d", i))
		idx.Put(key, Version{Offset: int64(i), Xmin: uint64(i + 1)})
	}

	seen := 0
	idx.ForEachKey(func(_ []byte, chain []Version) {
		seen++
		if len(chain) == 0 {
			t.Fatal("empty chain")
		}
	})
	if seen != n {
		t.Fatalf("expected %d keys after grow, got %d", n, seen)
	}
}

func TestConcurrentPutsDifferentKeys(t *testing.T) {
	idx := New()
	defer idx.Close()

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := []byte(fmt.Sprintf("key-%d", i))
			idx.Put(key, Version{Offset: int64(i), ValueLen: 4, Xmin: uint64(i + 1)})
		}()
	}
	wg.Wait()

	seen := 0
	idx.ForEachKey(func(_ []byte, chain []Version) {
		seen++
		if len(chain) != 1 {
			t.Errorf("expected single version per key")
		}
	})
	if seen != n {
		t.Fatalf("expected %d keys, got %d", n, seen)
	}
}

func TestSlotTableUsesU32Entries(t *testing.T) {
	idx := New()
	defer idx.Close()

	seg := idx.shards[0]
	seg.mu.RLock()
	table := seg.tableOff()
	slots := seg.slotCount()
	ver := readU32(seg.shardData(), hdrVersionOff)
	tableBytes := uint64(len(seg.buf.data)) - table
	seg.mu.RUnlock()

	if tableBytes != uint64(slots)*slotWidth {
		t.Fatalf("slot table width: table bytes=%d want %d (slots=%d)", tableBytes, uint64(slots)*slotWidth, slots)
	}
	if slotWidth != 4 {
		t.Fatalf("slotWidth=%d want 4", slotWidth)
	}
	if ver != formatVersion {
		t.Fatalf("formatVersion=%d want %d", ver, formatVersion)
	}

	if err := idx.Put([]byte("u32-slot"), Version{Offset: 1, Xmin: 1}); err != nil {
		t.Fatal(err)
	}
	var n int
	idx.WalkVersions([]byte("u32-slot"), func(Version) bool {
		n++
		return true
	})
	if n != 1 {
		t.Fatalf("walk after u32 slot put: %d", n)
	}
}

func TestArenaUsesLinkedPages(t *testing.T) {
	idx := New()
	defer idx.Close()

	a := make([]byte, 40<<10)
	copy(a, "page-a")
	b := make([]byte, 40<<10)
	for i := 0; i < 256; i++ {
		b[0] = byte(i)
		if hashKey(a)&255 == hashKey(b)&255 && a[0] != b[0] {
			break
		}
	}
	if hashKey(a)&255 != hashKey(b)&255 {
		t.Fatal("could not place two large keys on one shard")
	}
	if err := idx.Put(a, Version{Offset: 1, Xmin: 1}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(b, Version{Offset: 2, Xmin: 2}); err != nil {
		t.Fatal(err)
	}

	seg := idx.shardFor(a)
	seg.mu.RLock()
	head := seg.arenaHead()
	next := readU32(seg.arenaBytes(), int(head)+pageNextOff)
	seg.mu.RUnlock()
	if next == 0 {
		t.Fatal("expected a second arena page after spilling the first")
	}

	var nA, nB int
	idx.WalkVersions(a, func(Version) bool { nA++; return true })
	idx.WalkVersions(b, func(Version) bool { nB++; return true })
	if nA != 1 || nB != 1 {
		t.Fatalf("after page spill: A=%d B=%d", nA, nB)
	}
}

func TestWriteSlotRejectsOffsetAboveU32(t *testing.T) {
	buf := make([]byte, 16)
	if err := writeSlot(buf, 0, 0, uint64(maxSlotOffset)+1); err != ErrSlotOffset {
		t.Fatalf("want ErrSlotOffset, got %v", err)
	}
	if err := writeSlot(buf, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if readSlot(buf, 0, 0) != 0 {
		t.Fatal("empty slot should stay 0")
	}
	if err := writeSlot(buf, 0, 0, maxSlotOffset); err != nil {
		t.Fatal(err)
	}
	if readSlot(buf, 0, 0) != maxSlotOffset {
		t.Fatalf("got %d want maxSlotOffset", readSlot(buf, 0, 0))
	}
}

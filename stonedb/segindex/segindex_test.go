// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package segindex

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutAndWalkVersions(t *testing.T) {
	idx := Open()
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

func TestForEachKeyAndDropXid(t *testing.T) {
	idx := Open()
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

func TestHashTableGrowSameSegment(t *testing.T) {
	idx := Open()
	defer idx.Close()

	var keys [][]byte
	for i := 0; len(keys) < 1500; i++ {
		k := []byte(fmt.Sprintf("grow-key-%d", i))
		if hashKey(k)&255 == 0 {
			keys = append(keys, k)
		}
	}
	t.Logf("generated %d keys for segment 0", len(keys))

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
}

func TestHashTableGrowManyKeys(t *testing.T) {
	idx := Open()
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
	idx := Open()
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

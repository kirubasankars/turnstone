// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"fmt"
	"testing"
)

func TestIndexArenaLimit_RejectsGrow(t *testing.T) {
	idx := New()
	defer idx.Close()

	baseline := idx.UsedBytes()
	idx.SetMaxArenaBytes(baseline + 4096)
	idx.RecalcUsedBytes()

	key := []byte("limit-key")
	for i := 0; i < 4000; i++ {
		err := idx.Put(key, Version{Offset: int64(i), ValueLen: 512, Xmin: uint64(i + 1)})
		if err == nil {
			continue
		}
		if err != ErrArenaLimit {
			t.Fatalf("unexpected error: %v", err)
		}
		if idx.UsedBytes() > idx.MaxArenaBytes() {
			t.Fatalf("used=%d exceeds limit=%d", idx.UsedBytes(), idx.MaxArenaBytes())
		}
		return
	}
	t.Fatal("expected arena limit error")
}

func TestIndexArenaLimit_ZeroDisables(t *testing.T) {
	idx := New()
	defer idx.Close()

	idx.SetMaxArenaBytes(0)
	key := []byte("free-key")
	for i := 0; i < 2000; i++ {
		if err := idx.Put([]byte(fmt.Sprintf("%s-%d", key, i)), Version{Offset: int64(i), Xmin: uint64(i + 1)}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
}

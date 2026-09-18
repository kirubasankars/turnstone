// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "testing"

func TestSharedBudget_TwoIndexesShareCap(t *testing.T) {
	a := New()
	defer a.Close()
	b := New()
	defer b.Close()

	used := a.UsedBytes() + b.UsedBytes()
	budget := NewSharedBudget(used + 1)
	if err := a.SetSharedBudget(budget); err != nil {
		t.Fatal(err)
	}
	if err := b.SetSharedBudget(budget); err != nil {
		t.Fatal(err)
	}

	fill := func(idx *Index, prefix string) error {
		key := []byte(prefix)
		for i := 0; i < 20000; i++ {
			if err := idx.Put(key, Version{Offset: int64(i), Xmin: uint64(i + 1)}); err != nil {
				return err
			}
		}
		return nil
	}

	errA := fill(a, "a")
	errB := fill(b, "b")
	if errA != ErrArenaLimit && errB != ErrArenaLimit {
		t.Fatalf("expected shared cap to reject a grow, a=%v b=%v used=%d max=%d", errA, errB, budget.Used(), budget.Max())
	}
	if budget.Used() > budget.Max() {
		t.Fatalf("used %d exceeds max %d", budget.Used(), budget.Max())
	}
}

func TestSharedBudget_OpenRejectsWhenFloorExceedsCap(t *testing.T) {
	idx := New()
	defer idx.Close()
	if err := idx.SetSharedBudget(NewSharedBudget(1)); err != ErrArenaLimit {
		t.Fatalf("want ErrArenaLimit, got %v", err)
	}
}

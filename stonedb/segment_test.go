// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import "testing"

func TestSegmentTracker_SealOnCommitBoundary(t *testing.T) {
	const target = 1000
	st := newSegmentTracker(target, 0)

	st.addStaleAtOffset(100, 50)
	st.maybeSealAfterCommit(500)
	if st.sealedCount() != 0 {
		t.Fatalf("expected no seal below target, got %d sealed", st.sealedCount())
	}

	st.maybeSealAfterCommit(1000)
	if st.sealedCount() != 1 {
		t.Fatalf("expected one sealed segment, got %d", st.sealedCount())
	}
	start, end, ok := st.bounds(0, 2000)
	if !ok || start != 0 || end != 1000 {
		t.Fatalf("unexpected first segment bounds: %d-%d ok=%v", start, end, ok)
	}
	start, end, ok = st.bounds(1, 2000)
	if !ok || start != 1000 || end != 2000 {
		t.Fatalf("unexpected active segment bounds: %d-%d ok=%v", start, end, ok)
	}
}

func TestSegmentTracker_StaleAttributedByOffset(t *testing.T) {
	st := newSegmentTracker(100, 0)
	st.maybeSealAfterCommit(100)
	st.maybeSealAfterCommit(200)

	st.addStaleAtOffset(50, 10)
	st.addStaleAtOffset(150, 20)

	if st.staleBytes(0) != 10 {
		t.Fatalf("segment 0 stale=%d want 10", st.staleBytes(0))
	}
	if st.staleBytes(1) != 20 {
		t.Fatalf("segment 1 stale=%d want 20", st.staleBytes(1))
	}
	if st.totalStaleBytes() != 30 {
		t.Fatalf("total stale=%d want 30", st.totalStaleBytes())
	}
}

func TestSegmentTracker_DeductStale(t *testing.T) {
	st := newSegmentTracker(100, 0)
	st.addStaleAtOffset(10, 40)
	st.deductStale(0, 15)
	if st.staleBytes(0) != 25 {
		t.Fatalf("stale after deduct=%d want 25", st.staleBytes(0))
	}
}

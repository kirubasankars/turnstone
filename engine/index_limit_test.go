// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"errors"
	"testing"
)

func TestIndexArenaLimit_RejectsWrite(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{UnsafeDisableFsync: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	baseline := db.index.IndexArenaUsedBytes()
	if baseline <= 0 {
		t.Fatalf("expected baseline arena usage, got %d", baseline)
	}
	db.index.SetMaxArenaBytes(baseline + 4096)

	key := []byte("limit-key")
	val := make([]byte, 512)
	var limitErr error
	for i := 0; i < 10000; i++ {
		tx := db.NewTransaction(true)
		if err := tx.Put(key, val); err != nil {
			if errors.Is(err, ErrIndexArenaLimit) {
				limitErr = err
				tx.Discard()
				break
			}
			t.Fatalf("unexpected put error: %v", err)
		}
		if err := tx.Commit(); err != nil {
			if errors.Is(err, ErrIndexArenaLimit) {
				limitErr = err
				break
			}
			t.Fatalf("unexpected commit error: %v", err)
		}
	}
	if !errors.Is(limitErr, ErrIndexArenaLimit) {
		t.Fatalf("expected ErrIndexArenaLimit, got %v", limitErr)
	}
}

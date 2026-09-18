// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"os"
	"testing"

	"turnstone/internal/mlock"
)

func TestOpen_MlockPagePoolAndIndex(t *testing.T) {
	probe := make([]byte, os.Getpagesize())
	if err := mlock.Lock(probe); err != nil {
		if mlock.IsDenied(err) || !mlock.Supported() {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	mlock.Unlock(probe)

	dir := t.TempDir()
	db, err := Open(dir, Options{
		Mlock:              true,
		SharedBuffersBytes: sharedBufferPageSize * sharedBufferMinPages,
		ValueCacheBytes:    -1,
		UnsafeDisableFsync: true,
	})
	if err != nil {
		if mlock.IsDenied(err) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

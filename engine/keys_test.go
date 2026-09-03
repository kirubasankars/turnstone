// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestListKeys(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, Options{UnsafeDisableFsync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("Put alpha: %v", err)
	}
	if err := tx.Put([]byte("beta"), []byte("2")); err != nil {
		t.Fatalf("Put beta: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx2 := db.NewTransaction(true)
	if err := tx2.Delete([]byte("alpha")); err != nil {
		t.Fatalf("Delete alpha: %v", err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Commit delete: %v", err)
	}

	keys, err := db.ListKeys("", 0, 10)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 1 || keys[0] != "beta" {
		t.Fatalf("expected [beta], got %v", keys)
	}

	prefixKeys, err := db.ListKeys("be", 0, 10)
	if err != nil {
		t.Fatalf("ListKeys prefix: %v", err)
	}
	if len(prefixKeys) != 1 || prefixKeys[0] != "beta" {
		t.Fatalf("expected [beta] for prefix be, got %v", prefixKeys)
	}
}

func TestListKeysLimit(t *testing.T) {
	if os.Getenv("TS_UNSAFE_DISABLE_FSYNC") == "" {
		t.Setenv("TS_UNSAFE_DISABLE_FSYNC", "true")
	}
	dir := filepath.Join(t.TempDir(), "db")
	db, err := Open(dir, Options{UnsafeDisableFsync: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if err := tx.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	keys, err := db.ListKeys("", 0, 2)
	if err != nil {
		t.Fatalf("ListKeys: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

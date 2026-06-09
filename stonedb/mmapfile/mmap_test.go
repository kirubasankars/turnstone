// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package mmapfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscardUnmapsWithoutSync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	f, err := Open(path, PageSize*4)
	if err != nil {
		t.Fatal(err)
	}
	f.Data()[0] = 42
	if err := f.Discard(); err != nil {
		t.Fatalf("Discard failed: %v", err)
	}
	if f.Data() != nil {
		t.Fatal("expected nil data after Discard")
	}
	if f.Len() != 0 {
		t.Fatal("expected zero length after Discard")
	}
}

func TestOpenPrivateUnmapsWithoutPersisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "private.bin")

	f, err := OpenPrivate(path, PageSize*4)
	if err != nil {
		t.Fatal(err)
	}
	f.Data()[0] = 42
	if err := f.Discard(); err != nil {
		t.Fatalf("Discard failed: %v", err)
	}

	reopened, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range reopened {
		if b != 0 {
			t.Fatalf("MAP_PRIVATE write leaked to disk: byte=%d", b)
		}
	}
}

func TestCloseSyncsAndUnmaps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	f, err := Open(path, PageSize*4)
	if err != nil {
		t.Fatal(err)
	}
	f.Data()[0] = 99
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if f.Data() != nil {
		t.Fatal("expected nil data after Close")
	}
}

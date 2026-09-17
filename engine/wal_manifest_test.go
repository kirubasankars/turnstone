// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func testManifest(id uint32, file string) *walManifest {
	return &walManifest{
		Version:     walManifestVersion,
		SegmentSize: 4096,
		ActiveID:    id,
		Segments: []walManifestSegment{{
			ID:      id,
			File:    file,
			BaseLSN: 0,
		}},
	}
}

func TestSaveWalManifest_AtomicReplace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walManifestName)

	if err := saveWalManifest(path, testManifest(1, "seg-000001.wal")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(walManifestTmpPath(path)); !os.IsNotExist(err) {
		t.Fatal("expected no leftover tmp after a successful save")
	}

	if err := saveWalManifest(path, testManifest(2, "seg-000002.wal")); err != nil {
		t.Fatal(err)
	}
	got, err := loadWalManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveID != 2 || got.Segments[0].File != "seg-000002.wal" {
		t.Fatalf("loaded %+v, want active 2 / seg-000002.wal", got)
	}
}

func TestSaveWalManifest_TornTmpKeepsOld(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walManifestName)
	if err := saveWalManifest(path, testManifest(1, "seg-000001.wal")); err != nil {
		t.Fatal(err)
	}

	// Crash during the next write: dest is complete, tmp is garbage.
	if err := os.WriteFile(walManifestTmpPath(path), []byte("{"), fileMode); err != nil {
		t.Fatal(err)
	}
	got, err := loadWalManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveID != 1 {
		t.Fatalf("ActiveID=%d, want 1 (torn tmp must not replace dest)", got.ActiveID)
	}
}

func TestLoadWalManifest_PromotesCompleteTmp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walManifestName)
	tmp := walManifestTmpPath(path)

	data, err := json.MarshalIndent(testManifest(3, "seg-000003.wal"), "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, append(data, '\n'), fileMode); err != nil {
		t.Fatal(err)
	}

	got, err := loadWalManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ActiveID != 3 {
		t.Fatalf("ActiveID=%d, want 3 from recovered tmp", got.ActiveID)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected dest after promoting tmp: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("tmp should be gone after promote")
	}
}

func TestLoadWalManifest_TornTmpWithoutDest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walManifestName)
	if err := os.WriteFile(walManifestTmpPath(path), []byte("{"), fileMode); err != nil {
		t.Fatal(err)
	}
	_, err := loadWalManifest(path)
	if !os.IsNotExist(err) {
		t.Fatalf("torn tmp without dest: %v, want ErrNotExist", err)
	}
}

func TestOpenDataLog_RecoversManifestTmp(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 4096)
	if err != nil {
		t.Fatal(err)
	}
	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	if _, err := log.AppendEncoded(payload, true); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	walDir := filepath.Join(dir, walDirName)
	path := filepath.Join(walDir, walManifestName)
	tmp := walManifestTmpPath(path)
	if err := os.Rename(path, tmp); err != nil {
		t.Fatal(err)
	}

	log2, err := OpenDataLog(dir, nil, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer log2.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("open should promote tmp to dest: %v", err)
	}
	if log2.SegmentCount() != 1 {
		t.Fatalf("segments=%d, want 1 after tmp recover", log2.SegmentCount())
	}
}

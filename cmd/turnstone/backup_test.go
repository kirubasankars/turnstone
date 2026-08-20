// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateRestoreChain(t *testing.T) {
	full := BackupMeta{
		Type:    backupTypeFull,
		BaseLSN: 0,
		EndLSN:  100,
		SHA256:  "aaa",
	}
	diff := BackupMeta{
		Type:         backupTypeDifferential,
		BaseLSN:      100,
		EndLSN:       250,
		ParentSHA256: "aaa",
		SHA256:       "bbb",
	}

	if err := validateRestoreChain([]BackupMeta{full}); err != nil {
		t.Fatalf("full only: %v", err)
	}
	if err := validateRestoreChain([]BackupMeta{full, diff}); err != nil {
		t.Fatalf("full+diff: %v", err)
	}

	badBase := diff
	badBase.BaseLSN = 99
	if err := validateRestoreChain([]BackupMeta{full, badBase}); err == nil {
		t.Fatal("expected base_lsn mismatch error")
	}

	badParent := diff
	badParent.ParentSHA256 = "wrong"
	if err := validateRestoreChain([]BackupMeta{full, badParent}); err == nil {
		t.Fatal("expected parent_sha256 mismatch error")
	}

	badFirst := full
	badFirst.Type = backupTypeDifferential
	if err := validateRestoreChain([]BackupMeta{badFirst}); err == nil {
		t.Fatal("expected first backup type error")
	}
}

func TestLoadBackupMetaLegacyOpID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, defaultBackupMeta)
	legacy := `{
  "timestamp": "2026-09-12T00:00:00Z",
  "database": "1",
  "type": "full",
  "base_opid": 0,
  "end_opid": 42,
  "sha256": "deadbeef"
}`
	if err := os.WriteFile(path, []byte(legacy), 0644); err != nil {
		t.Fatal(err)
	}
	meta, err := loadBackupMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.EndLSN != 42 {
		t.Fatalf("expected end_lsn 42, got %d", meta.EndLSN)
	}
}

func TestResolveRestoreChain(t *testing.T) {
	dirs, err := resolveRestoreChain("single", "")
	if err != nil || len(dirs) != 1 || dirs[0] != "single" {
		t.Fatalf("single dir: dirs=%v err=%v", dirs, err)
	}

	dirs, err = resolveRestoreChain("", "a, b ,c")
	if err != nil || len(dirs) != 3 {
		t.Fatalf("chain: dirs=%v err=%v", dirs, err)
	}
}

func TestParseReplLogRangePayload(t *testing.T) {
	payload := []byte{
		0, 0, 0, 1, '1',
		0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 3,
		0x01, 0x02, 0x03,
	}
	end, data, err := parseReplLogRangePayload(payload, "1")
	if err != nil {
		t.Fatal(err)
	}
	if end != 3 || len(data) != 3 {
		t.Fatalf("got end=%d len=%d", end, len(data))
	}
}

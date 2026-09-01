// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateRestoreChain(t *testing.T) {
	full := Meta{
		Type:    TypeFull,
		BaseLSN: 0,
		EndLSN:  100,
		SHA256:  "aaa",
	}
	diff := Meta{
		Type:         TypeDifferential,
		BaseLSN:      100,
		EndLSN:       250,
		ParentSHA256: "aaa",
		SHA256:       "bbb",
	}

	if err := ValidateRestoreChain([]Meta{full}); err != nil {
		t.Fatalf("full only: %v", err)
	}
	if err := ValidateRestoreChain([]Meta{full, diff}); err != nil {
		t.Fatalf("full+diff: %v", err)
	}

	badBase := diff
	badBase.BaseLSN = 99
	if err := ValidateRestoreChain([]Meta{full, badBase}); err == nil {
		t.Fatal("expected base_lsn mismatch error")
	}

	badParent := diff
	badParent.ParentSHA256 = "wrong"
	if err := ValidateRestoreChain([]Meta{full, badParent}); err == nil {
		t.Fatal("expected parent_sha256 mismatch error")
	}

	badFirst := full
	badFirst.Type = TypeDifferential
	if err := ValidateRestoreChain([]Meta{badFirst}); err == nil {
		t.Fatal("expected first backup type error")
	}

	if err := ValidateRestoreChain(nil); err == nil {
		t.Fatal("expected empty chain error")
	}

	badFullBase := full
	badFullBase.BaseLSN = 10
	if err := ValidateRestoreChain([]Meta{badFullBase}); err == nil {
		t.Fatal("expected full non-zero base_lsn error")
	}

	badSecondType := full
	badSecond := Meta{Type: TypeFull, BaseLSN: 100, EndLSN: 200, SHA256: "bbb"}
	if err := ValidateRestoreChain([]Meta{badSecondType, badSecond}); err == nil {
		t.Fatal("expected second backup must be differential error")
	}
}

func TestLoadMetaErrors(t *testing.T) {
	if _, err := LoadMeta(filepath.Join(t.TempDir(), "missing.meta")); err == nil {
		t.Fatal("expected missing file error")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, DefaultMetaFile)
	if err := os.WriteFile(path, []byte("{"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMeta(path); err == nil {
		t.Fatal("expected invalid json error")
	}

	writeRawMeta(t, dir, `{"type":"full","base_lsn":0,"end_lsn":1,"sha256":"x"}`)
	if _, err := LoadMeta(path); err == nil {
		t.Fatal("expected missing database error")
	}

	writeRawMeta(t, dir, `{"database":"1","type":"snapshot","base_lsn":0,"end_lsn":1,"sha256":"x"}`)
	if _, err := LoadMeta(path); err == nil {
		t.Fatal("expected unknown type error")
	}
}

func TestResolveRestoreChain_EmptyChain(t *testing.T) {
	if _, err := ResolveRestoreChain("", " , "); err == nil {
		t.Fatal("expected empty chain error")
	}
}

func TestResolveWALFile(t *testing.T) {
	if got := ResolveWALFile("dir", "wal.bin", true); got != filepath.Join("dir", "wal.bin.gz") {
		t.Fatalf("unexpected compressed path: %s", got)
	}
}

func TestLoadMetaLegacyOpID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, DefaultMetaFile)
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
	meta, err := LoadMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	if meta.EndLSN != 42 {
		t.Fatalf("expected end_lsn 42, got %d", meta.EndLSN)
	}
}

func TestResolveRestoreChain(t *testing.T) {
	dirs, err := ResolveRestoreChain("single", "")
	if err != nil || len(dirs) != 1 || dirs[0] != "single" {
		t.Fatalf("single dir: dirs=%v err=%v", dirs, err)
	}

	dirs, err = ResolveRestoreChain("", "a, b ,c")
	if err != nil || len(dirs) != 3 {
		t.Fatalf("chain: dirs=%v err=%v", dirs, err)
	}
}

func TestParseLogRangePayload(t *testing.T) {
	payload := []byte{
		0, 0, 0, 1, '1',
		0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 3,
		0x01, 0x02, 0x03,
	}
	end, data, err := parseLogRangePayload(payload, "1")
	if err != nil {
		t.Fatal(err)
	}
	if end != 3 || len(data) != 3 {
		t.Fatalf("got end=%d len=%d", end, len(data))
	}
}

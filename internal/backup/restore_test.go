// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRunRestore_NoBackupDirs(t *testing.T) {
	_, err := RunRestore(context.Background(), RestoreOptions{
		OutHome: t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected no backup dirs error")
	}
}

func TestRunRestore_OutHomeExists(t *testing.T) {
	outHome := t.TempDir()
	backupDir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    outHome,
	})
	if err == nil {
		t.Fatal("expected out home exists error")
	}
}

func TestRunRestore_MissingMeta(t *testing.T) {
	backupDir := t.TempDir()
	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if err == nil {
		t.Fatal("expected missing meta error")
	}
}

func TestRunRestore_MissingWALFile(t *testing.T) {
	backupDir := t.TempDir()
	writeRawMeta(t, backupDir, `{
  "timestamp": "2026-09-12T00:00:00Z",
  "database": "1",
  "type": "full",
  "base_lsn": 0,
  "end_lsn": 10,
  "sha256": "abc"
}`)

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if err == nil {
		t.Fatal("expected missing wal file error")
	}
}

func TestRunRestore_InvalidChainGap(t *testing.T) {
	fullDir := t.TempDir()
	diffDir := t.TempDir()
	wal := writeEngineWAL(t, "k1")
	fullMeta := writeBackupArtifact(t, fullDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	diffWal := writeEngineWAL(t, "k2")
	writeBackupArtifact(t, diffDir, "1", TypeDifferential, fullMeta.EndLSN+1, fullMeta.EndLSN+1+uint64(len(diffWal)), diffWal, false, fullMeta.SHA256)

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{fullDir, diffDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if err == nil {
		t.Fatal("expected chain base_lsn mismatch error")
	}
}

func TestRunRestore_DiffOnlyRejected(t *testing.T) {
	diffDir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	writeBackupArtifact(t, diffDir, "1", TypeDifferential, 100, 100+uint64(len(wal)), wal, false, "aaa")

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{diffDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if err == nil {
		t.Fatal("expected first backup must be full error")
	}
}

func TestRunRestore_MixedDatabases(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	wal1 := writeEngineWAL(t, "a")
	wal2 := writeEngineWAL(t, "b")
	meta1 := writeBackupArtifact(t, dir1, "1", TypeFull, 0, uint64(len(wal1)), wal1, false, "")
	writeBackupArtifact(t, dir2, "2", TypeDifferential, meta1.EndLSN, meta1.EndLSN+uint64(len(wal2)), wal2, false, meta1.SHA256)

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{dir1, dir2},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if err == nil {
		t.Fatal("expected mixed database error")
	}
}

func TestRunRestore_ChecksumMismatch(t *testing.T) {
	backupDir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	meta := writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")
	meta.SHA256 = "deadbeef"
	if err := SaveMeta(backupDir, meta); err != nil {
		t.Fatal(err)
	}

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
		Verify:     true,
	})
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}

func TestRunRestore_CorruptWALRejected(t *testing.T) {
	backupDir := t.TempDir()
	garbage := []byte{0xFF, 0xFE, 0xFD, 0xFC}
	writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(garbage)), garbage, false, "")

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
		Verify:     true,
	})
	if err == nil {
		t.Fatal("expected corrupt wal apply error")
	}
}

func TestRunRestore_TruncatedWALRejected(t *testing.T) {
	backupDir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	walPath := filepath.Join(backupDir, DefaultWALFile)
	if err := os.Truncate(walPath, int64(len(wal)/2)); err != nil {
		t.Fatal(err)
	}

	_, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
		Verify:     true,
	})
	if err == nil {
		t.Fatal("expected truncated wal failure")
	}
}

func TestRunRestore_CompressedChain(t *testing.T) {
	fullDir := t.TempDir()
	diffDir := t.TempDir()
	wal1 := writeEngineWAL(t, "base")
	fullMeta := writeBackupArtifact(t, fullDir, "1", TypeFull, 0, uint64(len(wal1)), wal1, true, "")

	wal2 := writeEngineWAL(t, "delta")
	writeBackupArtifact(t, diffDir, "1", TypeDifferential, fullMeta.EndLSN, fullMeta.EndLSN+uint64(len(wal2)), wal2, true, fullMeta.SHA256)

	outHome := filepath.Join(t.TempDir(), "restored")
	meta, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{fullDir, diffDir},
		OutHome:    outHome,
		Verify:     true,
	})
	if err != nil {
		t.Fatalf("compressed chain restore failed: %v", err)
	}
	if meta.EndLSN <= fullMeta.EndLSN {
		t.Fatalf("expected final end_lsn > full end_lsn, got %d vs %d", meta.EndLSN, fullMeta.EndLSN)
	}
}

func TestRunRestore_SuccessWithoutVerify(t *testing.T) {
	backupDir := t.TempDir()
	wal := writeEngineWAL(t, "restore-key")
	meta := writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	outHome := filepath.Join(t.TempDir(), "restored")
	got, err := RunRestore(context.Background(), RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    outHome,
		Verify:     false,
	})
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if got.EndLSN != meta.EndLSN {
		t.Fatalf("end_lsn %d != meta %d", got.EndLSN, meta.EndLSN)
	}
}

func TestRunRestore_ContextCancellation(t *testing.T) {
	backupDir := t.TempDir()
	wal := writeEngineWAL(t, "k1", "k2", "k3", "k4", "k5")
	writeBackupArtifact(t, backupDir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RunRestore(ctx, RestoreOptions{
		BackupDirs: []string{backupDir},
		OutHome:    filepath.Join(t.TempDir(), "new_home"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
}

func TestResolveWALInputFile_NotFound(t *testing.T) {
	_, _, err := ResolveWALInputFile(t.TempDir(), DefaultWALFile)
	if err == nil {
		t.Fatal("expected not found error")
	}
}

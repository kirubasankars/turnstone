// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/protocol"
)

func TestRunBackup_RequiresTLS(t *testing.T) {
	_, err := RunBackup(context.Background(), BackupOptions{
		Type:   TypeFull,
		OutDir: t.TempDir(),
	})
	if err == nil || err.Error() != "TLS config is required" {
		t.Fatalf("expected TLS required error, got %v", err)
	}
}

func TestRunBackup_InvalidType(t *testing.T) {
	tlsConf := testTLSConfig(t)
	_, err := RunBackup(context.Background(), BackupOptions{
		Type:   "snapshot",
		OutDir: t.TempDir(),
		TLS:    tlsConf,
	})
	if err == nil {
		t.Fatal("expected invalid type error")
	}
}

func TestRunBackup_DifferentialRequiresBase(t *testing.T) {
	tlsConf := testTLSConfig(t)
	_, err := RunBackup(context.Background(), BackupOptions{
		Type:   TypeDifferential,
		OutDir: t.TempDir(),
		TLS:    tlsConf,
	})
	if err == nil {
		t.Fatal("expected differential base requirement error")
	}
}

func TestRunBackup_BaseMetaMissing(t *testing.T) {
	tlsConf := testTLSConfig(t)
	_, err := RunBackup(context.Background(), BackupOptions{
		Type:         TypeDifferential,
		OutDir:       t.TempDir(),
		BaseMetaPath: filepath.Join(t.TempDir(), "missing.meta"),
		DBName:       "1",
		TLS:          tlsConf,
	})
	if err == nil {
		t.Fatal("expected missing base meta error")
	}
}

func TestRunBackup_BaseMetaDatabaseMismatch(t *testing.T) {
	tlsConf := testTLSConfig(t)
	dir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	writeBackupArtifact(t, dir, "2", TypeFull, 0, uint64(len(wal)), wal, false, "")

	_, err := RunBackup(context.Background(), BackupOptions{
		Host:         "127.0.0.1:1",
		DBName:       "1",
		Type:         TypeDifferential,
		OutDir:       t.TempDir(),
		BaseMetaPath: filepath.Join(dir, DefaultMetaFile),
		TLS:          tlsConf,
	})
	if err == nil {
		t.Fatal("expected database mismatch error")
	}
}

func TestRunBackup_ConnectFailure(t *testing.T) {
	tlsConf := testTLSConfig(t)
	_, err := RunBackup(context.Background(), BackupOptions{
		Host:     "127.0.0.1:1",
		DBName:   "1",
		Type:     TypeFull,
		OutDir:   t.TempDir(),
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	})
	if err == nil {
		t.Fatal("expected connect failure")
	}
}

func TestRunBackup_ContextCancellationRemovesPartialFile(t *testing.T) {
	tlsConf := testTLSConfig(t)
	outDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RunBackup(ctx, BackupOptions{
		Host:     "127.0.0.1:1",
		DBName:   "1",
		Type:     TypeFull,
		OutDir:   outDir,
		WaitIdle: time.Second,
		TLS:      tlsConf,
	})
	if err == nil {
		t.Fatal("expected backup failure after cancel")
	}
	if _, err := os.Stat(ResolveWALFile(outDir, DefaultWALFile, false)); !os.IsNotExist(err) {
		t.Fatalf("expected partial wal file removed, stat err=%v", err)
	}
}

func TestRunBackup_ContextCancellationDuringStream(t *testing.T) {
	tlsConf := testTLSConfig(t)
	wal := writeEngineWAL(t, "k")
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")
		_ = writeBackupLogRange(conn, "1", 0, uint64(len(wal)), wal)
		time.Sleep(2 * time.Second)
	})
	defer stop()

	outDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := RunBackup(ctx, BackupOptions{
		Host:     addr,
		DBName:   "1",
		Type:     TypeFull,
		OutDir:   outDir,
		WaitIdle: 5 * time.Second,
		TLS:      tlsConf,
	})
	if err == nil {
		t.Fatal("expected backup failure after stream timeout/cancel")
	}
	if _, err := os.Stat(ResolveWALFile(outDir, DefaultWALFile, false)); !os.IsNotExist(err) {
		t.Fatalf("expected partial wal file removed after cancel, stat err=%v", err)
	}
}

func TestRunBackup_FromLSNOverride(t *testing.T) {
	tlsConf := testTLSConfig(t)
	dir := t.TempDir()
	wal := writeEngineWAL(t, "k")
	meta := writeBackupArtifact(t, dir, "1", TypeFull, 0, uint64(len(wal)), wal, false, "")

	_, err := RunBackup(context.Background(), BackupOptions{
		Host:         "127.0.0.1:1",
		DBName:       "1",
		Type:         TypeDifferential,
		OutDir:       t.TempDir(),
		FromLSN:      meta.EndLSN + 1000,
		BaseMetaPath: filepath.Join(dir, DefaultMetaFile),
		TLS:          tlsConf,
		WaitIdle:     50 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected failure when connecting with explicit from-lsn to dead host")
	}
}

// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/internal/backup"
)

func TestBackup_RejectsUndefinedDatabase(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "bk_undef", clientTLS)
	defer cancel()

	_, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   filepath.Join(baseDir, "out_undef"),
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err == nil {
		t.Fatal("expected backup failure against undefined database")
	}
}

func TestBackup_RejectsUnknownDatabase(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "bk_unknown", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	_, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     addr,
		DBName:   "99",
		OutDir:   filepath.Join(baseDir, "out_unknown"),
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err == nil {
		t.Fatal("expected backup failure for unknown database")
	}
}

func TestBackup_RejectsClientCertificate(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)

	_, addr, cancel := startServerNode(t, baseDir, "bk_clientcert", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "k", "v")

	_, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   filepath.Join(baseDir, "out_clientcert"),
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      clientTLS,
	})
	if err == nil {
		t.Fatal("expected backup failure with client certificate")
	}
}

func TestBackup_DifferentialFailsWhenBaseWALPurged(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	primarySrv, addr, cancel := startServerNode(t, baseDir, "bk_purge", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "base", "v")

	ctx := context.Background()
	fullDir := filepath.Join(baseDir, "bk_purge_full")
	fullMeta, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}

	writeKeyVal(t, client, "after-full", "v2")

	st1 := primarySrv.stores["1"]
	if err := st1.DB.MarkRetention(); err != nil {
		t.Fatal(err)
	}
	st1.RemoveAllReplicas()
	st1.EnforceRetentionPolicy()
	walPath := filepath.Join(baseDir, "bk_purge", "data", "1", "wal", "seg-000001.wal")
	if err := os.Remove(walPath); err != nil {
		t.Fatal(err)
	}

	_, err = backup.RunBackup(ctx, backup.BackupOptions{
		Host:         addr,
		DBName:       "1",
		OutDir:       filepath.Join(baseDir, "bk_purge_diff"),
		Type:         backup.TypeDifferential,
		BaseMetaPath: filepath.Join(fullDir, backup.DefaultMetaFile),
		WaitIdle:     200 * time.Millisecond,
		TLS:          adminTLS,
	})
	if err == nil {
		t.Fatal("expected differential backup failure when base WAL is purged")
	}
	_ = fullMeta
}

func TestBackup_DifferentialWrongBaseMetaDatabase(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "bk_meta", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1", "2")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "k", "v")

	ctx := context.Background()
	fullDir := filepath.Join(baseDir, "bk_meta_full")
	if _, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     addr,
		DBName:   "2",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	}); err != nil {
		t.Fatalf("full backup db2 failed: %v", err)
	}

	_, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:         addr,
		DBName:       "1",
		OutDir:       filepath.Join(baseDir, "bk_meta_diff"),
		Type:         backup.TypeDifferential,
		BaseMetaPath: filepath.Join(fullDir, backup.DefaultMetaFile),
		WaitIdle:     200 * time.Millisecond,
		TLS:          adminTLS,
	})
	if err == nil {
		t.Fatal("expected base meta database mismatch error")
	}
}

func TestRestore_RejectsExistingOutHome(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "restore_exist", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "k", "v")

	fullDir := filepath.Join(baseDir, "restore_exist_full")
	if _, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	}); err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	existingHome := filepath.Join(baseDir, "already_there")
	if err := os.MkdirAll(existingHome, 0755); err != nil {
		t.Fatal(err)
	}

	_, err := backup.RunRestore(context.Background(), backup.RestoreOptions{
		BackupDirs: []string{fullDir},
		OutHome:    existingHome,
	})
	if err == nil {
		t.Fatal("expected restore failure when out home exists")
	}
}

func TestRestore_RejectsBrokenChain(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "restore_chain", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "k1", "v1")

	ctx := context.Background()
	fullDir := filepath.Join(baseDir, "restore_chain_full")
	fullMeta, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}

	writeKeyVal(t, client, "k2", "v2")
	diffDir := filepath.Join(baseDir, "restore_chain_diff")
	if _, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:         addr,
		DBName:       "1",
		OutDir:       diffDir,
		Type:         backup.TypeDifferential,
		BaseMetaPath: filepath.Join(fullDir, backup.DefaultMetaFile),
		WaitIdle:     200 * time.Millisecond,
		TLS:          adminTLS,
	}); err != nil {
		t.Fatalf("diff backup failed: %v", err)
	}

	metaPath := filepath.Join(diffDir, backup.DefaultMetaFile)
	meta, err := backup.LoadMeta(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	meta.BaseLSN = fullMeta.EndLSN + 100
	if err := backup.SaveMeta(diffDir, meta); err != nil {
		t.Fatal(err)
	}

	_, err = backup.RunRestore(ctx, backup.RestoreOptions{
		BackupDirs: []string{fullDir, diffDir},
		OutHome:    filepath.Join(baseDir, "restore_chain_out"),
		Verify:     true,
	})
	if err == nil {
		t.Fatal("expected restore failure for broken chain")
	}
}

func TestRestore_RejectsChecksumMismatch(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, addr, cancel := startServerNode(t, baseDir, "restore_crc", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "crc-key", "crc-val")

	fullDir := filepath.Join(baseDir, "restore_crc_full")
	if _, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     addr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	}); err != nil {
		t.Fatalf("backup failed: %v", err)
	}

	meta, err := backup.LoadMeta(filepath.Join(fullDir, backup.DefaultMetaFile))
	if err != nil {
		t.Fatal(err)
	}
	meta.SHA256 = "badchecksum"
	if err := backup.SaveMeta(fullDir, meta); err != nil {
		t.Fatal(err)
	}

	_, err = backup.RunRestore(context.Background(), backup.RestoreOptions{
		BackupDirs: []string{fullDir},
		OutHome:    filepath.Join(baseDir, "restore_crc_out"),
		Verify:     true,
	})
	if err == nil {
		t.Fatal("expected checksum mismatch on restore")
	}
}

func TestBackup_ServerUnreachable(t *testing.T) {
	baseDir, _ := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, err := backup.RunBackup(context.Background(), backup.BackupOptions{
		Host:     "127.0.0.1:1",
		DBName:   "1",
		OutDir:   filepath.Join(baseDir, "unreachable"),
		Type:     backup.TypeFull,
		WaitIdle: 50 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err == nil {
		t.Fatal("expected unreachable server error")
	}
}

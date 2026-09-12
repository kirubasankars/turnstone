// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"turnstone/internal/backup"
)

func TestBackupRestore_FullIntegration(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_bk", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	client := connectClient(t, primaryAddr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "backup-key", "backup-value")

	ctx := context.Background()
	fullDir := filepath.Join(baseDir, "backup_full")
	fullMeta, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     primaryAddr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		Compress: false,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}
	if fullMeta.BaseLSN != 0 {
		t.Fatalf("expected full backup base_lsn 0, got %d", fullMeta.BaseLSN)
	}
	if fullMeta.EndLSN == 0 {
		t.Fatal("expected full backup end_lsn > 0")
	}

	restoredNode := filepath.Join(baseDir, "restored_full")
	finalMeta, err := backup.RunRestore(ctx, backup.RestoreOptions{
		BackupDirs: []string{fullDir},
		OutHome:    restoredNode,
		Verify:     true,
	})
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if finalMeta.EndLSN != fullMeta.EndLSN {
		t.Fatalf("restored end_lsn %d != backup end_lsn %d", finalMeta.EndLSN, fullMeta.EndLSN)
	}

	_, restoredAddr, cancelRestored := startServerNode(t, baseDir, "restored_full", clientTLS)
	defer cancelRestored()
	promoteNode(t, baseDir, restoredAddr, "1")

	restoredClient := connectClient(t, restoredAddr, clientTLS)
	defer restoredClient.Close()
	selectDatabase(t, restoredClient, "1")

	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		val := readKey(t, restoredClient, "backup-key")
		return string(val) == "backup-value"
	}, "restored database missing backup-key")
}

func TestBackupRestore_DifferentialChainIntegration(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")

	_, primaryAddr, cancelPrimary := startServerNode(t, baseDir, "primary_diff", clientTLS)
	defer cancelPrimary()
	promoteNode(t, baseDir, primaryAddr, "1")

	client := connectClient(t, primaryAddr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "base-key", "base-value")

	ctx := context.Background()
	fullDir := filepath.Join(baseDir, "backup_chain_full")
	fullMeta, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:     primaryAddr,
		DBName:   "1",
		OutDir:   fullDir,
		Type:     backup.TypeFull,
		Compress: true,
		WaitIdle: 200 * time.Millisecond,
		TLS:      adminTLS,
	})
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}

	writeKeyVal(t, client, "delta-key", "delta-value")

	diffDir := filepath.Join(baseDir, "backup_chain_diff")
	diffMeta, err := backup.RunBackup(ctx, backup.BackupOptions{
		Host:         primaryAddr,
		DBName:       "1",
		OutDir:       diffDir,
		Type:         backup.TypeDifferential,
		BaseMetaPath: filepath.Join(fullDir, backup.DefaultMetaFile),
		Compress:     true,
		WaitIdle:     200 * time.Millisecond,
		TLS:          adminTLS,
	})
	if err != nil {
		t.Fatalf("differential backup failed: %v", err)
	}
	if diffMeta.BaseLSN != fullMeta.EndLSN {
		t.Fatalf("diff base_lsn %d != full end_lsn %d", diffMeta.BaseLSN, fullMeta.EndLSN)
	}
	if diffMeta.EndLSN <= fullMeta.EndLSN {
		t.Fatalf("expected diff end_lsn > full end_lsn, got %d vs %d", diffMeta.EndLSN, fullMeta.EndLSN)
	}

	restoredNode := filepath.Join(baseDir, "restored_chain")
	finalMeta, err := backup.RunRestore(ctx, backup.RestoreOptions{
		BackupDirs: []string{fullDir, diffDir},
		OutHome:    restoredNode,
		Verify:     true,
	})
	if err != nil {
		t.Fatalf("restore chain failed: %v", err)
	}
	if finalMeta.EndLSN != diffMeta.EndLSN {
		t.Fatalf("restored end_lsn %d != diff end_lsn %d", finalMeta.EndLSN, diffMeta.EndLSN)
	}

	_, restoredAddr, cancelRestored := startServerNode(t, baseDir, "restored_chain", clientTLS)
	defer cancelRestored()
	promoteNode(t, baseDir, restoredAddr, "1")

	restoredClient := connectClient(t, restoredAddr, clientTLS)
	defer restoredClient.Close()
	selectDatabase(t, restoredClient, "1")

	waitForConditionOrTimeout(t, 5*time.Second, func() bool {
		base := readKey(t, restoredClient, "base-key")
		delta := readKey(t, restoredClient, "delta-key")
		return string(base) == "base-value" && string(delta) == "delta-value"
	}, "restored database missing keys from full+differential chain")
}

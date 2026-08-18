// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	backupTypeFull         = "full"
	backupTypeDifferential = "differential"
	defaultBackupFile      = "wal.bin"
	defaultBackupMeta      = "backup.meta"
)

// BackupMeta describes a WAL backup artifact set.
// OpID fields store the global byte LSN (exclusive-end replication cursor).
type BackupMeta struct {
	Timestamp    time.Time `json:"timestamp"`
	Database     string    `json:"database"`
	Type         string    `json:"type"`
	BaseOpID     uint64    `json:"base_opid"`
	EndOpID      uint64    `json:"end_opid"`
	ParentSHA256 string    `json:"parent_sha256,omitempty"`
	Compressed   bool      `json:"compressed"`
	SHA256       string    `json:"sha256"`
}

func loadBackupMeta(path string) (BackupMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return BackupMeta{}, err
	}
	var meta BackupMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return BackupMeta{}, fmt.Errorf("invalid backup meta: %w", err)
	}
	if meta.Database == "" {
		return BackupMeta{}, fmt.Errorf("backup meta missing database")
	}
	if meta.Type != backupTypeFull && meta.Type != backupTypeDifferential {
		return BackupMeta{}, fmt.Errorf("backup meta has unknown type %q", meta.Type)
	}
	return meta, nil
}

func saveBackupMeta(dir string, meta BackupMeta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, defaultBackupMeta), data, 0644)
}

func resolveBackupFile(dir, name string, compressed bool) string {
	if compressed && filepath.Ext(name) != ".gz" {
		return filepath.Join(dir, name+".gz")
	}
	return filepath.Join(dir, name)
}

func resolveBackupInputFile(dir, name string) (string, bool, error) {
	plain := filepath.Join(dir, name)
	if _, err := os.Stat(plain); err == nil {
		return plain, false, nil
	}
	gz := plain + ".gz"
	if _, err := os.Stat(gz); err == nil {
		return gz, true, nil
	}
	return "", false, fmt.Errorf("backup file not found: %s", plain)
}

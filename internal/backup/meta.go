// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	TypeFull         = "full"
	TypeDifferential = "differential"
	DefaultWALFile   = "wal.bin"
	DefaultMetaFile  = "backup.meta"
)

// Meta describes a WAL backup artifact set.
// LSN fields are the global byte offsets used as the replication cursor.
type Meta struct {
	Timestamp    time.Time `json:"timestamp"`
	Database     string    `json:"database"`
	Type         string    `json:"type"`
	BaseLSN      uint64    `json:"base_lsn"`
	EndLSN       uint64    `json:"end_lsn"`
	ParentSHA256 string    `json:"parent_sha256,omitempty"`
	Compressed   bool      `json:"compressed"`
	SHA256       string    `json:"sha256"`
}

type metaRaw struct {
	Timestamp    time.Time `json:"timestamp"`
	Database     string    `json:"database"`
	Type         string    `json:"type"`
	BaseLSN      uint64    `json:"base_lsn"`
	EndLSN       uint64    `json:"end_lsn"`
	BaseOpID     uint64    `json:"base_opid"`
	EndOpID      uint64    `json:"end_opid"`
	ParentSHA256 string    `json:"parent_sha256,omitempty"`
	Compressed   bool      `json:"compressed"`
	SHA256       string    `json:"sha256"`
}

func LoadMeta(path string) (Meta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Meta{}, err
	}
	var raw metaRaw
	if err := json.Unmarshal(data, &raw); err != nil {
		return Meta{}, fmt.Errorf("invalid backup meta: %w", err)
	}
	meta := Meta{
		Timestamp:    raw.Timestamp,
		Database:     raw.Database,
		Type:         raw.Type,
		BaseLSN:      raw.BaseLSN,
		EndLSN:       raw.EndLSN,
		ParentSHA256: raw.ParentSHA256,
		Compressed:   raw.Compressed,
		SHA256:       raw.SHA256,
	}
	if meta.BaseLSN == 0 && raw.BaseOpID != 0 {
		meta.BaseLSN = raw.BaseOpID
	}
	if meta.EndLSN == 0 && raw.EndOpID != 0 {
		meta.EndLSN = raw.EndOpID
	}
	if meta.Database == "" {
		return Meta{}, fmt.Errorf("backup meta missing database")
	}
	if meta.Type != TypeFull && meta.Type != TypeDifferential {
		return Meta{}, fmt.Errorf("backup meta has unknown type %q", meta.Type)
	}
	return meta, nil
}

func SaveMeta(dir string, meta Meta) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, DefaultMetaFile), data, 0644)
}

func ResolveWALFile(dir, name string, compressed bool) string {
	if compressed && filepath.Ext(name) != ".gz" {
		return filepath.Join(dir, name+".gz")
	}
	return filepath.Join(dir, name)
}

func ResolveWALInputFile(dir, name string) (string, bool, error) {
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

func ValidateRestoreChain(metas []Meta) error {
	if len(metas) == 0 {
		return fmt.Errorf("empty backup chain")
	}
	if metas[0].Type != TypeFull {
		return fmt.Errorf("first backup must be type full, got %q", metas[0].Type)
	}
	if metas[0].BaseLSN != 0 {
		return fmt.Errorf("full backup must start at LSN 0, got %d", metas[0].BaseLSN)
	}

	prevEnd := metas[0].EndLSN
	prevSHA := metas[0].SHA256
	for i := 1; i < len(metas); i++ {
		meta := metas[i]
		if meta.Type != TypeDifferential {
			return fmt.Errorf("backup %d must be differential, got %q", i, meta.Type)
		}
		if meta.BaseLSN != prevEnd {
			return fmt.Errorf("backup %d base_lsn %d does not match previous end_lsn %d", i, meta.BaseLSN, prevEnd)
		}
		if meta.ParentSHA256 != "" && meta.ParentSHA256 != prevSHA {
			return fmt.Errorf("backup %d parent_sha256 does not match previous backup", i)
		}
		prevEnd = meta.EndLSN
		prevSHA = meta.SHA256
	}
	return nil
}

func ResolveRestoreChain(inDir, chain string) ([]string, error) {
	if chain == "" {
		return []string{inDir}, nil
	}
	parts := strings.Split(chain, ",")
	dirs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dirs = append(dirs, part)
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("empty --chain")
	}
	return dirs, nil
}

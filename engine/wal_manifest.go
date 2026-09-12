// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	walDirName       = "wal"
	walManifestName  = "manifest.json"
	defaultWalSegSize = 64 << 20 // 64 MiB
	walManifestVersion = 1
)

type walManifestSegment struct {
	ID      uint32 `json:"id"`
	File    string `json:"file"`
	BaseLSN int64  `json:"base_lsn"`
	EndLSN  int64  `json:"end_lsn,omitempty"`
}

type walManifest struct {
	Version     int                  `json:"version"`
	SegmentSize int64                `json:"segment_size"`
	ScanFloor   int64                `json:"scan_floor"`
	ActiveID    uint32               `json:"active_id"`
	Segments    []walManifestSegment `json:"segments"`
}

func walSegmentFileName(id uint32) string {
	return fmt.Sprintf("seg-%06d.wal", id)
}

func loadWalManifest(path string) (*walManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m walManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m.Version != walManifestVersion {
		return nil, fmt.Errorf("unsupported wal manifest version %d", m.Version)
	}
	if len(m.Segments) == 0 {
		return nil, fmt.Errorf("wal manifest has no segments")
	}
	return &m, nil
}

func saveWalManifest(path string, m *walManifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, fileMode); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func migrateLegacyDataLog(dbDir, walDir string, segmentSize int64) (*walManifest, error) {
	legacyPath := filepath.Join(dbDir, logFileName)
	if _, err := os.Stat(legacyPath); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(walDir, dirMode); err != nil {
		return nil, err
	}
	segName := walSegmentFileName(1)
	segPath := filepath.Join(walDir, segName)
	if err := os.Rename(legacyPath, segPath); err != nil {
		return nil, fmt.Errorf("migrate legacy log: %w", err)
	}
	m := &walManifest{
		Version:     walManifestVersion,
		SegmentSize: segmentSize,
		ActiveID:    1,
		Segments: []walManifestSegment{{
			ID:      1,
			File:    segName,
			BaseLSN: 0,
		}},
	}
	return m, saveWalManifest(filepath.Join(walDir, walManifestName), m)
}

func createFreshWalManifest(walDir string, segmentSize int64) (*walManifest, error) {
	if err := os.MkdirAll(walDir, dirMode); err != nil {
		return nil, err
	}
	segName := walSegmentFileName(1)
	segPath := filepath.Join(walDir, segName)
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	m := &walManifest{
		Version:     walManifestVersion,
		SegmentSize: segmentSize,
		ActiveID:    1,
		Segments: []walManifestSegment{{
			ID:      1,
			File:    segName,
			BaseLSN: 0,
		}},
	}
	if err := saveWalManifest(filepath.Join(walDir, walManifestName), m); err != nil {
		return nil, err
	}
	return m, nil
}

func normalizeWalSegmentSize(size int64) int64 {
	if size <= 0 {
		return defaultWalSegSize
	}
	return size
}

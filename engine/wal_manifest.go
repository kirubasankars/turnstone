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
	walDirName         = "wal"
	walManifestName    = "manifest.json"
	walManifestTmpName = "manifest.json.tmp"
	defaultWalSegSize  = 64 << 20 // 64 MiB
	walManifestVersion = 1
)

type walManifestSegment struct {
	ID      uint32 `json:"id"`
	File    string `json:"file"`
	BaseLSN int64  `json:"base_lsn"`
	EndLSN  int64  `json:"end_lsn,omitempty"`
}

type walManifest struct {
	// Version is the on-disk manifest schema (currently 1).
	Version     int                  `json:"version"`
	SegmentSize int64                `json:"segment_size"`
	ScanFloor   int64                `json:"scan_floor"` // reserved; scan floor lives in engine memory today
	ActiveID    uint32               `json:"active_id"`
	Segments    []walManifestSegment `json:"segments"`
}

func walSegmentFileName(id uint32) string {
	return fmt.Sprintf("seg-%06d.wal", id)
}

func loadWalManifest(path string) (*walManifest, error) {
	m, err := readWalManifestFile(path)
	if err == nil {
		return m, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	// Crash after fsync(tmp) but before rename: dest is missing, tmp is complete.
	tmp := walManifestTmpPath(path)
	m, err = readWalManifestFile(tmp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		// Torn tmp and no dest: treat as no manifest so Open can create fresh.
		_ = os.Remove(tmp)
		return nil, os.ErrNotExist
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	return m, nil
}

func readWalManifestFile(path string) (*walManifest, error) {
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

func walManifestTmpPath(path string) string {
	return filepath.Join(filepath.Dir(path), walManifestTmpName)
}

func saveWalManifest(path string, m *walManifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

// writeFileAtomic replaces path with data using the POSIX durable-rename
// sequence: write sibling .tmp, fsync the file (full, not fdatasync — size
// must persist), rename over dest, fsync the parent directory. Readers never
// observe a torn manifest.json.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := walManifestTmpPath(path)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

func createFreshWalManifest(walDir string, segmentSize int64) (*walManifest, error) {
	if err := os.MkdirAll(walDir, dirMode); err != nil {
		return nil, err
	}
	segName := walSegmentFileName(1)
	segPath := filepath.Join(walDir, segName)
	f, err := createAllocatedWALFile(segPath, segmentSize)
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

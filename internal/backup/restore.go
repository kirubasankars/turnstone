// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"turnstone/engine"
)

type RestoreOptions struct {
	BackupDirs []string
	OutHome    string
	File       string
	Verify     bool
}

func RunRestore(ctx context.Context, opts RestoreOptions) (Meta, error) {
	if len(opts.BackupDirs) == 0 {
		return Meta{}, fmt.Errorf("no backup directories provided")
	}
	if opts.File == "" {
		opts.File = DefaultWALFile
	}
	if _, err := os.Stat(opts.OutHome); err == nil {
		return Meta{}, fmt.Errorf("target home %s already exists", opts.OutHome)
	}

	metas := make([]Meta, 0, len(opts.BackupDirs))
	for _, dir := range opts.BackupDirs {
		meta, err := LoadMeta(filepath.Join(dir, DefaultMetaFile))
		if err != nil {
			return Meta{}, fmt.Errorf("load meta from %s: %w", dir, err)
		}
		metas = append(metas, meta)
	}
	if err := ValidateRestoreChain(metas); err != nil {
		return Meta{}, err
	}

	dbName := metas[0].Database
	for _, meta := range metas[1:] {
		if meta.Database != dbName {
			return Meta{}, fmt.Errorf("backup chain mixes databases: %s vs %s", dbName, meta.Database)
		}
	}

	dbDir := filepath.Join(opts.OutHome, "data", dbName)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return Meta{}, fmt.Errorf("create database dir: %w", err)
	}

	db, err := engine.Open(dbDir, engine.Options{})
	if err != nil {
		return Meta{}, fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	for i, dir := range opts.BackupDirs {
		meta := metas[i]
		bkPath, _, err := ResolveWALInputFile(dir, opts.File)
		if err != nil {
			return Meta{}, err
		}
		if err := ingestBackupFile(ctx, bkPath, meta, opts.Verify, db); err != nil {
			return Meta{}, err
		}
	}

	return metas[len(metas)-1], nil
}

func ingestBackupFile(ctx context.Context, path string, meta Meta, verify bool, db *engine.DB) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader = f
	var hasher hash.Hash
	if verify {
		hasher = sha256.New()
		reader = io.TeeReader(f, hasher)
	}

	bufReader := bufio.NewReader(reader)
	peek, _ := bufReader.Peek(2)
	isGzip := len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b
	if isGzip {
		gzR, err := gzip.NewReader(bufReader)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gzR.Close()
		reader = gzR
	} else {
		reader = bufReader
	}

	const batchSize = 4 * 1024 * 1024
	buf := make([]byte, 0, batchSize)
	tmp := make([]byte, 32*1024)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, err := reader.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if len(buf) >= batchSize {
				if _, err := db.ApplyLogRange(buf); err != nil {
					return fmt.Errorf("apply log range: %w", err)
				}
				buf = buf[:0]
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read backup: %w", err)
		}
	}
	if len(buf) > 0 {
		if _, err := db.ApplyLogRange(buf); err != nil {
			return fmt.Errorf("apply final log range: %w", err)
		}
	}

	if verify {
		calculated := hex.EncodeToString(hasher.Sum(nil))
		if calculated != meta.SHA256 {
			return fmt.Errorf("checksum mismatch for %s: expected %s got %s", path, meta.SHA256, calculated)
		}
	}
	return nil
}

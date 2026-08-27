// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"
)

type BackupOptions struct {
	Host         string
	DBName       string
	OutDir       string
	File         string
	Type         string
	FromLSN      uint64
	BaseMetaPath string
	Compress     bool
	WaitIdle     time.Duration
	TLS          *tls.Config
}

func RunBackup(ctx context.Context, opts BackupOptions) (Meta, error) {
	if opts.TLS == nil {
		return Meta{}, fmt.Errorf("TLS config is required")
	}
	if opts.Type != TypeFull && opts.Type != TypeDifferential {
		return Meta{}, fmt.Errorf("invalid backup type %q (want full or differential)", opts.Type)
	}
	if opts.File == "" {
		opts.File = DefaultWALFile
	}

	startLSN := uint64(0)
	parentSHA := ""
	if opts.Type == TypeDifferential {
		if opts.FromLSN > 0 {
			startLSN = opts.FromLSN
		} else if opts.BaseMetaPath != "" {
			baseMeta, err := LoadMeta(opts.BaseMetaPath)
			if err != nil {
				return Meta{}, fmt.Errorf("load base meta: %w", err)
			}
			if baseMeta.Database != opts.DBName {
				return Meta{}, fmt.Errorf("base meta database %q does not match db %q", baseMeta.Database, opts.DBName)
			}
			startLSN = baseMeta.EndLSN
			parentSHA = baseMeta.SHA256
		} else {
			return Meta{}, fmt.Errorf("differential backup requires FromLSN or BaseMetaPath")
		}
	}

	if err := os.MkdirAll(opts.OutDir, 0755); err != nil {
		return Meta{}, fmt.Errorf("create output dir: %w", err)
	}

	outPath := ResolveWALFile(opts.OutDir, opts.File, opts.Compress)
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return Meta{}, fmt.Errorf("create backup file: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	diskWriter := io.MultiWriter(f, hasher)
	var outputWriter io.Writer = diskWriter
	var gzipW *gzip.Writer
	if opts.Compress {
		gzipW = gzip.NewWriter(diskWriter)
		outputWriter = gzipW
	}

	streamRes, err := StreamLogRange(ctx, StreamOptions{
		Host:     opts.Host,
		DBName:   opts.DBName,
		StartLSN: startLSN,
		WaitIdle: opts.WaitIdle,
		TLS:      opts.TLS,
	}, outputWriter)
	if err != nil {
		if ctx.Err() != nil {
			f.Close()
			_ = os.Remove(outPath)
		}
		return Meta{}, err
	}

	if opts.Compress && gzipW != nil {
		if err := gzipW.Close(); err != nil {
			return Meta{}, fmt.Errorf("gzip close: %w", err)
		}
	}

	meta := Meta{
		Timestamp:    time.Now(),
		Database:     opts.DBName,
		Type:         opts.Type,
		BaseLSN:      startLSN,
		EndLSN:       streamRes.EndLSN,
		ParentSHA256: parentSHA,
		Compressed:   opts.Compress,
		SHA256:       hex.EncodeToString(hasher.Sum(nil)),
	}
	if err := SaveMeta(opts.OutDir, meta); err != nil {
		return Meta{}, fmt.Errorf("write backup meta: %w", err)
	}
	return meta, nil
}

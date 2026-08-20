// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"turnstone/engine"
)

func newRestoreCmd() *cobra.Command {
	var inDir string
	var outHome string
	var backupFile string
	var verify bool
	var chain string

	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore WAL backups into a new home directory",
		Long: `Rebuild a database from one or more physical WAL backups. A full backup can be
restored alone. Differential backups must be applied in order after their
parent chain (use --chain for multiple directories).

Each backup's LSN fields describe the global byte range contained in the
artifact.`,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			dirs, err := resolveRestoreChain(inDir, chain)
			if err != nil {
				log.Fatalf("Restore failed: %v", err)
			}

			if err := runRestore(ctx, restoreOptions{
				BackupDirs: dirs,
				OutHome:    outHome,
				File:       backupFile,
				Verify:     verify,
			}); err != nil {
				log.Fatalf("Restore failed: %v", err)
			}
		},
	}

	cmd.Flags().StringVar(&inDir, "in", "backup_data", "Input directory for a single backup")
	cmd.Flags().StringVar(&outHome, "out", "restored_data", "Target home directory to create")
	cmd.Flags().StringVar(&backupFile, "file", defaultBackupFile, "Backup filename inside each backup directory")
	cmd.Flags().BoolVar(&verify, "verify", true, "Verify SHA256 checksum before restoring")
	cmd.Flags().StringVar(&chain, "chain", "", "Comma-separated backup directories to apply in order")

	return cmd
}

type restoreOptions struct {
	BackupDirs []string
	OutHome    string
	File       string
	Verify     bool
}

func resolveRestoreChain(inDir, chain string) ([]string, error) {
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

func runRestore(ctx context.Context, opts restoreOptions) error {
	if len(opts.BackupDirs) == 0 {
		return fmt.Errorf("no backup directories provided")
	}
	if _, err := os.Stat(opts.OutHome); err == nil {
		return fmt.Errorf("target home %s already exists", opts.OutHome)
	}

	metas := make([]BackupMeta, 0, len(opts.BackupDirs))
	for _, dir := range opts.BackupDirs {
		meta, err := loadBackupMeta(filepath.Join(dir, defaultBackupMeta))
		if err != nil {
			return fmt.Errorf("load meta from %s: %w", dir, err)
		}
		metas = append(metas, meta)
	}
	if err := validateRestoreChain(metas); err != nil {
		return err
	}

	dbName := metas[0].Database
	for _, meta := range metas[1:] {
		if meta.Database != dbName {
			return fmt.Errorf("backup chain mixes databases: %s vs %s", dbName, meta.Database)
		}
	}

	fmt.Printf("Restoring database %s into %s\n", dbName, opts.OutHome)
	fmt.Printf("Applying %d backup artifact(s)\n", len(opts.BackupDirs))

	dbDir := filepath.Join(opts.OutHome, "data", dbName)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("create database dir: %w", err)
	}

	db, err := engine.Open(dbDir, engine.Options{})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	for i, dir := range opts.BackupDirs {
		meta := metas[i]
		bkPath, compressed, err := resolveBackupInputFile(dir, opts.File)
		if err != nil {
			return err
		}
		if compressed != meta.Compressed {
			log.Printf("Warning: compression flag in meta (%v) differs from file extension for %s", meta.Compressed, bkPath)
		}

		fmt.Printf("Ingesting %s (%s, LSN [%d, %d))\n", bkPath, meta.Type, meta.BaseLSN, meta.EndLSN)
		if err := ingestBackupFile(ctx, bkPath, meta, opts.Verify, db); err != nil {
			return err
		}
	}

	finalLSN := metas[len(metas)-1].EndLSN
	fmt.Printf("Restore complete. Database at %s (end_lsn=%d)\n", dbDir, finalLSN)
	return nil
}

func validateRestoreChain(metas []BackupMeta) error {
	if len(metas) == 0 {
		return fmt.Errorf("empty backup chain")
	}
	if metas[0].Type != backupTypeFull {
		return fmt.Errorf("first backup must be type full, got %q", metas[0].Type)
	}
	if metas[0].BaseLSN != 0 {
		return fmt.Errorf("full backup must start at LSN 0, got %d", metas[0].BaseLSN)
	}

	prevEnd := metas[0].EndLSN
	prevSHA := metas[0].SHA256
	for i := 1; i < len(metas); i++ {
		meta := metas[i]
		if meta.Type != backupTypeDifferential {
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

func ingestBackupFile(ctx context.Context, path string, meta BackupMeta, verify bool, db *engine.DB) error {
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

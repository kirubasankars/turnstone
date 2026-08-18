// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

func newBackupCmd() *cobra.Command {
	var host string
	var dbName string
	var outDir string
	var backupFile string
	var backupType string
	var fromOpID uint64
	var baseMetaPath string
	var compress bool
	var waitIdle time.Duration

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Stream a physical WAL backup from a primary database",
		Long: `Connect to a running primary and stream raw WAL frames using the replication
protocol. Full backups start at opid 0; differential backups resume from a
previous backup's end_opid (stored in backup.meta).

The opid fields in backup metadata are the global byte LSN used as the
replication cursor.`,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			if err := runBackup(ctx, backupOptions{
				Home:         homeDir,
				Host:         host,
				DBName:       dbName,
				OutDir:         outDir,
				File:         backupFile,
				Type:         backupType,
				FromOpID:     fromOpID,
				BaseMetaPath: baseMetaPath,
				Compress:     compress,
				WaitIdle:     waitIdle,
			}); err != nil {
				if ctx.Err() != nil {
					log.Println("Backup stopped by user.")
					return
				}
				log.Fatalf("Backup failed: %v", err)
			}
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost:6379", "Primary server address")
	cmd.Flags().StringVar(&dbName, "db", "1", "Database name to backup")
	cmd.Flags().StringVar(&outDir, "out", "backup_data", "Output directory for backup artifacts")
	cmd.Flags().StringVar(&backupFile, "file", defaultBackupFile, "Backup filename (appends .gz when --compress is set)")
	cmd.Flags().StringVar(&backupType, "type", backupTypeFull, "Backup type: full or differential")
	cmd.Flags().Uint64Var(&fromOpID, "from-opid", 0, "Start LSN for differential backup (overrides --base-meta)")
	cmd.Flags().StringVar(&baseMetaPath, "base-meta", "", "Previous backup.meta to resume from for differential backup")
	cmd.Flags().BoolVar(&compress, "compress", true, "Enable GZIP compression")
	cmd.Flags().DurationVar(&waitIdle, "wait", 2*time.Second, "Idle time before finishing once caught up")

	return cmd
}

type backupOptions struct {
	Home         string
	Host         string
	DBName       string
	OutDir       string
	File         string
	Type         string
	FromOpID     uint64
	BaseMetaPath string
	Compress     bool
	WaitIdle     time.Duration
}

func runBackup(ctx context.Context, opts backupOptions) error {
	if opts.Type != backupTypeFull && opts.Type != backupTypeDifferential {
		return fmt.Errorf("invalid --type %q (want full or differential)", opts.Type)
	}

	startOpID := uint64(0)
	parentSHA := ""
	if opts.Type == backupTypeDifferential {
		if opts.FromOpID > 0 {
			startOpID = opts.FromOpID
		} else if opts.BaseMetaPath != "" {
			baseMeta, err := loadBackupMeta(opts.BaseMetaPath)
			if err != nil {
				return fmt.Errorf("load base meta: %w", err)
			}
			if baseMeta.Database != opts.DBName {
				return fmt.Errorf("base meta database %q does not match --db %q", baseMeta.Database, opts.DBName)
			}
			startOpID = baseMeta.EndOpID
			parentSHA = baseMeta.SHA256
		} else {
			return fmt.Errorf("differential backup requires --from-opid or --base-meta")
		}
	}

	log.Printf("Starting %s backup from %s [db=%s, base_opid=%d]...", opts.Type, opts.Host, opts.DBName, startOpID)

	if err := os.MkdirAll(opts.OutDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	outPath := resolveBackupFile(opts.OutDir, opts.File, opts.Compress)
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create backup file: %w", err)
	}
	defer f.Close()

	var outputWriter io.Writer
	hasher := sha256.New()
	diskWriter := io.MultiWriter(f, hasher)
	var gzipW *gzip.Writer
	if opts.Compress {
		gzipW = gzip.NewWriter(diskWriter)
		outputWriter = gzipW
	} else {
		outputWriter = diskWriter
	}

	start := time.Now()
	streamRes, err := streamReplLogRange(ctx, opts.Home, replStreamOptions{
		Host:      opts.Host,
		DBName:    opts.DBName,
		StartOpID: startOpID,
		WaitIdle:  opts.WaitIdle,
	}, outputWriter)
	if err != nil {
		if ctx.Err() != nil {
			f.Close()
			_ = os.Remove(outPath)
		}
		return err
	}

	if opts.Compress && gzipW != nil {
		if err := gzipW.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
	}

	meta := BackupMeta{
		Timestamp:    time.Now(),
		Database:     opts.DBName,
		Type:         opts.Type,
		BaseOpID:     startOpID,
		EndOpID:      streamRes.EndOpID,
		ParentSHA256: parentSHA,
		Compressed:   opts.Compress,
		SHA256:       hex.EncodeToString(hasher.Sum(nil)),
	}
	if err := saveBackupMeta(opts.OutDir, meta); err != nil {
		return fmt.Errorf("write backup meta: %w", err)
	}

	log.Printf("Backup successful in %v", time.Since(start))
	log.Printf("Location: %s", outPath)
	log.Printf("Bytes: %d", streamRes.Bytes)
	log.Printf("OpID range: [%d, %d)", meta.BaseOpID, meta.EndOpID)
	log.Printf("Checksum: %s", meta.SHA256)
	return nil
}

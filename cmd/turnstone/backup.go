// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"turnstone/internal/backup"
	"turnstone/internal/tlsutil"
)

func newBackupCmd() *cobra.Command {
	var host string
	var dbName string
	var outDir string
	var backupFile string
	var backupType string
	var fromLSN uint64
	var baseMetaPath string
	var compress bool
	var waitIdle time.Duration

	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Stream a physical WAL backup from a primary database",
		Long: `Connect to a running primary and stream raw WAL frames using the replication
protocol. Full backups start at WAL LSN 0; differential backups resume from a
previous backup's end_lsn (stored in backup.meta).`,
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			tlsConf, err := tlsutil.LoadFromHome(homeDir, tlsutil.RoleAdmin)
			if err != nil {
				log.Fatalf("Backup failed: %v", err)
			}

			meta, err := backup.RunBackup(ctx, backup.BackupOptions{
				Host:         host,
				DBName:       dbName,
				OutDir:       outDir,
				File:         backupFile,
				Type:         backupType,
				FromLSN:      fromLSN,
				BaseMetaPath: baseMetaPath,
				Compress:     compress,
				WaitIdle:     waitIdle,
				TLS:          tlsConf,
			})
			if err != nil {
				if ctx.Err() != nil {
					log.Println("Backup stopped by user.")
					return
				}
				log.Fatalf("Backup failed: %v", err)
			}

			outPath := backup.ResolveWALFile(outDir, backupFile, compress)
			log.Printf("Backup successful")
			log.Printf("Location: %s", outPath)
			log.Printf("LSN range: [%d, %d)", meta.BaseLSN, meta.EndLSN)
			log.Printf("Checksum: %s", meta.SHA256)
		},
	}

	cmd.Flags().StringVar(&host, "host", "localhost:6379", "Primary server address")
	cmd.Flags().StringVar(&dbName, "db", "1", "Database name to backup")
	cmd.Flags().StringVar(&outDir, "out", "backup_data", "Output directory for backup artifacts")
	cmd.Flags().StringVar(&backupFile, "file", backup.DefaultWALFile, "Backup filename (appends .gz when --compress is set)")
	cmd.Flags().StringVar(&backupType, "type", backup.TypeFull, "Backup type: full or differential")
	cmd.Flags().Uint64Var(&fromLSN, "from-lsn", 0, "Start WAL LSN for differential backup (overrides --base-meta)")
	cmd.Flags().StringVar(&baseMetaPath, "base-meta", "", "Previous backup.meta to resume from for differential backup")
	cmd.Flags().BoolVar(&compress, "compress", true, "Enable GZIP compression")
	cmd.Flags().DurationVar(&waitIdle, "wait", 2*time.Second, "Idle time before finishing once caught up")

	return cmd
}

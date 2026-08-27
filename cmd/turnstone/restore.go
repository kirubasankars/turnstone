// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"turnstone/internal/backup"
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

			dirs, err := backup.ResolveRestoreChain(inDir, chain)
			if err != nil {
				log.Fatalf("Restore failed: %v", err)
			}

			meta, err := backup.RunRestore(ctx, backup.RestoreOptions{
				BackupDirs: dirs,
				OutHome:    outHome,
				File:       backupFile,
				Verify:     verify,
			})
			if err != nil {
				log.Fatalf("Restore failed: %v", err)
			}

			dbDir := filepath.Join(outHome, "data", meta.Database)
			fmt.Printf("Restore complete. Database at %s (end_lsn=%d)\n", dbDir, meta.EndLSN)
		},
	}

	cmd.Flags().StringVar(&inDir, "in", "backup_data", "Input directory for a single backup")
	cmd.Flags().StringVar(&outHome, "out", "restored_data", "Target home directory to create")
	cmd.Flags().StringVar(&backupFile, "file", backup.DefaultWALFile, "Backup filename inside each backup directory")
	cmd.Flags().BoolVar(&verify, "verify", true, "Verify SHA256 checksum before restoring")
	cmd.Flags().StringVar(&chain, "chain", "", "Comma-separated backup directories to apply in order")

	return cmd
}

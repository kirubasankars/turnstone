// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"github.com/spf13/cobra"
)

var homeDir string

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "turnstone",
		Short: "TurnstoneDB — persistent transactional key-value store",
		Long: `TurnstoneDB is a persistent, transactional key-value store with optional replication.

Use the subcommands to initialize a data home, run the server, open an interactive
client, or run benchmarks.`,
	}

	root.PersistentFlags().StringVar(&homeDir, "home", "tsdata", "Home directory for data and certs")

	root.AddCommand(
		newInitCmd(),
		newServerCmd(),
		newCLICmd(),
		newBenchCmd(),
	)

	return root
}

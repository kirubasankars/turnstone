// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"turnstone/config"
)

func newInitCmd() *cobra.Command {
	var serverIP string

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize a TurnstoneDB home directory",
		Long: `Create a new home directory with TLS certificates, data directories, and turnstone.json.

Example:
  turnstone init --home tsdata --ip 192.168.1.10,myserver.local`,
		Run: func(cmd *cobra.Command, args []string) {
			if homeDir == "" {
				fmt.Fprintln(os.Stderr, "Error: --home is required")
				os.Exit(1)
			}

			fmt.Printf("Initializing TurnstoneDB home at: %s\n", homeDir)

			var extraHosts []string
			if serverIP != "" {
				extraHosts = strings.Split(serverIP, ",")
				fmt.Printf("Adding subject alternative names: %v\n", extraHosts)
			}

			defaultCfg := config.Config{
				Port:              ":6379",
				Debug:             false,
				MaxConns:          1000,
				NumberOfDatabases: 4,
				TLSCertFile:       "certs/server.crt",
				TLSKeyFile:        "certs/server.key",
				TLSCAFile:         "certs/ca.crt",
				TLSClientCertFile: "certs/client.crt",
				TLSClientKeyFile:  "certs/client.key",
				MetricsAddr:       ":9090",
				LogRetention:      "replication",
			}

			configPath := filepath.Join(homeDir, "turnstone.json")
			if err := config.GenerateConfigArtifacts(homeDir, defaultCfg, configPath, extraHosts...); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to generate artifacts: %v\n", err)
				os.Exit(1)
			}

			fmt.Println("Initialization complete.")
		},
	}

	cmd.Flags().StringVar(&serverIP, "ip", "", "Comma-separated server IPs/hostnames for the TLS certificate SANs")

	return cmd
}

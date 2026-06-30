// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"turnstone/config"
)

var (
	homeDir  = flag.String("home", "tsdata", "Path to home directory to generate")
	serverIP = flag.String("ip", "", "Comma-separated list of Server IPs/Hostnames to include in certificate (e.g. 192.168.1.10,myserver.local)")
)

func main() {
	flag.Parse()

	if *homeDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -home directory is required")
		os.Exit(1)
	}

	fmt.Printf("Initializing TurnstoneDB home at: %s\n", *homeDir)

	var extraHosts []string
	if *serverIP != "" {
		extraHosts = strings.Split(*serverIP, ",")
		fmt.Printf("Adding subject alternative names: %v\n", extraHosts)
	}

	// 1. Define Default Server Config
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

	configPath := filepath.Join(*homeDir, "turnstone.json")

	// 2. Generate Artifacts (Directories, Certs, Server Config)
	if err := config.GenerateConfigArtifacts(*homeDir, defaultCfg, configPath, extraHosts...); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate artifacts: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Initialization complete.")
}

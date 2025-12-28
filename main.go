/*
Package main acts as the entry point for the TurnstoneDB server.
It handles command-line flag parsing, configuration loading, security initialization,
and starts the main server and storage engine.
*/
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	var homeDir string
	flag.StringVar(&homeDir, "home", "", "Home directory for configuration, data, and certificates (Required)")
	genConfig := flag.Bool("generate-config", false, "Generate a sample configuration file and certificates")

	flag.Parse()

	if homeDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -home argument is required")
		flag.Usage()
		os.Exit(1)
	}

	resolvedConfigPath := filepath.Join(homeDir, "config.json")

	// Set reasonable defaults
	defaultCfg := Config{
		Port:                  DefaultPort,
		Debug:                 false,
		MaxConns:              500,
		Fsync:                 true,
		AllowRecoveryTruncate: false,
		Role:                  "leader",
		MinReplicas:           0,
		IndexType:             "leveldb",
		TLSCertFile:           "certs/server.crt",
		TLSKeyFile:            "certs/server.key",
		TLSCAFile:             "certs/ca.crt",
		TLSClientCertFile:     "certs/client.crt",
		TLSClientKeyFile:      "certs/client.key",
		MetricsAddr:           ":9090",
	}

	// Generate artifacts if requested
	if *genConfig {
		generateConfigArtifacts(homeDir, defaultCfg, resolvedConfigPath)
		return
	}

	// Load configuration
	cfg := defaultCfg
	if fileData, err := os.ReadFile(resolvedConfigPath); err == nil {
		if err := json.Unmarshal(fileData, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing config file: %v\n", err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	} else {
		// Config file doesn't exist, create the home dir just in case
		_ = os.MkdirAll(homeDir, 0o755)
	}

	// Normalize paths relative to homeDir
	cfg.TLSCertFile = ResolvePath(homeDir, cfg.TLSCertFile)
	cfg.TLSKeyFile = ResolvePath(homeDir, cfg.TLSKeyFile)
	cfg.TLSCAFile = ResolvePath(homeDir, cfg.TLSCAFile)
	cfg.TLSClientCertFile = ResolvePath(homeDir, cfg.TLSClientCertFile)
	cfg.TLSClientKeyFile = ResolvePath(homeDir, cfg.TLSClientKeyFile)

	// Setup Logging
	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	validateSecurityConfig(cfg, logger)

	// Setup Signal Handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize Storage Engine (using fixed "data" directory)
	dataDir := ResolvePath(homeDir, "data")
	store, err := NewStore(dataDir, logger, cfg.AllowRecoveryTruncate, cfg.MinReplicas, cfg.Fsync, cfg.IndexType)
	if err != nil {
		logger.Error("Failed to init store", "err", err)
		os.Exit(1)
	}

	// Start Replication Client if configured (Follower Mode)
	if cfg.ReplicaOf != "" {
		logger.Info("Starting in FOLLOWER mode", "leader", cfg.ReplicaOf)
		StartReplicationClient(cfg.ReplicaOf, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile, store, logger)
	}

	// Start TCP Server
	srv := NewServer(cfg.Port, cfg.MetricsAddr, store, logger, cfg.MaxConns, false, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
		srv.CloseAll()
	}()

	<-ctx.Done()
}

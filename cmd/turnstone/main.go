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
	"strconv"
	"syscall"
	"time"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/server"
	"turnstone/store"
)

func main() {
	homeDir := flag.String("home", ".", "Home directory for relative paths (certs, data, etc.)")
	genConfig := flag.Bool("generate-config", false, "Generate default config and certificates then exit")
	flag.Parse()

	configPath := config.ResolvePath(*homeDir, "turnstone.json")

	// Default configuration for generation
	defaultCfg := config.Config{
		Port:               ":6379",
		Debug:              true,
		MaxConns:           1000,
		TLSCertFile:        "certs/server.crt",
		TLSKeyFile:         "certs/server.key",
		TLSCAFile:          "certs/ca.crt",
		TLSClientCertFile:  "certs/client.crt",
		TLSClientKeyFile:   "certs/client.key",
		MetricsAddr:        ":9090",
		NumberOfPartitions: 1,
	}

	if *genConfig {
		if err := config.GenerateConfigArtifacts(*homeDir, defaultCfg, configPath); err != nil {
			fmt.Printf("Failed to generate config artifacts: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Load Configuration
	cfgData, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Printf("Error reading config file: %v. Use -generate-config to create one.\n", err)
		os.Exit(1)
	}

	var cfg config.Config
	if err := json.Unmarshal(cfgData, &cfg); err != nil {
		fmt.Printf("Error parsing config file: %v\n", err)
		os.Exit(1)
	}

	// Setup Logger
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if cfg.Debug {
		opts.Level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))

	logger.Info("Starting TurnstoneDB (Persistent)", "port", cfg.Port, "home", *homeDir)

	// Validate Security Config
	if err := config.ValidateSecurityConfig(cfg); err != nil {
		logger.Error("Security config validation failed", "err", err)
		os.Exit(1)
	}

	// Initialize Persistent Stores
	stores := make(map[string]*store.Store)
	dataRoot := config.ResolvePath(*homeDir, "data")
	for i := 0; i < cfg.NumberOfPartitions; i++ {
		name := strconv.Itoa(i)
		partDir := filepath.Join(dataRoot, name)

		s, err := store.NewStore(partDir, logger.With("partition", name))
		if err != nil {
			logger.Error("Failed to initialize store", "partition", name, "err", err)
			os.Exit(1)
		}
		stores[name] = s
	}

	// Initialize Server
	srv, err := server.NewServer(
		cfg.Port,
		stores,
		logger,
		cfg.MaxConns,
		config.ResolvePath(*homeDir, cfg.TLSCertFile),
		config.ResolvePath(*homeDir, cfg.TLSKeyFile),
		config.ResolvePath(*homeDir, cfg.TLSCAFile),
	)
	if err != nil {
		logger.Error("Failed to create server", "err", err)
		os.Exit(1)
	}

	// Start Metrics Server
	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger)
	}

	// Run Server with Graceful Shutdown Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped with error", "err", err)
			os.Exit(1)
		}
	}()

	// Wait for Signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("Shutting down...", "signal", sig)

	// Shutdown Logic
	cancel()       // Signal server to stop accepting connections
	srv.CloseAll() // Close listeners and stores
	time.Sleep(100 * time.Millisecond)
	logger.Info("Shutdown complete")
}

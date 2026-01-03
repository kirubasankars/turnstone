package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/server"
	"turnstone/store"
)

var (
	genConfig = flag.Bool("gen-config", false, "Generate sample configuration and certificates then exit")
	homeDir   = flag.String("home", ".", "Home directory for data and certificates")
)

func main() {
	flag.Parse()

	// Configuration file is always expected in the home directory
	configPath := filepath.Join(*homeDir, "turnstone.json")

	// Handle Config Generation Mode
	if *genConfig {
		defaultCfg := config.Config{
			Port:                  ":6379",
			Debug:                 true,
			MaxConns:              1000,
			Fsync:                 true,
			AllowRecoveryTruncate: false,
			TLSCertFile:           "certs/server.crt",
			TLSKeyFile:            "certs/server.key",
			TLSCAFile:             "certs/ca.crt",
			TLSClientCertFile:     "certs/client.crt",
			TLSClientKeyFile:      "certs/client.key",
			MetricsAddr:           ":9090",
			NumberOfPartitions:    2,
		}
		if err := config.GenerateConfigArtifacts(*homeDir, defaultCfg, configPath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate artifacts: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// 1. Load Configuration
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read config file '%s': %v\n", configPath, err)
		os.Exit(1)
	}

	var cfg config.Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse config json: %v\n", err)
		os.Exit(1)
	}

	// 2. Validate Security
	if err := config.ValidateSecurityConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Config validation failed: %v\n", err)
		os.Exit(1)
	}

	// Resolve Paths
	absCert := config.ResolvePath(*homeDir, cfg.TLSCertFile)
	absKey := config.ResolvePath(*homeDir, cfg.TLSKeyFile)
	absCA := config.ResolvePath(*homeDir, cfg.TLSCAFile)
	absClientCert := config.ResolvePath(*homeDir, cfg.TLSClientCertFile)
	absClientKey := config.ResolvePath(*homeDir, cfg.TLSClientKeyFile)

	// 3. Setup Logger
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	if cfg.Debug {
		opts.Level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	logger.Info("Starting TurnstoneDB",
		"version", "0.1.0",
		"go", runtime.Version(),
		"pid", os.Getpid(),
		"home", *homeDir,
		"config", configPath,
	)

	// 4. Initialize Stores
	stores := make(map[string]*store.Store)
	dataRoot := filepath.Join(*homeDir, "data")

	for i := 0; i < cfg.NumberOfPartitions; i++ {
		partitionName := fmt.Sprintf("%d", i)
		partitionPath := filepath.Join(dataRoot, partitionName)

		// Create store with context-aware logger
		dbLogger := logger.With("component", "store", "partition", partitionName)

		st, err := store.NewStore(partitionPath, dbLogger, cfg.AllowRecoveryTruncate, 0, cfg.Fsync)
		if err != nil {
			logger.Error("Failed to initialize store", "partition", partitionName, "err", err)
			os.Exit(1)
		}
		stores[partitionName] = st
	}

	// 5. Setup Replication TLS (Client Side)
	// The server uses server certs for incoming, but needs client certs for outgoing replication
	replTLS, err := loadReplicationTLS(absCA, absClientCert, absClientKey)
	if err != nil {
		logger.Error("Failed to load replication TLS config", "err", err)
		os.Exit(1)
	}

	// 6. Initialize Replication Manager
	rm := replication.NewReplicationManager(stores, replTLS, logger.With("component", "replication"))
	rm.Start()

	// 7. Initialize Server
	srv, err := server.NewServer(
		cfg.Port,
		stores,
		logger.With("component", "server"),
		cfg.MaxConns,
		protocol.MaxTxDuration,
		absCert,
		absKey,
		absCA,
		rm,
	)
	if err != nil {
		logger.Error("Failed to create server", "err", err)
		os.Exit(1)
	}

	// 8. Start Metrics Server
	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger.With("component", "metrics"))
	}

	// 9. Run Server with Graceful Shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Info("Shutting down...")
		cancel()
		srv.CloseAll()
	}()

	if err := srv.Run(ctx); err != nil {
		logger.Error("Server error", "err", err)
		os.Exit(1)
	}

	logger.Info("TurnstoneDB stopped")
}

func loadReplicationTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca failed: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append ca cert")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load keypair failed: %w", err)
	}

	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

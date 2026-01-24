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
	"strconv"
	"strings"
	"syscall"
	"time"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/replication"
	"turnstone/server"
	"turnstone/store"
)

var (
	initFlag = flag.Bool("init", false, "Generate configuration and certificates, then exit")
	mode     = flag.String("mode", "server", "Operation mode: 'server' or 'cdc'")
	homeDir  = flag.String("home", "tsdata", "Home directory for data and certs")
)

func main() {
	flag.Parse()

	if *initFlag {
		if err := runInit(*homeDir); err != nil {
			fmt.Fprintf(os.Stderr, "Initialization failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	if *mode == "cdc" {
		replication.StartFileConsumer(*homeDir, logger)
		return
	}

	runServer(logger)
}

func runInit(home string) error {
	defaultCfg := config.Config{
		Port:                 ":6379",
		Debug:                false,
		MaxConns:             1000,
		NumberOfDatabases:    4,
		TLSCertFile:          "certs/server.crt",
		TLSKeyFile:           "certs/server.key",
		TLSCAFile:            "certs/ca.crt",
		TLSClientCertFile:    "certs/client.crt",
		TLSClientKeyFile:     "certs/client.key",
		MetricsAddr:          ":9090",
		WALRetention:         "2h",          // Default duration if strategy is time
		WALRetentionStrategy: "replication", // Default strategy
		BlockCacheSize:       "64MB",        // Default block cache
	}
	configPath := filepath.Join(home, "turnstone.json")
	if err := config.GenerateConfigArtifacts(home, defaultCfg, configPath); err != nil {
		return fmt.Errorf("failed to generate artifacts: %w", err)
	}

	// Use the exported struct from replication package to ensure consistency.
	// This defaults to using certs/cdc.crt and certs/cdc.key.
	cdcCfg := replication.DefaultFileConsumerConfig()

	cdcBytes, err := json.MarshalIndent(cdcCfg, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal CDC config: %w", err)
	}

	cdcConfigPath := filepath.Join(home, "turnstone.cdc.json")
	if err := os.WriteFile(cdcConfigPath, cdcBytes, 0o644); err != nil {
		return fmt.Errorf("failed to write CDC config: %w", err)
	}
	fmt.Printf("Sample CDC configuration written to %s\n", cdcConfigPath)

	return nil
}

func parseBytes(s string) (int, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, nil
	}
	var mult int64 = 1
	suffix := ""
	if strings.HasSuffix(s, "GB") {
		mult = 1024 * 1024 * 1024
		suffix = "GB"
	} else if strings.HasSuffix(s, "MB") {
		mult = 1024 * 1024
		suffix = "MB"
	} else if strings.HasSuffix(s, "KB") {
		mult = 1024
		suffix = "KB"
	} else if strings.HasSuffix(s, "B") {
		mult = 1
		suffix = "B"
	}

	numStr := strings.TrimSuffix(s, suffix)
	val, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
	if err != nil {
		return 0, err
	}
	return int(val * mult), nil
}

func runServer(logger *slog.Logger) {
	configPath := filepath.Join(*homeDir, "turnstone.json")
	cfgBytes, err := os.ReadFile(configPath)
	if err != nil {
		logger.Error("Failed to read config file", "path", configPath, "err", err)
		os.Exit(1)
	}
	var cfg config.Config
	if err := json.Unmarshal(cfgBytes, &cfg); err != nil {
		logger.Error("Failed to parse config file", "err", err)
		os.Exit(1)
	}

	if cfg.Debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	// Parse WAL Retention
	walRetention := 2 * time.Hour
	if cfg.WALRetention != "" {
		if d, err := time.ParseDuration(cfg.WALRetention); err == nil {
			walRetention = d
		} else {
			logger.Warn("Invalid wal_retention, using default 2h", "err", err)
		}
	}

	// Parse Block Cache
	blockCacheSize := 64 * 1024 * 1024 // Default 64MB
	if cfg.BlockCacheSize != "" {
		if s, err := parseBytes(cfg.BlockCacheSize); err == nil {
			blockCacheSize = s
		} else {
			logger.Warn("Invalid block_cache_size, using default 64MB", "val", cfg.BlockCacheSize, "err", err)
		}
	}

	// Set default strategy if missing
	if cfg.WALRetentionStrategy == "" {
		cfg.WALRetentionStrategy = "replication"
	}

	certFile := config.ResolvePath(*homeDir, cfg.TLSCertFile)
	keyFile := config.ResolvePath(*homeDir, cfg.TLSKeyFile)
	caFile := config.ResolvePath(*homeDir, cfg.TLSCAFile)

	// Server uses client certs for internal replication client if needed
	clientCertFile := config.ResolvePath(*homeDir, cfg.TLSClientCertFile)
	clientKeyFile := config.ResolvePath(*homeDir, cfg.TLSClientKeyFile)

	stores := make(map[string]*store.Store)

	for i := 0; i <= cfg.NumberOfDatabases; i++ {
		name := strconv.Itoa(i)
		path := filepath.Join(*homeDir, "data", name)
		isSystem := (i == 0)
		st, err := store.NewStore(path, logger.With("db", name), 0, isSystem, cfg.WALRetentionStrategy, cfg.MaxDiskUsagePercent, blockCacheSize)
		if err != nil {
			logger.Error("Failed to initialize store", "db", name, "err", err)
			os.Exit(1)
		}
		// Set retention duration on the underlying DB only if strategy is "time".
		// If strategy is "replication", NewStore initializes it to 0 (disabled),
		// and we should NOT overwrite it with the config value here.
		if cfg.WALRetentionStrategy == "time" {
			st.DB.SetWALRetentionTime(walRetention)
		}
		stores[name] = st
	}

	replTLS, err := loadClientTLS(clientCertFile, clientKeyFile, caFile)
	if err != nil {
		logger.Error("Failed to load replication TLS config", "err", err)
		os.Exit(1)
	}

	rm := replication.NewReplicationManager(cfg.ID, stores, replTLS, logger)

	srv, err := server.NewServer(
		cfg.ID,
		cfg.Port,
		stores,
		logger,
		cfg.MaxConns,
		certFile,
		keyFile,
		caFile,
		rm,
	)
	if err != nil {
		logger.Error("Failed to create server", "err", err)
		os.Exit(1)
	}

	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped unexpectedly", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("Shutting down...")

	srv.CloseAll()
	logger.Info("Shutdown complete")
}

func loadClientTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
	}, nil
}

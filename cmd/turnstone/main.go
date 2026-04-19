package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/replication"
	"turnstone/server"
	"turnstone/store"
)

var (
	mode    = flag.String("mode", "server", "Operation mode: 'server', 'cdc', or 'dev'")
	homeDir = flag.String("home", "tsdata", "Home directory for data and certs")
)

func main() {
	flag.Parse()

	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	// Verify home directory exists before starting
	if _, err := os.Stat(*homeDir); os.IsNotExist(err) {
		logger.Error("Home directory does not exist. Run 'turnstone-genconfig -home <path>' first.", "path", *homeDir)
		os.Exit(1)
	}

	if *mode == "cdc" {
		replication.StartFileConsumer(*homeDir, logger)
		return
	}

	runServer(logger, *mode == "dev")
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

func runServer(logger *slog.Logger, devMode bool) {
	if devMode {
		logger.Info("Starting in DEV mode: Transaction timeouts disabled, all DBs auto-promoted")
	}

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
		
		// DB 0 is now treated as a regular database (minReplicas configurable, not implicitly system)
		// We hardcode minReplicas=0 for initial startup, but it can be promoted later.
		st, err := store.NewStore(path, logger.With("db", name), 0, cfg.WALRetentionStrategy, cfg.MaxDiskUsagePercent, blockCacheSize)
		if err != nil {
			logger.Error("Failed to initialize store", "db", name, "err", err)
			os.Exit(1)
		}

		// DEV MODE: Auto-promote databases so they are writable immediately
		if devMode {
			st.SetMinReplicas(0)
			if err := st.Promote(); err != nil {
				logger.Error("Failed to auto-promote DB in dev mode", "db", name, "err", err)
			} else {
				logger.Info("Dev Mode: Auto-promoted DB to PRIMARY", "db", name)
			}
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
		devMode,
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

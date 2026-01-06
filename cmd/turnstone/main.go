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
	"syscall"

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

	// 1. Init Mode: Generate Certs and Default Config
	if *initFlag {
		if err := runInit(*homeDir); err != nil {
			fmt.Fprintf(os.Stderr, "Initialization failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Setup Logging
	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))

	// 2. CDC Client Mode
	if *mode == "cdc" {
		runCDC(logger)
		return
	}

	// 3. Server Mode
	runServer(logger)
}

// runInit generates the necessary configuration artifacts.
// It is extracted here to be testable by main_test.go.
func runInit(home string) error {
	defaultCfg := config.Config{
		Port:               ":6379",
		Debug:              true,
		MaxConns:           1000,
		NumberOfPartitions: 16,
		TLSCertFile:        "certs/server.crt",
		TLSKeyFile:         "certs/server.key",
		TLSCAFile:          "certs/ca.crt",
		TLSClientCertFile:  "certs/client.crt",
		TLSClientKeyFile:   "certs/client.key",
		MetricsAddr:        ":9090",
	}
	configPath := filepath.Join(home, "turnstone.json")
	if err := config.GenerateConfigArtifacts(home, defaultCfg, configPath); err != nil {
		return fmt.Errorf("failed to generate artifacts: %w", err)
	}

	// Generate default CDC config
	cdcCfg := struct {
		Host        string `json:"host"`
		Partition   string `json:"partition"`
		StartSeq    uint64 `json:"start_seq"`
		StateFile   string `json:"state_file"`
		MetricsAddr string `json:"metrics_addr"`
	}{
		Host:        "localhost:6379",
		Partition:   "1",
		StartSeq:    0,
		StateFile:   "cdc.state",
		MetricsAddr: ":9091",
	}

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

func runServer(logger *slog.Logger) {
	// Load Config
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
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	// Resolve Paths
	certFile := config.ResolvePath(*homeDir, cfg.TLSCertFile)
	keyFile := config.ResolvePath(*homeDir, cfg.TLSKeyFile)
	caFile := config.ResolvePath(*homeDir, cfg.TLSCAFile)
	clientCertFile := config.ResolvePath(*homeDir, cfg.TLSClientCertFile)
	clientKeyFile := config.ResolvePath(*homeDir, cfg.TLSClientKeyFile)

	// Initialize Stores (Partitions)
	stores := make(map[string]*store.Store)

	// Create system partition 0 (Read-Only metadata if needed) and user partitions 1..N
	for i := 0; i <= cfg.NumberOfPartitions; i++ {
		name := strconv.Itoa(i)
		path := filepath.Join(*homeDir, "data", name)

		isSystem := (i == 0)
		// MinReplicas = 0 for now. In a real cluster, this might come from config per partition.
		st, err := store.NewStore(path, logger.With("partition", name), true, 0, isSystem)
		if err != nil {
			logger.Error("Failed to initialize store", "partition", name, "err", err)
			os.Exit(1)
		}
		stores[name] = st
	}

	// Load Client Certs for Replication Manager (Server acts as client to other nodes)
	replTLS, err := loadClientTLS(clientCertFile, clientKeyFile, caFile)
	if err != nil {
		logger.Error("Failed to load replication TLS config", "err", err)
		os.Exit(1)
	}

	// Initialize Replication Manager
	rm := replication.NewReplicationManager(stores, replTLS, logger)

	// Initialize Server
	srv, err := server.NewServer(
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

	// Start Metrics
	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger)
	}

	// Run Server
	// Handle shutdown gracefully
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

	// Cleanup
	srv.CloseAll()
	logger.Info("Shutdown complete")
}

func runCDC(logger *slog.Logger) {
	// 1. Initialize with Defaults
	cfg := replication.CDCConfig{
		Host:        "localhost:6379",
		Home:        *homeDir,
		Partition:   "1",
		StartID:     0,
		StateFile:   "cdc.state",
		TextMode:    true,
		MetricsAddr: ":9091",
		Logger:      logger,
	}

	// 2. Load JSON Config Override if present
	configPath := filepath.Join(*homeDir, "turnstone.cdc.json")
	if data, err := os.ReadFile(configPath); err == nil {
		logger.Info("Loading CDC configuration from file", "path", configPath)

		var jsonCfg struct {
			Host        string `json:"host"`
			Partition   string `json:"partition"`
			StartSeq    uint64 `json:"start_seq"`
			StateFile   string `json:"state_file"`
			MetricsAddr string `json:"metrics_addr"`
		}

		if err := json.Unmarshal(data, &jsonCfg); err != nil {
			logger.Error("Failed to parse CDC config file", "err", err)
			os.Exit(1)
		}

		if jsonCfg.Host != "" {
			cfg.Host = jsonCfg.Host
		}
		if jsonCfg.Partition != "" {
			cfg.Partition = jsonCfg.Partition
		}
		if jsonCfg.StartSeq != 0 {
			cfg.StartID = jsonCfg.StartSeq
		}
		if jsonCfg.StateFile != "" {
			cfg.StateFile = jsonCfg.StateFile
		}
		if jsonCfg.MetricsAddr != "" {
			cfg.MetricsAddr = jsonCfg.MetricsAddr
		}
	} else if !os.IsNotExist(err) {
		logger.Warn("Could not read CDC config file", "path", configPath, "err", err)
	}

	logger.Info("Starting CDC Client", "host", cfg.Host, "partition", cfg.Partition)
	replication.StartCDC(cfg)
}

// loadClientTLS loads the client certificate and key, and the CA certificate.
// It is extracted here to be testable by main_test.go.
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

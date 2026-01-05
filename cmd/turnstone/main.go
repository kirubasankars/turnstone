package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

// --- Configuration Structs (JSON) ---

type ServerConfig struct {
	Port        string `json:"port"`
	HTTPPort    string `json:"http_port"`
	Partitions  int    `json:"partitions"`
	MinReplicas int    `json:"min_replicas"`
}

type ClientCDCConfig struct {
	Host        string `json:"host"`
	Partition   string `json:"partition"`
	StartID     uint64 `json:"start_id"`
	StateFile   string `json:"state_file"`
	TextMode    bool   `json:"text_mode"`
	MetricsAddr string `json:"metrics_addr"`
}

func main() {
	// 1. Define Only the Core Flags
	home := flag.String("home", ".", "Path to home directory containing certs/ and data/")
	genConfig := flag.Bool("generate-config", false, "Generate sample turnstone.json, turnstone.cdc.json, and mTLS certs in home dir")
	cdcMode := flag.Bool("cdc", false, "Run in CDC Client mode instead of Server mode")
	debug := flag.Bool("debug", false, "Enable debug logging")

	flag.Parse()

	// 2. Define Defaults
	defaultServer := ServerConfig{
		Port:        ":6379",
		HTTPPort:    ":9090",
		Partitions:  1,
		MinReplicas: 0,
	}

	defaultCDC := ClientCDCConfig{
		Host:        "localhost:6379",
		Partition:   "0",
		StartID:     0,
		StateFile:   "cdc.state",
		TextMode:    false,
		MetricsAddr: ":9091",
	}

	// 3. Handle Config Generation
	if *genConfig {
		// Use existing config package to generate certs + config
		// We map our simplified struct to the config package struct expected by GenerateConfigArtifacts
		legacyCfg := config.Config{
			Port:               defaultServer.Port,
			Debug:              true,
			MaxConns:           1000,
			TLSCertFile:        "certs/server.crt",
			TLSKeyFile:         "certs/server.key",
			TLSCAFile:          "certs/ca.crt",
			TLSClientCertFile:  "certs/client.crt",
			TLSClientKeyFile:   "certs/client.key",
			MetricsAddr:        defaultServer.HTTPPort,
			NumberOfPartitions: defaultServer.Partitions,
		}

		// Generate certs and legacy config structure if needed by other tools
		if err := config.GenerateConfigArtifacts(*home, legacyCfg, filepath.Join(*home, "config.json.legacy")); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating artifacts: %v\n", err)
			os.Exit(1)
		}

		// Generate new simplified config files
		if err := generateConfigs(*home, defaultServer, defaultCDC); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating simplified configs: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("Configuration and certificates generated successfully.")
		return
	}

	// 4. Setup Logger
	var logger *slog.Logger
	if *debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		if *cdcMode {
			logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		} else {
			logger = slog.New(slog.NewTextHandler(os.Stdout, nil))
		}
	}

	// 5. Dispatch based on mode
	if *cdcMode {
		// Load CDC Config from JSON
		config := defaultCDC
		configPath := filepath.Join(*home, "turnstone.cdc.json")
		if fileExists(configPath) {
			if err := loadJSON(configPath, &config); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load turnstone.cdc.json: %v\n", err)
				os.Exit(1)
			}
			if *debug {
				logger.Debug("Loaded config", "file", configPath, "config", config)
			}
		} else {
			if *debug {
				logger.Debug("Config file not found, using defaults", "file", configPath)
			}
		}

		// Map to replication package config
		cfg := replication.CDCConfig{
			Host:        config.Host,
			Home:        *home,
			Partition:   config.Partition,
			StartID:     config.StartID,
			StateFile:   config.StateFile,
			TextMode:    config.TextMode,
			MetricsAddr: config.MetricsAddr,
			Logger:      logger,
		}
		replication.StartCDC(cfg)

	} else {
		// Load Server Config from JSON
		config := defaultServer
		configPath := filepath.Join(*home, "turnstone.json")
		if fileExists(configPath) {
			if err := loadJSON(configPath, &config); err != nil {
				logger.Error("Failed to load turnstone.json", "err", err)
				os.Exit(1)
			}
			logger.Info("Loaded configuration", "file", configPath)
		} else {
			logger.Info("Config file not found, using defaults", "file", configPath)
		}

		startServer(*home, config.Port, config.HTTPPort, config.Partitions, config.MinReplicas, logger)
	}
}

// --- Helpers ---

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func loadJSON(path string, v interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}

func generateConfigs(home string, srv ServerConfig, cdc ClientCDCConfig) error {
	// Note: config.GenerateConfigArtifacts already creates directories,
	// but we double check or create if that call wasn't made.
	if err := os.MkdirAll(home, 0o755); err != nil {
		return err
	}

	writeJSON := func(name string, v interface{}) error {
		path := filepath.Join(home, name)
		if fileExists(path) {
			fmt.Printf("Skipping %s (already exists)\n", path)
			return nil
		}
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			return err
		}
		fmt.Printf("Generated %s\n", path)
		return nil
	}

	if err := writeJSON("turnstone.json", srv); err != nil {
		return err
	}
	if err := writeJSON("turnstone.cdc.json", cdc); err != nil {
		return err
	}
	return nil
}

// --- Server Logic ---

func startServer(home, port, httpPort string, numPartitions, minReplicas int, logger *slog.Logger) {
	logger.Info("Starting TurnstoneDB", "port", port, "home", home)

	// 1. Certificates
	certFile := filepath.Join(home, "certs", "server.crt")
	keyFile := filepath.Join(home, "certs", "server.key")
	caFile := filepath.Join(home, "certs", "ca.crt")
	clientCert := filepath.Join(home, "certs", "client.crt")
	clientKey := filepath.Join(home, "certs", "client.key")

	// 2. Initialize Stores
	stores := make(map[string]*store.Store)
	for i := 0; i < numPartitions; i++ {
		name := strconv.Itoa(i)
		dir := filepath.Join(home, "data", name)
		st, err := store.NewStore(dir, logger, true, minReplicas, false)
		if err != nil {
			logger.Error("Failed to init store", "partition", name, "err", err)
			os.Exit(1)
		}
		stores[name] = st
	}

	// 3. Setup Replication Manager
	var rm *replication.ReplicationManager
	if _, err := os.Stat(clientCert); err == nil {
		cert, _ := tls.LoadX509KeyPair(clientCert, clientKey)
		ca, _ := os.ReadFile(caFile)
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tlsConf := &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            pool,
			InsecureSkipVerify: true, // Internal replication often uses IP sans names
		}
		rm = replication.NewReplicationManager(stores, tlsConf, logger)
	} else {
		logger.Warn("Client certs not found, outbound replication disabled")
	}

	// 4. Create Server
	srv, err := server.NewServer(port, stores, logger, 1000, certFile, keyFile, caFile, rm)
	if err != nil {
		logger.Error("Failed to create server", "err", err)
		os.Exit(1)
	}

	// 5. Signals
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("Shutting down...")
		cancel()
		srv.CloseAll()
	}()

	// 6. Metrics
	if httpPort != "" {
		metrics.StartMetricsServer(httpPort, stores, srv, logger)
	}

	// 7. Run
	if err := srv.Run(ctx); err != nil {
		logger.Error("Server stopped", "err", err)
	}
}

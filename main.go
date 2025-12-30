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
	"syscall"
	"time"
)

func main() {
	var homeDir string
	flag.StringVar(&homeDir, "home", "", "Home directory (Required)")
	genConfig := flag.Bool("generate-config", false, "Generate configuration artifacts")
	devMode := flag.Bool("dev", false, "Dev Mode (TxTimeout=120s)")
	flag.Parse()

	if homeDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -home argument is required")
		flag.Usage()
		os.Exit(1)
	}

	configPath := filepath.Join(homeDir, "config.json")
	defaultCfg := Config{
		Port: DefaultPort, MaxConns: 500, Fsync: true,
		TLSCertFile: "certs/server.crt", TLSKeyFile: "certs/server.key", TLSCAFile: "certs/ca.crt",
		TLSClientCertFile: "certs/client.crt", TLSClientKeyFile: "certs/client.key", MetricsAddr: ":9090",
		Databases: []DatabaseConfig{
			{Name: "default", Role: "leader"},
		},
	}

	if *genConfig {
		if err := GenerateConfigArtifacts(homeDir, defaultCfg, configPath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate config: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Load Config
	cfg := defaultCfg
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Config parse error: %v\n", err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Config read error: %v\n", err)
		os.Exit(1)
	} else {
		_ = os.MkdirAll(homeDir, 0o755)
	}

	// Resolve absolute paths
	cfg.TLSCertFile = ResolvePath(homeDir, cfg.TLSCertFile)
	cfg.TLSKeyFile = ResolvePath(homeDir, cfg.TLSKeyFile)
	cfg.TLSCAFile = ResolvePath(homeDir, cfg.TLSCAFile)
	cfg.TLSClientCertFile = ResolvePath(homeDir, cfg.TLSClientCertFile)
	cfg.TLSClientKeyFile = ResolvePath(homeDir, cfg.TLSClientKeyFile)

	// Logging
	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	txDuration := MaxTxDuration
	if *devMode {
		logger.Info("DEV MODE Enabled")
		txDuration = 120 * time.Second
	}

	if err := ValidateSecurityConfig(cfg); err != nil {
		logger.Error("Config invalid", "err", err)
		os.Exit(1)
	}

	// Initialize Stores
	// Ensure "0" (System DB) always exists
	hasSystemDB := false
	for _, dbCfg := range cfg.Databases {
		if dbCfg.Name == "0" {
			hasSystemDB = true
			break
		}
	}
	if !hasSystemDB {
		cfg.Databases = append(cfg.Databases, DatabaseConfig{Name: "0", Role: "leader"})
	}

	stores := make(map[string]*Store)
	for _, dbCfg := range cfg.Databases {
		dbPath := filepath.Join(homeDir, "data", dbCfg.Name)
		store, err := NewStore(dbPath, logger, cfg.AllowRecoveryTruncate, dbCfg.MinReplicas, cfg.Fsync)
		if err != nil {
			logger.Error("Failed to init store", "db", dbCfg.Name, "err", err)
			os.Exit(1)
		}
		stores[dbCfg.Name] = store
	}

	// Initialize Replication Manager (Follower Mode)
	// We init RM if client certs are valid, regardless of initial config, to allow dynamic commands
	var replManager *ReplicationManager
	clientCert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
	if err == nil {
		caCert, _ := os.ReadFile(cfg.TLSCAFile)
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		tlsConf := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool}

		replManager = NewReplicationManager(stores, tlsConf, logger)
		replManager.Start(cfg.Databases)
	} else {
		logger.Warn("Client certs not found, replication client disabled", "err", err)
	}

	srv, err := NewServer(cfg.Port, cfg.MetricsAddr, stores, logger, cfg.MaxConns, txDuration, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile, replManager)
	if err != nil {
		logger.Error("Server init failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
	}()

	<-ctx.Done()
	srv.CloseAll()
}

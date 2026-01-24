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
	"time"

	"turnstone/config"
	"turnstone/metrics"
	"turnstone/protocol"
	"turnstone/replication"
	"turnstone/server"
	"turnstone/store"
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
	defaultCfg := config.Config{
		Port: protocol.DefaultPort, MaxConns: 500, Fsync: true,
		TLSCertFile: "certs/server.crt", TLSKeyFile: "certs/server.key", TLSCAFile: "certs/ca.crt",
		TLSClientCertFile: "certs/client.crt", TLSClientKeyFile: "certs/client.key", MetricsAddr: ":9090",
		NumberOfDatabases: 16,
	}

	if *genConfig {
		if err := config.GenerateConfigArtifacts(homeDir, defaultCfg, configPath); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to generate config: %v\n", err)
			os.Exit(1)
		}
		return
	}

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

	cfg.TLSCertFile = config.ResolvePath(homeDir, cfg.TLSCertFile)
	cfg.TLSKeyFile = config.ResolvePath(homeDir, cfg.TLSKeyFile)
	cfg.TLSCAFile = config.ResolvePath(homeDir, cfg.TLSCAFile)
	cfg.TLSClientCertFile = config.ResolvePath(homeDir, cfg.TLSClientCertFile)
	cfg.TLSClientKeyFile = config.ResolvePath(homeDir, cfg.TLSClientKeyFile)

	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}

	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ensure home directory exists: %v\n", err)
		os.Exit(1)
	}

	logPath := filepath.Join(homeDir, "turnstone.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open log file %s: %v\n", logPath, err)
		os.Exit(1)
	}
	defer logFile.Close()

	multiWriter := io.MultiWriter(os.Stdout, logFile)
	logger := slog.New(slog.NewTextHandler(multiWriter, &slog.HandlerOptions{Level: lvl}))

	txDuration := protocol.MaxTxDuration
	if *devMode {
		logger.Info("DEV MODE Enabled")
		txDuration = 120 * time.Second
	}

	if err := config.ValidateSecurityConfig(cfg); err != nil {
		logger.Error("Config invalid", "err", err)
		os.Exit(1)
	}

	stores := make(map[string]*store.Store)
	for i := 0; i < cfg.NumberOfDatabases; i++ {
		dbName := strconv.Itoa(i)
		dbPath := filepath.Join(homeDir, "data", dbName)
		st, err := store.NewStore(dbPath, logger, cfg.AllowRecoveryTruncate, 0, cfg.Fsync)
		if err != nil {
			logger.Error("Failed to init store", "db", dbName, "err", err)
			os.Exit(1)
		}
		stores[dbName] = st
	}

	var replManager *replication.ReplicationManager
	clientCert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
	if err == nil {
		caCert, _ := os.ReadFile(cfg.TLSCAFile)
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		tlsConf := &tls.Config{Certificates: []tls.Certificate{clientCert}, RootCAs: pool}

		replManager = replication.NewReplicationManager(stores, tlsConf, logger)
		replManager.Start()
	} else {
		logger.Warn("Client certs not found, replication client disabled", "err", err)
	}

	srv, err := server.NewServer(cfg.Port, stores, logger, cfg.MaxConns, txDuration, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile, replManager)
	if err != nil {
		logger.Error("Server init failed", "err", err)
		os.Exit(1)
	}

	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger)
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

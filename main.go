package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 1. Define Flags
	var homeDir string
	flag.StringVar(&homeDir, "home", "", "Home directory for configuration, data, and certificates (Required)")
	genConfig := flag.Bool("generate-config", false, "Generate a sample configuration file and certificates")

	// New flag for CDC client DB selection
	flagDB := flag.Int("db", 0, "Database index for CDC operations (default 0)")

	flag.Parse()

	if homeDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -home argument is required")
		flag.Usage()
		os.Exit(1)
	}

	resolvedConfigPath := filepath.Join(homeDir, "config.json")

	defaultCfg := Config{
		Port:              DefaultPort,
		Dir:               DefaultDataDir,
		Debug:             false,
		MaxConns:          500,
		MaxSyncs:          10,
		Fsync:             true,
		Role:              "leader",
		LeaderAddr:        "",
		CDCState:          "cdc.state",
		CDCBase64:         false,
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		TLSClientCertFile: "certs/client.crt",
		TLSClientKeyFile:  "certs/client.key",
		SentinelAddr:      "",
		MetricsAddr:       "",
	}

	if *genConfig {
		generateConfigArtifacts(homeDir, defaultCfg, resolvedConfigPath)
		return
	}

	// 2. Load Config from JSON
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
		// Ensure home exists before starting
		_ = os.MkdirAll(homeDir, 0o755)
	}

	// 3. Resolve Paths
	cfg.Dir = ResolvePath(homeDir, cfg.Dir)
	cfg.CDCState = ResolvePath(homeDir, cfg.CDCState)
	cfg.TLSCertFile = ResolvePath(homeDir, cfg.TLSCertFile)
	cfg.TLSKeyFile = ResolvePath(homeDir, cfg.TLSKeyFile)
	cfg.TLSCAFile = ResolvePath(homeDir, cfg.TLSCAFile)
	cfg.TLSClientCertFile = ResolvePath(homeDir, cfg.TLSClientCertFile)
	cfg.TLSClientKeyFile = ResolvePath(homeDir, cfg.TLSClientKeyFile)

	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	validateSecurityConfig(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Role {
	case "leader":
		runServer(ctx, cfg, logger, false)
		<-ctx.Done()

	case "follower":
		if cfg.LeaderAddr == "" {
			logger.Error("Follower mode requires leader_addr in config")
			os.Exit(1)
		}

		// Initialize stores eagerly
		stores := runServer(ctx, cfg, logger, true)

		// Start CDC Client for EACH database
		for i := 0; i < MaxDatabases; i++ {
			store := stores[i]
			dbIdx := i

			startGen, startOff, err := store.GetReplicationState()
			if err != nil {
				logger.Warn("Failed to load replication state", "db", dbIdx, "err", err)
			}

			logger.Info("Starting Replication", "db", dbIdx, "leader", cfg.LeaderAddr, "gen", startGen, "off", startOff)
			handler := NewReplicaHandler(store)

			client := NewCDCClient(cfg.LeaderAddr, handler, dbIdx, startGen, startOff, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)

			go func() {
				if err := client.Run(ctx); err != nil && err != context.Canceled {
					logger.Error("Replication failed", "db", dbIdx, "err", err)
					stop() // Stop app if replication fails critically
				}
			}()
		}

		<-ctx.Done()

	case "cdc-stdout":
		runCDCStdout(ctx, cfg, *flagDB, logger)

	case "sentinel":
		runSentinelMode(cfg)

	default:
		fmt.Println("Unknown role.")
		os.Exit(1)
	}
}

func runCDCStdout(ctx context.Context, cfg Config, dbIdx int, logger *slog.Logger) {
	if cfg.LeaderAddr == "" {
		fmt.Println("Usage: config role=cdc-stdout requires leader_addr")
		os.Exit(1)
	}

	if dbIdx < 0 || dbIdx >= MaxDatabases {
		fmt.Fprintf(os.Stderr, "Error: Invalid database index %d\n", dbIdx)
		os.Exit(1)
	}

	var startGen uint64
	var startOff int64

	if cfg.CDCState != "" {
		loadedGen, loadedOff, err := loadState(cfg.CDCState)
		if err == nil {
			startGen = loadedGen
			startOff = loadedOff
		}
	}

	if cfg.MetricsAddr != "" {
		StartMetricsServer(cfg.MetricsAddr, nil, logger)
	}

	logger.Info("Starting CDC Stdout", "db", dbIdx, "leader", cfg.LeaderAddr)

	handler := NewPipeHandler(cfg.CDCBase64, cfg.CDCState)
	client := NewCDCClient(cfg.LeaderAddr, handler, dbIdx, startGen, startOff, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)

	if err := client.Run(ctx); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "CDC Error: %v\n", err)
		os.Exit(1)
	}
}

func runSentinelMode(cfg Config) {
	target := cfg.SentinelAddr
	if target == "" {
		host, port, err := net.SplitHostPort(cfg.Port)
		if err != nil {
			if strings.HasPrefix(cfg.Port, ":") {
				target = "127.0.0.1" + cfg.Port
			} else {
				target = cfg.Port
			}
		} else {
			if host == "" {
				host = "127.0.0.1"
			}
			target = net.JoinHostPort(host, port)
		}
	}

	err := runSentinelCheck(target, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Sentinel Health Check Failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// runServer starts the Server with eagerly initialized stores.
func runServer(ctx context.Context, cfg Config, logger *slog.Logger, readOnly bool) []*Store {
	var stores []*Store

	// Eager Initialization Loop
	for i := 0; i < MaxDatabases; i++ {
		dbPath := filepath.Join(cfg.Dir, strconv.Itoa(i))
		store, err := NewStore(dbPath, logger, false, false, cfg.Fsync, 500*time.Millisecond)
		if err != nil {
			logger.Error("Failed to init store", "db", i, "err", err)
			os.Exit(1)
		}
		stores = append(stores, store)
	}

	srv := NewServer(cfg.Port, cfg.MetricsAddr, stores, logger, cfg.MaxConns, cfg.MaxSyncs, readOnly, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
		// Ensure all stores are closed on shutdown
		srv.CloseAll()
	}()

	return stores
}

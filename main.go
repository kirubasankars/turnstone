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
	"strings"
	"syscall"
	"time"
)

func main() {
	// 1. Define Flags
	var homeDir string
	flag.StringVar(&homeDir, "home", "", "Home directory for configuration, data, and certificates (Required)")
	genConfig := flag.Bool("generate-config", false, "Generate a sample configuration file and certificates")

	// Flag overrides (optional pointers)
	flagPort := flag.String("port", "", "Override port")
	flagRole := flag.String("role", "", "Override role")

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

	// 3. Apply Flag Overrides
	if *flagPort != "" {
		cfg.Port = *flagPort
	}
	if *flagRole != "" {
		cfg.Role = *flagRole
	}

	// 4. Resolve Paths
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
		// Initialize Server with Read-Only Mode
		// Note: Follower also does lazy loading now.
		// However, for CDC replication to work, the ReplicaHandler needs the stores.
		// ReplicaHandler logic needs to be updated to use server's getStore?
		// Actually, standard follower replication logic below iterates MaxDatabases.
		// To replicate, we need the stores.

		// For the sake of the exercise (Lazy Load), we will initialize stores lazily
		// BUT the CDC loop below iterates 0..15. This will force initialization of all DBs at startup
		// if we are in Follower mode. This is acceptable for Follower mode as it needs to replicate everything.
		// Lazy load applies primarily to Leader mode (client-driven).

		srv := runServer(ctx, cfg, logger, true)

		// Start CDC Client for EACH database
		// This loop inherently disables lazy loading for Follower, which is correct (Full Replication).
		for i := 0; i < MaxDatabases; i++ {
			// We access the store via the Server to ensure it's initialized
			store, err := srv.getStore(i)
			if err != nil {
				logger.Error("Failed to init store for replication", "db", i, "err", err)
				continue
			}

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
		runCDCStdout(ctx, cfg, logger)

	case "sentinel":
		runSentinelMode(cfg)

	default:
		fmt.Println("Unknown role.")
		os.Exit(1)
	}
}

func runCDCStdout(ctx context.Context, cfg Config, logger *slog.Logger) {
	if cfg.LeaderAddr == "" {
		fmt.Println("Usage: config role=cdc-stdout requires leader_addr")
		os.Exit(1)
	}

	dbIdx := 0
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

// runServer starts the Server without pre-initializing stores (Lazy Loading).
func runServer(ctx context.Context, cfg Config, logger *slog.Logger, readOnly bool) *Server {
	storeCfg := StoreConfig{
		Dir:           cfg.Dir,
		Logger:        logger,
		Fsync:         cfg.Fsync,
		FsyncInterval: 500 * time.Millisecond,
	}

	srv := NewServer(cfg.Port, cfg.MetricsAddr, storeCfg, logger, cfg.MaxConns, cfg.MaxSyncs, readOnly, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
		// Ensure all lazy-loaded stores are closed on shutdown
		srv.CloseAll()
	}()

	return srv
}

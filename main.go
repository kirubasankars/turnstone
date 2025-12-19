package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Config holds the application configuration
type Config struct {
	Port              string `json:"port"`
	Dir               string `json:"dir"`
	Debug             bool   `json:"debug"`
	MaxConns          int    `json:"max_conns"`
	MaxSyncs          int    `json:"max_syncs"`
	Fsync             bool   `json:"fsync"`
	RequirePassBcrypt string `json:"require_pass_bcrypt"` // Store Bcrypt hash of the password
	Role              string `json:"role"`
	LeaderAddr        string `json:"leader_addr"`
	LeaderAuth        string `json:"leader_auth"` // Plaintext password to send to leader (if follower)
	CDCState          string `json:"cdc_state"`
	CDCBase64         bool   `json:"cdc_base64"`
}

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	genConfig := flag.Bool("generate-config", false, "Generate a sample configuration file")
	genHash := flag.String("generate-hash", "", "Generate bcrypt hash for a password and exit")
	flag.Parse()

	// Handle Hash Generation helper
	if *genHash != "" {
		hash, err := bcrypt.GenerateFromPassword([]byte(*genHash), bcrypt.DefaultCost)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating hash: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(hash))
		return
	}

	// Default Configuration
	defaultCfg := Config{
		Port:              DefaultPort,
		Dir:               DefaultDataDir,
		Debug:             false,
		MaxConns:          500,
		MaxSyncs:          10,
		Fsync:             true,
		RequirePassBcrypt: "",
		Role:              "leader",
		CDCState:          "cdc.state",
		CDCBase64:         false,
	}

	// Handle Config Generation
	if *genConfig {
		data, err := json.MarshalIndent(defaultCfg, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating config: %v\n", err)
			os.Exit(1)
		}

		if err := os.WriteFile(*configPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing config file: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Sample configuration written to %s\n", *configPath)
		return
	}

	cfg := defaultCfg

	// Load Config File
	if fileData, err := os.ReadFile(*configPath); err == nil {
		if err := json.Unmarshal(fileData, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing config file: %v\n", err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	} else {
		// If specific config path given but not found, error out.
		// If default "config.json" missing, proceed with defaults.
		if *configPath != "config.json" {
			fmt.Fprintf(os.Stderr, "Config file not found: %s\n", *configPath)
			os.Exit(1)
		}
	}

	// Logger Setup
	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Role {
	case "leader":
		// Start server and block until context is cancelled
		runServer(ctx, cfg.Port, cfg.Dir, logger, cfg.MaxConns, cfg.MaxSyncs, cfg.RequirePassBcrypt, cfg.Fsync, false)
		<-ctx.Done()

	case "follower":
		if cfg.LeaderAddr == "" {
			logger.Error("Follower mode requires leader_addr in config")
			os.Exit(1)
		}
		// 1. Start Server in Read-Only Mode
		store := runServer(ctx, cfg.Port, cfg.Dir, logger, cfg.MaxConns, cfg.MaxSyncs, cfg.RequirePassBcrypt, cfg.Fsync, true)

		// 2. Load Resume State from BoltDB
		startGen, startOff, err := store.GetReplicationState()
		if err != nil {
			logger.Warn("Failed to load replication state, starting from 0", "err", err)
		}

		// 3. Start CDC Client (Background)
		logger.Info("Starting Replication", "leader", cfg.LeaderAddr, "gen", startGen, "off", startOff)
		handler := NewReplicaHandler(store)
		client := NewCDCClient(cfg.LeaderAddr, cfg.LeaderAuth, handler, startGen, startOff)

		go func() {
			if err := client.Run(ctx); err != nil && err != context.Canceled {
				logger.Error("Replication failed", "err", err)
				stop() // Kill server if replication dies hard
			}
		}()

		<-ctx.Done()

	case "cdc-stdout":
		if cfg.LeaderAddr == "" {
			fmt.Println("Usage: config role=cdc-stdout requires leader_addr")
			os.Exit(1)
		}
		// Resume Logic
		var startGen uint64
		var startOff int64
		if cfg.CDCState != "" {
			loadedGen, loadedOff, err := loadState(cfg.CDCState)
			if err == nil {
				startGen = loadedGen
				startOff = loadedOff
			}
		}

		handler := NewPipeHandler(cfg.CDCBase64, cfg.CDCState)
		client := NewCDCClient(cfg.LeaderAddr, cfg.LeaderAuth, handler, startGen, startOff)

		if err := client.Run(ctx); err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "CDC Error: %v\n", err)
			os.Exit(1)
		}

	default:
		fmt.Println("Unknown role in config. Use leader, follower, or cdc-stdout")
		os.Exit(1)
	}
}

// runServer starts the Store and Server, returning the Store instance.
// It launches the server in a goroutine to allow the main thread to handle other logic (like CDC).
// Note: This function assumes the caller will handle waiting for ctx.Done().
func runServer(ctx context.Context, port, dir string, logger *slog.Logger, maxConns, maxSyncs int, pass string, fsync, readOnly bool) *Store {
	store, err := NewStore(dir, logger, false, false, fsync, 500*time.Millisecond)
	if err != nil {
		logger.Error("Failed to init store", "err", err)
		os.Exit(1)
	}

	srv := NewServer(port, store, logger, maxConns, maxSyncs, pass, readOnly)

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
		// Ensure store is closed when server stops
		if err := store.Close(); err != nil {
			logger.Error("Store close error", "err", err)
		}
	}()

	return store
}

func loadState(path string) (uint64, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var s CheckpointState
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, 0, err
	}
	return s.Generation, s.Offset, nil
}

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"turnstone/client"
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
		runCDC(logger)
		return
	}

	runServer(logger)
}

func runInit(home string) error {
	defaultCfg := config.Config{
		Port:                 ":6379",
		Debug:                true,
		MaxConns:             1000,
		NumberOfDatabases:    16,
		TLSCertFile:          "certs/server.crt",
		TLSKeyFile:           "certs/server.key",
		TLSCAFile:            "certs/ca.crt",
		TLSClientCertFile:    "certs/client.crt",
		TLSClientKeyFile:     "certs/client.key",
		MetricsAddr:          ":9090",
		WALRetention:         "2h",          // Default duration if strategy is time
		WALRetentionStrategy: "replication", // Default strategy
	}
	// ID generation logic moved to config.GenerateConfigArtifacts
	configPath := filepath.Join(home, "turnstone.json")
	if err := config.GenerateConfigArtifacts(home, defaultCfg, configPath); err != nil {
		return fmt.Errorf("failed to generate artifacts: %w", err)
	}

	cdcCfg := struct {
		ID             string `json:"id"` // Unique CDC Client ID
		Host           string `json:"host"`
		Database       string `json:"database"`
		OutputDir      string `json:"output_dir"`
		StateFile      string `json:"state_file"`
		MaxFileSize    int    `json:"max_file_size_mb"`
		RotateInterval string `json:"rotate_interval"`
		FlushInterval  string `json:"flush_interval"` // New: Idle timeout
		ValueFormat    string `json:"value_format"`
		MetricsAddr    string `json:"metrics_addr"`
		TLSCertFile    string `json:"tls_cert_file"`
		TLSKeyFile     string `json:"tls_key_file"`
		TLSCAFile      string `json:"tls_ca_file"`
	}{
		ID:             "cdc-worker-1",
		Host:           "localhost:6379",
		Database:       "1",
		OutputDir:      "cdc_logs",
		StateFile:      "cdc.state",
		MaxFileSize:    100,
		RotateInterval: "1m",
		FlushInterval:  "30s", // Default 30s idle flush
		ValueFormat:    "text",
		MetricsAddr:    ":9091",
		TLSCertFile:    "certs/cdc.crt",
		TLSKeyFile:     "certs/cdc.key",
		TLSCAFile:      "certs/ca.crt",
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

	// Set default strategy if missing
	if cfg.WALRetentionStrategy == "" {
		cfg.WALRetentionStrategy = "replication"
	}

	certFile := config.ResolvePath(*homeDir, cfg.TLSCertFile)
	keyFile := config.ResolvePath(*homeDir, cfg.TLSKeyFile)
	caFile := config.ResolvePath(*homeDir, cfg.TLSCAFile)
	clientCertFile := config.ResolvePath(*homeDir, cfg.TLSClientCertFile)
	clientKeyFile := config.ResolvePath(*homeDir, cfg.TLSClientKeyFile)

	stores := make(map[string]*store.Store)

	for i := 0; i <= cfg.NumberOfDatabases; i++ {
		name := strconv.Itoa(i)
		path := filepath.Join(*homeDir, "data", name)
		isSystem := (i == 0)
		st, err := store.NewStore(path, logger.With("db", name), 0, isSystem, cfg.WALRetentionStrategy, cfg.MaxDiskUsagePercent)
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

// CDCLogEntry defines the JSON structure written to rotating files.
type CDCLogEntry struct {
	Key      string `json:"k"`
	Value    any    `json:"v,omitempty"`
	IsDelete bool   `json:"del,omitempty"`
	LogSeq   uint64 `json:"seq"`
	TxID     uint64 `json:"tx"`
	Time     int64  `json:"ts"`
}

type CDCState struct {
	LastSeq uint64    `json:"last_seq"`
	Updated time.Time `json:"updated"`
}

func runCDC(logger *slog.Logger) {
	cfg := struct {
		ID             string `json:"id"`
		Host           string `json:"host"`
		Database       string `json:"database"`
		OutputDir      string `json:"output_dir"`
		StateFile      string `json:"state_file"`
		MaxFileSize    int    `json:"max_file_size_mb"`
		RotateInterval string `json:"rotate_interval"`
		FlushInterval  string `json:"flush_interval"` // New: Idle timeout
		ValueFormat    string `json:"value_format"`
		MetricsAddr    string `json:"metrics_addr"`
		TLSCertFile    string `json:"tls_cert_file"`
		TLSKeyFile     string `json:"tls_key_file"`
		TLSCAFile      string `json:"tls_ca_file"`
	}{
		ID:             "cdc-default",
		Host:           "localhost:6379",
		Database:       "1",
		OutputDir:      "cdc_logs",
		StateFile:      "cdc.state",
		MaxFileSize:    100,
		RotateInterval: "1m",
		FlushInterval:  "30s",
		ValueFormat:    "text",
		MetricsAddr:    ":9091",
		TLSCertFile:    "certs/cdc.crt",
		TLSKeyFile:     "certs/cdc.key",
		TLSCAFile:      "certs/ca.crt",
	}

	configPath := filepath.Join(*homeDir, "turnstone.cdc.json")
	if data, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			logger.Error("Failed to parse CDC config file", "err", err)
			os.Exit(1)
		}
		logger.Info("Loaded CDC config", "path", configPath)
	}

	rotateDuration, err := time.ParseDuration(cfg.RotateInterval)
	if err != nil {
		logger.Warn("Invalid rotate_interval, defaulting to 1m", "val", cfg.RotateInterval)
		rotateDuration = 1 * time.Minute
	}

	flushDuration, err := time.ParseDuration(cfg.FlushInterval)
	if err != nil {
		logger.Warn("Invalid flush_interval, defaulting to 30s", "val", cfg.FlushInterval)
		flushDuration = 30 * time.Second
	}

	caPath := config.ResolvePath(*homeDir, cfg.TLSCAFile)
	certPath := config.ResolvePath(*homeDir, cfg.TLSCertFile)
	keyPath := config.ResolvePath(*homeDir, cfg.TLSKeyFile)
	outputDir := config.ResolvePath(*homeDir, cfg.OutputDir)
	statePath := config.ResolvePath(*homeDir, cfg.StateFile)

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		logger.Error("Failed to create output directory", "dir", outputDir, "err", err)
		os.Exit(1)
	}

	// Recovery: Cleanup .part files
	entries, err := os.ReadDir(outputDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".part") {
				path := filepath.Join(outputDir, e.Name())
				if err := os.Remove(path); err != nil {
					logger.Error("Failed to remove orphaned part file", "file", e.Name(), "err", err)
				} else {
					logger.Warn("Removed dirty part file (will re-fetch data)", "file", e.Name())
				}
			}
		}
	}

	startSeq := uint64(0)
	if sData, err := os.ReadFile(statePath); err == nil {
		var state CDCState
		if err := json.Unmarshal(sData, &state); err == nil {
			startSeq = state.LastSeq
			logger.Info("Resuming from checkpoint", "seq", startSeq, "time", state.Updated)
		}
	} else {
		logger.Info("No checkpoint found, starting from 0")
	}

	lastSeenSeq := startSeq
	var currentFile *os.File
	var currentBytes int64
	lastRotationTime := time.Now()
	lastWriteTime := time.Now() // Track idle time
	var fileMutex sync.Mutex

	saveState := func(seq uint64) {
		newState := CDCState{LastSeq: seq, Updated: time.Now()}
		data, _ := json.Marshal(newState)
		tmpPath := statePath + ".tmp"
		if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
			logger.Error("Failed to write state file", "err", err)
			return
		}
		os.Rename(tmpPath, statePath)
	}

	rotateFile := func(seq uint64) error {
		fileMutex.Lock()
		defer fileMutex.Unlock()

		if currentFile != nil {
			if err := currentFile.Sync(); err != nil {
				logger.Error("Failed to sync log file", "err", err)
			}
			currentFile.Close()

			oldPath := currentFile.Name()
			if strings.HasSuffix(oldPath, ".part") {
				newPath := strings.TrimSuffix(oldPath, ".part")
				if err := os.Rename(oldPath, newPath); err != nil {
					logger.Error("Failed to finalize log file", "old", oldPath, "err", err)
					return err
				} else {
					logger.Info("Finalized log file", "path", newPath)
					saveState(lastSeenSeq)
				}
			}
		}

		filename := fmt.Sprintf("cdc-%s-%d-%d.jsonl.part", cfg.Database, time.Now().UnixNano(), seq)
		path := filepath.Join(outputDir, filename)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		currentFile = f
		currentBytes = 0
		lastRotationTime = time.Now()
		lastWriteTime = time.Now() // Reset idle timer on new file
		logger.Info("Rotated log file", "file", filename)
		return nil
	}

	if err := rotateFile(lastSeenSeq); err != nil {
		logger.Error("Failed to create initial log file", "err", err)
		os.Exit(1)
	}

	// Double-signal handling for forced exit
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	// No defer cancel() here, we call it manually

	go func() {
		// First signal: Graceful
		sig := <-sigCh
		logger.Info("Received signal, shutting down...", "signal", sig)
		cancel()

		// Second signal: Forced
		sig = <-sigCh
		logger.Info("Received second signal, forcing exit", "signal", sig)
		os.Exit(1)
	}()

	// Ticker for Time-based and Idle-based rotation
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fileMutex.Lock()

				// Critical Sync
				if currentFile != nil {
					currentFile.Sync()
				}

				currentSeq := lastSeenSeq

				// Criteria 1: Absolute File Age (Max age)
				needsAgeRotation := time.Since(lastRotationTime) > rotateDuration

				// Criteria 2: Idle Time (Flush if silence)
				needsIdleRotation := time.Since(lastWriteTime) > flushDuration

				hasBytes := currentBytes > 0
				fileMutex.Unlock()

				// Only rotate if there is data to finalize
				if hasBytes && (needsAgeRotation || needsIdleRotation) {
					reason := "age"
					if needsIdleRotation {
						reason = "idle"
					}

					// Log the reason if debug
					if needsIdleRotation {
						logger.Info("Triggering idle flush", "idle_ms", time.Since(lastWriteTime).Milliseconds())
					}

					if err := rotateFile(currentSeq); err != nil {
						logger.Error("Rotation failed", "reason", reason, "err", err)
					}
				}
			}
		}
	}()

	logger.Info("Starting subscription loop", "database", cfg.Database, "format", cfg.ValueFormat, "client_id", cfg.ID)

	// Main Loop
Loop:
	for {
		if ctx.Err() != nil {
			break Loop
		}

		var cli *client.Client
		var err error

		if _, sErr := os.Stat(caPath); sErr == nil {
			cli, err = client.NewMTLSClientHelper(cfg.Host, caPath, certPath, keyPath, logger)
		} else {
			cli, err = client.NewClient(client.Config{
				Address:        cfg.Host,
				ConnectTimeout: 5 * time.Second,
				Logger:         logger,
			})
		}

		if err != nil {
			logger.Error("Connection failed, retrying in 5s", "err", err)
			select {
			case <-ctx.Done():
				break Loop
			case <-time.After(5 * time.Second):
				continue Loop
			}
		}

		// Inject Configured ID into the client so it's used during Subscribe handshake
		// Note: We need access to the underlying client config or a way to set it.
		// Since NewMTLSClientHelper returns a *Client with configured struct, we can't easily mutate it
		// without a setter or modifying NewMTLSClientHelper.
		// HOWEVER, we modified client.Config to include ClientID. NewMTLSClientHelper needs to support it or we reconstruct manually.
		// The helper doesn't take ClientID arg. We'll reconstruct manually here to be safe and explicit.
		cli.Close() // Close the helper-created one

		// Rebuild properly
		caCert, _ := os.ReadFile(caPath)
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caCert)
		cert, _ := tls.LoadX509KeyPair(certPath, keyPath)
		tlsConf := &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}}

		cli, err = client.NewClient(client.Config{
			Address:        cfg.Host,
			ClientID:       cfg.ID,
			TLSConfig:      tlsConf,
			ConnectTimeout: 5 * time.Second,
			Logger:         logger,
		})
		if err != nil {
			logger.Error("Client rebuild failed", "err", err)
			time.Sleep(5 * time.Second)
			continue Loop
		}

		logger.Info("Connected, subscribing...", "seq", lastSeenSeq)

		// Create a separate context for the subscription to handle graceful shutdown
		// while the client is blocked on Read.
		doneCh := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				cli.Close()
			case <-doneCh:
			}
		}()

		err = cli.Subscribe(cfg.Database, lastSeenSeq, func(c client.Change) error {
			var val any
			if !c.IsDelete {
				switch cfg.ValueFormat {
				case "json":
					var js any
					if err := json.Unmarshal(c.Value, &js); err == nil {
						val = js
					} else {
						val = string(c.Value)
					}
				case "base64":
					val = base64.StdEncoding.EncodeToString(c.Value)
				default:
					val = string(c.Value)
				}
			}

			entry := CDCLogEntry{
				Key:      string(c.Key),
				Value:    val,
				IsDelete: c.IsDelete,
				LogSeq:   c.LogSeq,
				TxID:     c.TxID,
				Time:     time.Now().UnixMilli(),
			}

			data, err := json.Marshal(entry)
			if err != nil {
				return fmt.Errorf("marshal error: %w", err)
			}
			data = append(data, '\n')

			fileMutex.Lock()
			defer fileMutex.Unlock()

			n, err := currentFile.Write(data)
			if err != nil {
				return err
			}
			currentBytes += int64(n)
			lastSeenSeq = c.LogSeq
			lastWriteTime = time.Now() // Update active timestamp

			if currentBytes > int64(cfg.MaxFileSize*1024*1024) {
				fileMutex.Unlock()
				if err := rotateFile(lastSeenSeq); err != nil {
					logger.Error("Rotation failed", "err", err)
					fileMutex.Lock()
				} else {
					fileMutex.Lock()
				}
			}

			return nil
		})

		close(doneCh) // Stop the watcher

		if ctx.Err() != nil {
			break Loop
		}

		if err != nil {
			// Fatal error check: if the server says we are OUT_OF_SYNC, we must stop.
			if strings.Contains(err.Error(), "OUT_OF_SYNC") {
				logger.Error("Fatal replication error: Client is out of sync with server WAL retention.", "err", err)
				logger.Error("Manual intervention required: Delete the state file to reset, or check server logs.")
				os.Exit(1)
			}

			logger.Error("Subscription dropped, retrying in 3s", "err", err)
		} else {
			logger.Warn("Subscription closed unexpectedly, retrying in 3s")
		}

		select {
		case <-ctx.Done():
			break Loop
		case <-time.After(3 * time.Second):
		}
	}

	logger.Info("CDC shutting down, finalizing state...")
	// Final rotation to ensure data is safe and state is updated
	if err := rotateFile(lastSeenSeq); err != nil {
		logger.Error("Final rotation failed", "err", err)
	}
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

package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"turnstone/client"
	"turnstone/config"
)

// FileConsumerConfig represents the JSON configuration for the file-based CDC consumer.
type FileConsumerConfig struct {
	ID             string `json:"id"`
	Host           string `json:"host"`
	Database       string `json:"database"`
	OutputDir      string `json:"output_dir"`
	StateFile      string `json:"state_file"`
	MaxFileSizeMB  int    `json:"max_file_size_mb"`
	RotateInterval string `json:"rotate_interval"`
	FlushInterval  string `json:"flush_interval"`
	ValueFormat    string `json:"value_format"`
	MetricsAddr    string `json:"metrics_addr"`
	TLSCertFile    string `json:"tls_cert_file"`
	TLSKeyFile     string `json:"tls_key_file"`
	TLSCAFile      string `json:"tls_ca_file"`
}

// DefaultFileConsumerConfig returns a sensible default configuration.
// By default, it looks for the specialized 'cdc' identity certs.
func DefaultFileConsumerConfig() FileConsumerConfig {
	return FileConsumerConfig{
		ID:             "cdc-worker-1",
		Host:           "localhost:6379",
		Database:       "1",
		OutputDir:      "cdc_logs",
		StateFile:      "cdc.state",
		MaxFileSizeMB:  100,
		RotateInterval: "1m",
		FlushInterval:  "30s",
		ValueFormat:    "text",
		MetricsAddr:    ":9091",
		TLSCertFile:    "certs/cdc.crt",
		TLSKeyFile:     "certs/cdc.key",
		TLSCAFile:      "certs/ca.crt",
	}
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

// CDCConfig holds configuration for the generic CDC client (Stdout/Library mode).
type CDCConfig struct {
	Host        string
	Home        string
	Database    string
	StateFile   string
	TextMode    bool
	MetricsAddr string
	Logger      *slog.Logger
	CertFile    string
	KeyFile     string
	CAFile      string
}

// Event defines the JSON structure for generic CDC events.
type Event struct {
	Seq      uint64      `json:"seq"`
	Tx       uint64      `json:"tx"`
	Key      string      `json:"key"`
	Val      interface{} `json:"val,omitempty"`
	IsDelete bool        `json:"del"`
}

// Global metric for monitoring within the replication package
var currentSeq atomic.Uint64

// StartFileConsumer runs the dedicated file-based CDC consumer mode.
// This handles configuration loading, file rotation, state persistence, and metrics.
func StartFileConsumer(homeDir string, logger *slog.Logger) {
	cfg := DefaultFileConsumerConfig()

	configPath := filepath.Join(homeDir, "turnstone.cdc.json")
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

	caPath := config.ResolvePath(homeDir, cfg.TLSCAFile)
	certPath := config.ResolvePath(homeDir, cfg.TLSCertFile)
	keyPath := config.ResolvePath(homeDir, cfg.TLSKeyFile)
	outputDir := config.ResolvePath(homeDir, cfg.OutputDir)
	statePath := config.ResolvePath(homeDir, cfg.StateFile)

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

	// Start Metrics Server
	go startMetricsServer(cfg.MetricsAddr, logger)

	lastSeenSeq := startSeq
	var currentFile *os.File
	var currentBytes int64
	lastRotationTime := time.Now()
	lastWriteTime := time.Now()
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
		lastWriteTime = time.Now()
		logger.Info("Rotated log file", "file", filename)
		return nil
	}

	if err := rotateFile(lastSeenSeq); err != nil {
		logger.Error("Failed to create initial log file", "err", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sig := <-sigCh
		logger.Info("Received signal, shutting down...", "signal", sig)
		cancel()
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
				if currentFile != nil {
					currentFile.Sync()
				}
				currentSeq := lastSeenSeq
				needsAgeRotation := time.Since(lastRotationTime) > rotateDuration
				needsIdleRotation := time.Since(lastWriteTime) > flushDuration
				hasBytes := currentBytes > 0
				fileMutex.Unlock()

				if hasBytes && (needsAgeRotation || needsIdleRotation) {
					reason := "age"
					if needsIdleRotation {
						reason = "idle"
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

Loop:
	for {
		if ctx.Err() != nil {
			break Loop
		}

		var cli *client.Client
		var err error

		if _, sErr := os.Stat(caPath); sErr == nil {
			// Manually construct client to ensure ClientID is set correctly
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
		} else {
			cli, err = client.NewClient(client.Config{
				Address:        cfg.Host,
				ClientID:       cfg.ID,
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

		logger.Info("Connected, subscribing...", "seq", lastSeenSeq)

		doneCh := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				cli.Close()
			case <-doneCh:
			}
		}()

		err = cli.Subscribe(cfg.Database, lastSeenSeq, func(c client.Change) error {
			if c.IsSnapshotDone {
				lastSeenSeq = c.LogSeq
				logger.Info("Snapshot sync complete, updated sequence", "seq", lastSeenSeq)
				saveState(lastSeenSeq)
				return nil
			}

			if len(c.Key) == 0 {
				return nil
			}

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
			lastWriteTime = time.Now()

			if currentBytes > int64(cfg.MaxFileSizeMB*1024*1024) {
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

		close(doneCh)

		if ctx.Err() != nil {
			break Loop
		}

		if err != nil {
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
	if err := rotateFile(lastSeenSeq); err != nil {
		logger.Error("Final rotation failed", "err", err)
	}
}

// StartCDC initiates the generic Change Data Capture process (Stdout mode).
// Kept for backward compatibility or library usage.
func StartCDC(cfg CDCConfig) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	lastSeq := uint64(0)
	if content, err := os.ReadFile(cfg.StateFile); err == nil {
		if val, err := strconv.ParseUint(string(content), 10, 64); err == nil {
			lastSeq = val
			currentSeq.Store(lastSeq)
			logger.Info("Resuming CDC state", "last_seq", lastSeq, "state_file", cfg.StateFile)
		}
	} else {
		logger.Info("Starting CDC from scratch (seq 0)", "reason", "no_state_file")
	}

	go startMetricsServer(cfg.MetricsAddr, logger)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("Received signal, shutting down...", "signal", sig)
		cancel()
		<-sigCh
		logger.Warn("Received second signal, forcing exit...")
		os.Exit(1)
	}()

	encoder := json.NewEncoder(os.Stdout)
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			break
		}

		startTime := time.Now()

		err := runCDCStream(ctx, cfg, lastSeq, encoder, func(seq uint64) {
			lastSeq = seq
			currentSeq.Store(seq)
		})

		if ctx.Err() != nil {
			break
		}

		if time.Since(startTime) > 10*time.Second {
			backoff = 1 * time.Second
		}

		if err != nil {
			if strings.Contains(err.Error(), "OUT_OF_SYNC") {
				logger.Error("FATAL REPLICATION ERROR: Server logs purged",
					"required_seq", lastSeq,
					"action", fmt.Sprintf("Delete %s to resync from head", cfg.StateFile),
					"err", err,
				)
				os.Exit(1)
			}
			logger.Error("CDC disconnected", "err", err, "retry_in", backoff)
		} else {
			logger.Info("CDC connection closed cleanly", "retry_in", backoff)
		}

		select {
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		case <-ctx.Done():
			break
		}
	}
}

func runCDCStream(ctx context.Context, cfg CDCConfig, startID uint64, encoder *json.Encoder, onProgress func(uint64)) error {
	var cl *client.Client
	var err error

	resolve := func(path, def string) string {
		if path == "" {
			path = def
		}
		if path != "" && !filepath.IsAbs(path) && cfg.Home != "" {
			return filepath.Join(cfg.Home, path)
		}
		return path
	}

	caPath := resolve(cfg.CAFile, "certs/ca.crt")
	certPath := resolve(cfg.CertFile, "certs/cdc.crt")
	keyPath := resolve(cfg.KeyFile, "certs/cdc.key")

	cfg.Logger.Info("Connecting to server", "host", cfg.Host)

	if _, statErr := os.Stat(caPath); statErr == nil {
		cl, err = client.NewMTLSClientHelper(cfg.Host, caPath, certPath, keyPath, cfg.Logger)
	} else {
		cl, err = client.NewClient(client.Config{Address: cfg.Host, ConnectTimeout: 5 * time.Second, Logger: cfg.Logger})
	}

	if err != nil {
		return err
	}

	defer cl.Close()

	go func() {
		<-ctx.Done()
		cl.Close()
	}()

	var latestPersistedSeq uint64 = startID
	var latestSeenSeq uint64 = startID
	lastFlushTime := time.Now()
	const flushInterval = 500 * time.Millisecond

	flushState := func(seq uint64) {
		if seq <= latestPersistedSeq {
			return
		}
		tmpFile := cfg.StateFile + ".tmp"
		if err := os.WriteFile(tmpFile, []byte(strconv.FormatUint(seq, 10)), 0o644); err == nil {
			_ = os.Rename(tmpFile, cfg.StateFile)
			latestPersistedSeq = seq
		}
	}

	defer func() {
		flushState(latestSeenSeq)
	}()

	return cl.Subscribe(cfg.Database, startID, func(c client.Change) error {
		evt := Event{
			Seq:      c.LogSeq,
			Tx:       c.TxID,
			Key:      string(c.Key),
			IsDelete: c.IsDelete,
		}
		if !c.IsDelete {
			if cfg.TextMode {
				evt.Val = string(c.Value)
			} else {
				evt.Val = c.Value
			}
		}

		if err := encoder.Encode(evt); err != nil {
			return err
		}

		latestSeenSeq = c.LogSeq
		onProgress(c.LogSeq)

		if time.Since(lastFlushTime) > flushInterval {
			flushState(c.LogSeq)
			lastFlushTime = time.Now()
		}
		return nil
	})
}

func startMetricsServer(addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		seq := currentSeq.Load()
		fmt.Fprintf(w, "# HELP cdc_last_seq The last log sequence number processed.\n")
		fmt.Fprintf(w, "# TYPE cdc_last_seq gauge\n")
		fmt.Fprintf(w, "cdc_last_seq %d\n", seq)
	})

	server := &http.Server{Addr: addr, Handler: mux}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Metrics server failed", "addr", addr, "err", err)
	}
}

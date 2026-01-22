package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"turnstone/client"
)

// CDCConfig holds configuration for the CDC client.
type CDCConfig struct {
	Host        string
	Home        string
	Database    string // Renamed from Partition
	StateFile   string
	TextMode    bool
	MetricsAddr string
	Logger      *slog.Logger
	// Custom TLS paths (relative to Home or absolute)
	CertFile string
	KeyFile  string
	CAFile   string
}

// Event defines the JSON structure for CDC events.
type Event struct {
	Seq      uint64      `json:"seq"`            // Global Log Sequence
	Tx       uint64      `json:"tx"`             // Transaction ID
	Key      string      `json:"key"`            // Key
	Val      interface{} `json:"val,omitempty"` // Value
	IsDelete bool        `json:"del"`            // True if deletion
}

// Global metric for monitoring within the replication package
var currentSeq atomic.Uint64

// StartCDC initiates the Change Data Capture process.
// It handles state resumption, signal handling, and automatic retries.
func StartCDC(cfg CDCConfig) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	// 1. Resume State
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

	// 2. Start Metrics Server
	go startMetricsServer(cfg.MetricsAddr, logger)

	// 3. Setup Signal Handling
	ctx, cancel := context.WithCancel(context.Background())
	// Buffer of 2 to catch the second signal for force quit
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		// First signal: Graceful shutdown
		sig := <-sigCh
		logger.Info("Received signal, shutting down...", "signal", sig)
		cancel()

		// Second signal: Hard exit
		<-sigCh
		logger.Warn("Received second signal, forcing exit...")
		os.Exit(1)
	}()

	encoder := json.NewEncoder(os.Stdout)
	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	// 4. Main Retry Loop
	for {
		if ctx.Err() != nil {
			break // Shutdown requested
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
			// FATAL ERROR CHECK
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

	// Helper to resolve paths relative to Home if necessary
	resolve := func(path, def string) string {
		if path == "" {
			path = def
		}
		// If path is relative and Home is set, join them.
		if path != "" && !filepath.IsAbs(path) && cfg.Home != "" {
			return filepath.Join(cfg.Home, path)
		}
		return path
	}

	// Use configured certs or fall back to standard locations
	caPath := resolve(cfg.CAFile, "certs/ca.crt")
	certPath := resolve(cfg.CertFile, "certs/client.crt")
	keyPath := resolve(cfg.KeyFile, "certs/client.key")

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

	// Ensure client closes on context cancel to unblock Subscribe
	go func() {
		<-ctx.Done()
		cl.Close()
	}()

	// State persistence optimization
	var latestPersistedSeq uint64 = startID
	var latestSeenSeq uint64 = startID
	lastFlushTime := time.Now()
	const flushInterval = 500 * time.Millisecond

	// Persistence Helper
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

	// Ensure we flush the very last seen state when we exit (e.g. connection drop or shutdown)
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

		// Update in-memory trackers
		latestSeenSeq = c.LogSeq
		onProgress(c.LogSeq)

		// Debounced Flush: Only write to disk if enough time has passed
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

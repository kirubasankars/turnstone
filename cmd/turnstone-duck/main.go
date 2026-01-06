package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"turnstone/client"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Configuration ---

type Config struct {
	ServerAddr     string
	Partition      string
	DBPath         string
	MetricsAddr    string
	BatchSize      int
	FlushInterval  time.Duration
	ConnectTimeout time.Duration
	CAFile         string
	CertFile       string
	KeyFile        string
	UnsafeMode     bool // New flag for performance
}

// --- Metrics ---

var (
	metricOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "turnstone_duck",
		Name:      "rows_processed_total",
		Help:      "Total number of rows processed",
	}, []string{"type"}) // type: insert, delete

	metricBatchDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "turnstone_duck",
		Name:      "batch_commit_duration_seconds",
		Help:      "Time taken to commit a batch to DuckDB",
		Buckets:   prometheus.DefBuckets,
	})

	metricLastSeq = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "turnstone_duck",
		Name:      "last_sequence_id",
		Help:      "The last LogSeq successfully committed",
	})

	metricBufferErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "turnstone_duck",
		Name:      "buffer_overflow_errors_total",
		Help:      "Number of times the internal channel buffer filled up",
	})
)

func init() {
	prometheus.MustRegister(metricOps, metricBatchDuration, metricLastSeq, metricBufferErrors)
}

// --- Main ---

func main() {
	cfg := parseFlags()

	// Setup Structured Logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Starting turnstone-duck", "config", cfg)

	// Start Metrics Server
	go startMetricsServer(cfg.MetricsAddr, logger)

	// Initialize DuckDB (Single Instance)
	// DuckDB handles locking internally, but we must ensure we are the only process writing to this file.
	db, err := sql.Open("duckdb", cfg.DBPath)
	if err != nil {
		logger.Error("Failed to open DuckDB", "path", cfg.DBPath, "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := initSchema(db, cfg.UnsafeMode); err != nil {
		logger.Error("Schema initialization failed", "error", err)
		os.Exit(1)
	}

	// Global Shutdown Context
	ctx, cancel := context.WithCancel(context.Background())
	setupSignalHandler(cancel, logger)

	// Main Retry Loop
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			logger.Info("Shutting down worker loop")
			return
		}

		err := runPipeline(ctx, db, cfg, logger)
		if err == nil {
			logger.Info("Pipeline finished cleanly")
			return
		}

		// Only log error if it wasn't a clean shutdown
		if ctx.Err() == nil {
			logger.Error("Pipeline failed", "error", err, "retry_in", backoff)
			select {
			case <-time.After(backoff):
				// Exponential backoff with cap
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
			case <-ctx.Done():
				return
			}
		} else {
			return
		}
	}
}

func parseFlags() Config {
	c := Config{}
	flag.StringVar(&c.ServerAddr, "addr", "localhost:6379", "TurnstoneDB address")
	flag.StringVar(&c.Partition, "partition", "1", "Partition ID")
	flag.StringVar(&c.DBPath, "db", "replica.duckdb", "DuckDB path")
	flag.StringVar(&c.MetricsAddr, "metrics", ":9090", "Metrics HTTP address")
	flag.IntVar(&c.BatchSize, "batch-size", 10000, "Rows per commit transaction")
	flag.DurationVar(&c.FlushInterval, "flush-interval", 10*time.Second, "Max time to buffer data before commit")
	flag.DurationVar(&c.ConnectTimeout, "timeout", 5*time.Second, "Connection timeout")
	flag.StringVar(&c.CAFile, "ca", "certs/ca.crt", "TLS CA file")
	flag.StringVar(&c.CertFile, "cert", "certs/cdc.crt", "TLS Cert file")
	flag.StringVar(&c.KeyFile, "key", "certs/cdc.key", "TLS Key file")
	flag.BoolVar(&c.UnsafeMode, "unsafe", false, "Enable unsafe mode (synchronous=off) for higher write throughput")
	flag.Parse()
	return c
}

func initSchema(db *sql.DB, unsafe bool) error {
	// PRAGMA for performance
	pragmas := []string{
		"PRAGMA memory_limit='1GB';",
	}
	if unsafe {
		// Increase WAL checkpoint threshold to reduce disk churn.
		// note: 'synchronous' is not supported in DuckDB.
		pragmas = append(pragmas, "PRAGMA wal_autocheckpoint='1GB';")
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}

	// Checkpoints table: Stores the last successfully processed sequence number
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS replication_state (partition_name TEXT PRIMARY KEY, last_seq UBIGINT)`); err != nil {
		return err
	}

	// Data Table: The actual replicated data
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS turnstone_data (key TEXT PRIMARY KEY, value TEXT, log_seq UBIGINT, tx_id UBIGINT)`)
	return err
}

func startMetricsServer(addr string, logger *slog.Logger) {
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	logger.Info("Metrics server listening", "addr", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		logger.Error("Metrics server failed", "error", err)
	}
}

func setupSignalHandler(cancel context.CancelFunc, logger *slog.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		// First signal: Initiate graceful shutdown
		sig := <-sigCh
		logger.Info("Received signal, initiating graceful shutdown (press again to force)", "signal", sig)
		cancel()

		// Second signal: Force exit
		sig = <-sigCh
		logger.Info("Received second signal, forcing exit", "signal", sig)
		os.Exit(1)
	}()
}

// --- Pipeline Logic ---

func runPipeline(ctx context.Context, db *sql.DB, cfg Config, logger *slog.Logger) error {
	// 1. Get Checkpoint
	// We read the last committed sequence number to know where to resume from.
	var startSeq uint64
	err := db.QueryRow("SELECT last_seq FROM replication_state WHERE partition_name = ?", cfg.Partition).Scan(&startSeq)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("read checkpoint: %w", err)
	}
	logger.Info("Resuming replication", "partition", cfg.Partition, "seq", startSeq)

	// 2. Connect
	var cli *client.Client
	if _, statErr := os.Stat(cfg.CAFile); statErr == nil {
		cli, err = client.NewMTLSClientHelper(cfg.ServerAddr, cfg.CAFile, cfg.CertFile, cfg.KeyFile, nil)
	} else {
		logger.Warn("Certificates not found, falling back to plaintext")
		cli, err = client.NewClient(client.Config{Address: cfg.ServerAddr, ConnectTimeout: cfg.ConnectTimeout})
	}
	if err != nil {
		return fmt.Errorf("connect failed: %w", err)
	}
	defer cli.Close()

	// 3. Channels
	eventsCh := make(chan client.Change, cfg.BatchSize*2)
	errCh := make(chan error, 1)

	// --- Reader Goroutine ---
	go func() {
		defer close(eventsCh)

		// Subscribe to the change feed starting from startSeq
		subErr := cli.Subscribe(cfg.Partition, startSeq, func(c client.Change) error {
			select {
			case eventsCh <- c:
				return nil
			case <-ctx.Done():
				return fmt.Errorf("shutdown")
			default:
				// If channel is full, we block. This provides backpressure to the server.
				metricBufferErrors.Inc()
				select {
				case eventsCh <- c:
					return nil
				case <-ctx.Done():
					return fmt.Errorf("shutdown")
				}
			}
		})

		if subErr != nil {
			// Don't report "closed connection" errors during normal shutdown
			if ctx.Err() == nil {
				select {
				case errCh <- subErr:
				default:
				}
			}
		}
	}()

	// --- Writer Goroutine (Main Thread) ---

	// PREPARE STATEMENTS ONCE (Optimization)
	// Preparing statements is expensive. We do it once here and reuse them in transactions.
	upsertQ := `INSERT OR REPLACE INTO turnstone_data (key, value, log_seq, tx_id) VALUES (?, ?, ?, ?)`
	delQ := `DELETE FROM turnstone_data WHERE key = ?`
	stateQ := `INSERT OR REPLACE INTO replication_state (partition_name, last_seq) VALUES (?, ?)`

	stmtUpsertGlobal, err := db.PrepareContext(ctx, upsertQ)
	if err != nil {
		return fmt.Errorf("prepare upsert: %w", err)
	}
	defer stmtUpsertGlobal.Close()

	stmtDelGlobal, err := db.PrepareContext(ctx, delQ)
	if err != nil {
		return fmt.Errorf("prepare delete: %w", err)
	}
	defer stmtDelGlobal.Close()

	stmtStateGlobal, err := db.PrepareContext(ctx, stateQ)
	if err != nil {
		return fmt.Errorf("prepare state: %w", err)
	}
	defer stmtStateGlobal.Close()

	var tx *sql.Tx
	var stmtUpsert, stmtDel, stmtState *sql.Stmt

	// Helper: Start a new Transaction
	startTx := func() error {
		var tErr error
		tx, tErr = db.BeginTx(ctx, nil)
		if tErr != nil {
			return tErr
		}

		// Use the already prepared statements within this transaction
		stmtUpsert = tx.Stmt(stmtUpsertGlobal)
		stmtDel = tx.Stmt(stmtDelGlobal)
		stmtState = tx.Stmt(stmtStateGlobal)

		return nil
	}

	// Helper: Commit the current batch
	commitBatch := func(seq uint64, count int) error {
		if count == 0 {
			return nil
		}
		start := time.Now()
		// 1. Update Checkpoint
		if _, err := stmtState.Exec(cfg.Partition, seq); err != nil {
			return fmt.Errorf("failed to write checkpoint: %w", err)
		}
		// 2. Commit Transaction (Data + Checkpoint)
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx: %w", err)
		}

		metricBatchDuration.Observe(time.Since(start).Seconds())
		metricLastSeq.Set(float64(seq))
		logger.Info("Batch committed", "count", count, "seq", seq, "dur_ms", time.Since(start).Milliseconds())
		return nil
	}

	if err := startTx(); err != nil {
		return err
	}

	batchCount := 0
	lastSeenSeq := startSeq // Track the latest sequence number seen in memory

	ticker := time.NewTicker(cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Safe Shutdown: Flush whatever is in memory
			if tx != nil && batchCount > 0 {
				logger.Info("Context cancelled, attempting final commit", "count", batchCount, "seq", lastSeenSeq)
				if err := commitBatch(lastSeenSeq, batchCount); err != nil {
					logger.Error("Final commit failed", "error", err)
				}
			} else if tx != nil {
				logger.Info("Context cancelled, rolling back empty transaction")
				tx.Rollback()
			}
			return nil

		case readerErr := <-errCh:
			// Reader failed unexpectedly
			if tx != nil {
				tx.Rollback()
			}
			return readerErr

		case <-ticker.C:
			// Time-based Flush
			// Check if we have pending data to write
			if batchCount > 0 {
				logger.Info("Flush interval reached", "pending", batchCount, "seq", lastSeenSeq)
				if err := commitBatch(lastSeenSeq, batchCount); err != nil {
					return err
				}
				batchCount = 0
				if err := startTx(); err != nil {
					return err
				}
			}

		case c, ok := <-eventsCh:
			if !ok {
				// Event channel closed cleanly (shouldn't happen unless reader exits)
				if tx != nil && batchCount > 0 {
					commitBatch(lastSeenSeq, batchCount)
				}
				return fmt.Errorf("event channel closed unexpectedly")
			}

			// Update state
			lastSeenSeq = c.LogSeq

			// Execute Op
			if c.IsDelete {
				if _, err := stmtDel.Exec(string(c.Key)); err != nil {
					tx.Rollback()
					return err
				}
				metricOps.WithLabelValues("delete").Inc()
			} else {
				if _, err := stmtUpsert.Exec(string(c.Key), string(c.Value), c.LogSeq, c.TxID); err != nil {
					tx.Rollback()
					return err
				}
				metricOps.WithLabelValues("insert").Inc()
			}

			batchCount++

			// Check Batch Size
			if batchCount >= cfg.BatchSize {
				if err := commitBatch(lastSeenSeq, batchCount); err != nil {
					return err
				}
				batchCount = 0
				if err := startTx(); err != nil {
					return err
				}
				// Reset ticker so we don't flush immediately after a full batch
				ticker.Reset(cfg.FlushInterval)
			}
		}
	}
}

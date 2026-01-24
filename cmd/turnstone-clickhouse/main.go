package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Configuration ---

type Config struct {
	InputDir     string
	ArchiveDir   string
	ClickHouse   ClickHouseConfig
	MetricsAddr  string
	PollInterval time.Duration
	MinFileAge   time.Duration
	BatchSize    int
}

type ClickHouseConfig struct {
	Addr     string
	Database string
	User     string
	Password string
}

// --- Metrics ---

var (
	metricOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "turnstone_ch",
		Name:      "rows_processed_total",
		Help:      "Total number of rows processed",
	}, []string{"type"}) // type: insert, delete

	metricFilesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "turnstone_ch",
		Name:      "files_processed_total",
		Help:      "Total number of log files processed",
	})

	metricRowsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "turnstone_ch",
		Name:      "rows_dropped_total",
		Help:      "Total number of rows dropped during parsing",
	})
)

func init() {
	prometheus.MustRegister(metricOps, metricFilesProcessed, metricRowsDropped)
}

// --- Data Models ---

type CDCEvent struct {
	Key      string          `json:"k"`
	Value    json.RawMessage `json:"v"`
	IsDelete bool            `json:"del"`
	LogSeq   uint64          `json:"seq"`
	TxID     uint64          `json:"tx"`
	Time     int64           `json:"ts"`
}

// --- Main ---

func main() {
	cfg := parseFlags()

	// Setup Structured Logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Starting turnstone-clickhouse", "config", cfg)

	if cfg.InputDir == "" {
		logger.Error("Input directory (-input) is required")
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.ArchiveDir, 0o755); err != nil {
		logger.Error("Failed to create archive directory", "err", err)
		os.Exit(1)
	}

	go startMetricsServer(cfg.MetricsAddr, logger)

	// Connect to ClickHouse
	conn, err := connectClickHouse(cfg.ClickHouse)
	if err != nil {
		logger.Error("Failed to connect to ClickHouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	if err := initSchema(context.Background(), conn, cfg.ClickHouse.Database); err != nil {
		logger.Error("Schema initialization failed", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	setupSignalHandler(cancel, logger)

	runFileIngestion(ctx, conn, cfg, logger)
}

func parseFlags() Config {
	c := Config{}
	flag.StringVar(&c.InputDir, "input", "", "Directory containing cdc-*.jsonl files")
	flag.StringVar(&c.ArchiveDir, "archive", "archive", "Directory to move processed files")
	flag.StringVar(&c.ClickHouse.Addr, "addr", "127.0.0.1:9000", "ClickHouse address")
	flag.StringVar(&c.ClickHouse.Database, "db", "turnstone", "ClickHouse database")
	flag.StringVar(&c.ClickHouse.User, "user", "default", "ClickHouse user")
	flag.StringVar(&c.ClickHouse.Password, "password", "", "ClickHouse password")
	flag.StringVar(&c.MetricsAddr, "metrics", ":9090", "Metrics HTTP address")
	flag.DurationVar(&c.PollInterval, "poll", 1*time.Second, "Polling interval")
	flag.DurationVar(&c.MinFileAge, "min-age", 1*time.Second, "Minimum file age")
	flag.IntVar(&c.BatchSize, "batch", 10000, "Batch insert size")
	flag.Parse()
	return c
}

func connectClickHouse(cfg ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.Addr},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Debug:           false,
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return conn, nil
}

func initSchema(ctx context.Context, conn driver.Conn, dbName string) error {
	// Ensure Database Exists
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
		return err
	}

	// Main Data Table using ReplacingMergeTree for deduplication/upserts
	// We use `log_seq` as the version column. Higher seq wins.
	// `is_deleted` marks tombstones.
	ddlData := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.turnstone_data (
			key String,
			value String,
			log_seq UInt64,
			tx_id UInt64,
			updated_at DateTime64(3),
			is_deleted UInt8
		) ENGINE = ReplacingMergeTree(log_seq)
		ORDER BY (key)
	`, dbName)

	if err := conn.Exec(ctx, ddlData); err != nil {
		return err
	}

	// Checkpoints table to track processed files
	// Also using ReplacingMergeTree to ensure only the latest attempt for a filename is kept
	ddlCheckpoints := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.file_checkpoints (
			filename String,
			processed_at DateTime
		) ENGINE = ReplacingMergeTree()
		ORDER BY (filename)
	`, dbName)

	if err := conn.Exec(ctx, ddlCheckpoints); err != nil {
		return err
	}

	return nil
}

func startMetricsServer(addr string, logger *slog.Logger) {
	http.Handle("/metrics", promhttp.Handler())
	logger.Info("Metrics server listening", "addr", addr)
	http.ListenAndServe(addr, nil)
}

func setupSignalHandler(cancel context.CancelFunc, logger *slog.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("Received signal, shutting down", "signal", sig)
		cancel()
	}()
}

// --- Ingestion Loop ---

func runFileIngestion(ctx context.Context, conn driver.Conn, cfg Config, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 1. List Files
			files, err := os.ReadDir(cfg.InputDir)
			if err != nil {
				logger.Error("Failed to list input directory", "err", err)
				continue
			}

			var candidates []string
			for _, f := range files {
				if !f.IsDir() && strings.HasPrefix(f.Name(), "cdc-") && strings.HasSuffix(f.Name(), ".jsonl") {
					info, err := f.Info()
					if err != nil {
						continue
					}
					if time.Since(info.ModTime()) < cfg.MinFileAge {
						continue
					}
					candidates = append(candidates, f.Name())
				}
			}

			sort.Strings(candidates)

			for _, fname := range candidates {
				if ctx.Err() != nil {
					return
				}

				// 2. Check deduplication
				if isProcessed(ctx, conn, cfg.ClickHouse.Database, fname) {
					os.Rename(filepath.Join(cfg.InputDir, fname), filepath.Join(cfg.ArchiveDir, fname))
					continue
				}

				// 3. Process
				start := time.Now()
				if err := processFile(ctx, conn, cfg, fname, logger); err != nil {
					logger.Error("Failed to process file", "file", fname, "err", err)
					break
				}
				duration := time.Since(start)

				// 4. Archive
				src := filepath.Join(cfg.InputDir, fname)
				dst := filepath.Join(cfg.ArchiveDir, fname)
				if err := os.Rename(src, dst); err != nil {
					logger.Error("Failed to move file to archive", "file", fname, "err", err)
				}
				metricFilesProcessed.Inc()
				logger.Info("File processed", "file", fname, "duration_sec", duration.Seconds())
			}
		}
	}
}

func isProcessed(ctx context.Context, conn driver.Conn, db, fname string) bool {
	query := fmt.Sprintf("SELECT count() FROM %s.file_checkpoints WHERE filename = ?", db)
	var count uint64
	if err := conn.QueryRow(ctx, query, fname).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func processFile(ctx context.Context, conn driver.Conn, cfg Config, fname string, logger *slog.Logger) error {
	path := filepath.Join(cfg.InputDir, fname)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Prepare Batch Insert
	var batch driver.Batch

	// Safety: Abort batch if function exits with batch active (prevents connection leak)
	defer func() {
		if batch != nil {
			_ = batch.Abort()
		}
	}()

	ensureBatch := func() error {
		if batch != nil {
			return nil
		}
		var err error
		batch, err = conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.turnstone_data", cfg.ClickHouse.Database))
		return err
	}

	scanner := bufio.NewScanner(f)
	// Increase buffer size for large lines if necessary
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	rowsInBatch := 0
	totalRows := 0
	var upserts, deletes float64

	flush := func() error {
		if batch == nil || rowsInBatch == 0 {
			return nil
		}
		if err := batch.Send(); err != nil {
			return err
		}
		batch = nil // Batch is consumed and closed
		rowsInBatch = 0
		return nil
	}

	// Initialize first batch
	if err := ensureBatch(); err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for scanner.Scan() {
		var evt CDCEvent
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if err := json.Unmarshal(line, &evt); err != nil {
			logger.Warn("Malformed JSON", "file", fname, "line", string(line), "err", err)
			metricRowsDropped.Inc()
			continue
		}

		// Map to ClickHouse Schema
		// is_deleted = 1 if IsDelete is true
		isDel := uint8(0)
		if evt.IsDelete {
			isDel = 1
			deletes++
		} else {
			upserts++
		}

		// Convert TS (ms) to time.Time
		ts := time.UnixMilli(evt.Time)

		// Value as string
		valStr := ""
		if len(evt.Value) > 0 {
			valStr = string(evt.Value)
		}

		if err := batch.Append(
			evt.Key,
			valStr,
			evt.LogSeq,
			evt.TxID,
			ts,
			isDel,
		); err != nil {
			return fmt.Errorf("batch append: %w", err)
		}

		rowsInBatch++
		totalRows++

		if rowsInBatch >= cfg.BatchSize {
			if err := flush(); err != nil {
				return fmt.Errorf("batch flush: %w", err)
			}
			// Prepare next
			if err := ensureBatch(); err != nil {
				return fmt.Errorf("prepare next batch: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	// Final flush
	if err := flush(); err != nil {
		return fmt.Errorf("final flush: %w", err)
	}

	// Mark file as processed
	// Using Exec for simple insert
	ckptQuery := fmt.Sprintf("INSERT INTO %s.file_checkpoints (filename, processed_at) VALUES (?, ?)", cfg.ClickHouse.Database)
	if err := conn.Exec(ctx, ckptQuery, fname, time.Now()); err != nil {
		return fmt.Errorf("checkpoint insert: %w", err)
	}

	metricOps.WithLabelValues("insert").Add(upserts)
	metricOps.WithLabelValues("delete").Add(deletes)

	return nil
}

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
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- Configuration ---

type Config struct {
	InputDir       string
	ArchiveDir     string
	DBPath         string
	MetricsAddr    string
	PollInterval   time.Duration
	MinFileAge     time.Duration
	VacuumInterval int // New config for periodic cleanup
	UnsafeMode     bool
}

// --- Metrics ---

var (
	metricOps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "turnstone_duck",
		Name:      "rows_processed_total",
		Help:      "Total number of rows processed via bulk load",
	}, []string{"type"}) // type: bulk_upsert, bulk_delete

	metricFilesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "turnstone_duck",
		Name:      "files_processed_total",
		Help:      "Total number of log files processed",
	})
)

func init() {
	prometheus.MustRegister(metricOps, metricFilesProcessed)
}

// --- Main ---

func main() {
	cfg := parseFlags()

	// Setup Structured Logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	logger.Info("Starting turnstone-duck (Bulk Loader Mode)", "config", cfg)

	if cfg.InputDir == "" {
		logger.Error("Input directory (-input) is required")
		os.Exit(1)
	}

	// Create Archive Dir
	if err := os.MkdirAll(cfg.ArchiveDir, 0o755); err != nil {
		logger.Error("Failed to create archive directory", "err", err)
		os.Exit(1)
	}

	// Start Metrics Server
	go startMetricsServer(cfg.MetricsAddr, logger)

	// Initialize DuckDB
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

	// Run File Loop
	runFileIngestion(ctx, db, cfg, logger)
}

func parseFlags() Config {
	c := Config{}
	flag.StringVar(&c.InputDir, "input", "", "Directory containing cdc-*.jsonl files")
	flag.StringVar(&c.ArchiveDir, "archive", "archive", "Directory to move processed files")
	flag.StringVar(&c.DBPath, "db", "replica.duckdb", "DuckDB path")
	flag.StringVar(&c.MetricsAddr, "metrics", ":9090", "Metrics HTTP address")
	flag.DurationVar(&c.PollInterval, "poll", 1*time.Second, "Polling interval for new files")
	flag.DurationVar(&c.MinFileAge, "min-age", 1*time.Second, "Minimum age for .jsonl files to ensure FS consistency")
	flag.IntVar(&c.VacuumInterval, "vacuum-interval", 50, "Run VACUUM ANALYZE after processing this many files (0 to disable)")
	flag.BoolVar(&c.UnsafeMode, "unsafe", false, "Enable unsafe mode (synchronous=off)")
	flag.Parse()
	return c
}

func initSchema(db *sql.DB, unsafe bool) error {
	pragmas := []string{
		"PRAGMA memory_limit='1GB';",
	}
	if unsafe {
		pragmas = append(pragmas, "PRAGMA wal_autocheckpoint='1GB';")
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return err
		}
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS file_checkpoints (filename TEXT PRIMARY KEY, processed_at TIMESTAMP)`); err != nil {
		return err
	}

	// Data Table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS turnstone_data (key TEXT PRIMARY KEY, value TEXT, log_seq UBIGINT, tx_id UBIGINT, updated_at TIMESTAMP)`)
	return err
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

// --- File Ingestion Logic ---

func runFileIngestion(ctx context.Context, db *sql.DB, cfg Config, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	filesSinceVacuum := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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

			if len(candidates) == 0 {
				continue
			}

			// Sort to process in chronological order
			sort.Strings(candidates)

			for _, fname := range candidates {
				if ctx.Err() != nil {
					return
				}

				if isProcessed(db, fname) {
					os.Rename(filepath.Join(cfg.InputDir, fname), filepath.Join(cfg.ArchiveDir, fname))
					continue
				}

				start := time.Now()
				if err := processFileBulk(db, cfg, fname, logger); err != nil {
					logger.Error("Failed to process file", "file", fname, "err", err)
					break // Stop processing to maintain order
				}
				duration := time.Since(start)

				src := filepath.Join(cfg.InputDir, fname)
				dst := filepath.Join(cfg.ArchiveDir, fname)
				if err := os.Rename(src, dst); err != nil {
					logger.Error("Failed to move file to archive", "file", fname, "err", err)
				}
				metricFilesProcessed.Inc()
				logger.Info("File processed", "file", fname, "duration_sec", duration.Seconds())

				// Vacuum Check
				filesSinceVacuum++
				if cfg.VacuumInterval > 0 && filesSinceVacuum >= cfg.VacuumInterval {
					logger.Info("Starting VACUUM ANALYZE...")
					vacStart := time.Now()
					if _, err := db.Exec("VACUUM ANALYZE"); err != nil {
						logger.Error("Vacuum failed", "err", err)
					} else {
						logger.Info("VACUUM ANALYZE complete", "duration_sec", time.Since(vacStart).Seconds())
					}
					filesSinceVacuum = 0
				}
			}
		}
	}
}

func isProcessed(db *sql.DB, fname string) bool {
	var exists int
	err := db.QueryRow("SELECT 1 FROM file_checkpoints WHERE filename = ?", fname).Scan(&exists)
	return err == nil
}

// processFileBulk loads JSON, deduplicates within the batch, and applies changes conditionally based on Sequence ID.
func processFileBulk(db *sql.DB, cfg Config, fname string, logger *slog.Logger) error {
	path := filepath.Join(cfg.InputDir, fname)

	// Use absolute path for safety with DuckDB internal calls
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("filepath error: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Load JSON -> Raw Temp Table
	// 'ignore_errors=true' allows us to read files that were truncated (half-written) during a crash.
	// Using 'v: JSON' allows us to store arbitrary JSON objects or strings efficiently.
	if _, err := tx.Exec(`DROP TABLE IF EXISTS batch_raw`); err != nil {
		return err
	}

	loadQuery := fmt.Sprintf(`
		CREATE TEMP TABLE batch_raw AS
		SELECT * FROM read_json('%s',
			columns={k: 'TEXT', v: 'JSON', del: 'BOOLEAN', seq: 'UBIGINT', tx: 'UBIGINT', ts: 'BIGINT'},
			format='newline_delimited',
			ignore_errors=true
		)`, absPath)

	if _, err := tx.Exec(loadQuery); err != nil {
		return fmt.Errorf("bulk load error: %w", err)
	}

	// 2. Deduplicate -> Deduped Temp Table
	if _, err := tx.Exec(`DROP TABLE IF EXISTS batch_dedup`); err != nil {
		return err
	}

	if _, err := tx.Exec(`
		CREATE TEMP TABLE batch_dedup AS
		SELECT k, v, del, seq, tx, ts
		FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY seq DESC) as rn
			FROM batch_raw
		) WHERE rn = 1
	`); err != nil {
		return fmt.Errorf("dedup error: %w", err)
	}

	// 3. Conditional DELETE
	resDel, err := tx.Exec(`
		DELETE FROM turnstone_data
		USING batch_dedup
		WHERE turnstone_data.key = batch_dedup.k
		  AND batch_dedup.del = true
		  AND batch_dedup.seq > turnstone_data.log_seq
	`)
	if err != nil {
		return fmt.Errorf("bulk delete error: %w", err)
	}

	rowsDel, _ := resDel.RowsAffected()
	if rowsDel > 0 {
		metricOps.WithLabelValues("bulk_delete").Add(float64(rowsDel))
	}

	// 4. Conditional UPSERT
	resUpsert, err := tx.Exec(`
		INSERT INTO turnstone_data (key, value, log_seq, tx_id, updated_at)
		SELECT k, v, seq, tx, to_timestamp(ts/1000.0)
		FROM batch_dedup
		WHERE del IS NULL OR del = false
		ON CONFLICT (key) DO UPDATE SET
			value      = EXCLUDED.value,
			log_seq    = EXCLUDED.log_seq,
			tx_id      = EXCLUDED.tx_id,
			updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.log_seq > turnstone_data.log_seq
	`)
	if err != nil {
		return fmt.Errorf("bulk upsert error: %w", err)
	}

	rowsUpsert, _ := resUpsert.RowsAffected()
	if rowsUpsert > 0 {
		metricOps.WithLabelValues("bulk_upsert").Add(float64(rowsUpsert))
	}

	// 5. Checkpoint
	if _, err := tx.Exec(`INSERT INTO file_checkpoints (filename, processed_at) VALUES (?, current_timestamp)`, fname); err != nil {
		return fmt.Errorf("checkpoint error: %w", err)
	}

	// Cleanup
	tx.Exec(`DROP TABLE batch_raw`)
	tx.Exec(`DROP TABLE batch_dedup`)

	if err := tx.Commit(); err != nil {
		return err
	}

	logger.Info("Bulk load complete", "file", fname, "deleted", rowsDel, "upserted", rowsUpsert)
	return nil
}

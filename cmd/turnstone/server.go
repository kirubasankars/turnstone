// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"turnstone/config"
	"turnstone/database"
	"turnstone/devtool"
	"turnstone/internal/tlsutil"
	"turnstone/metrics"
	"turnstone/repl"
	"turnstone/server"
)

func newServerCmd() *cobra.Command {
	var devMode bool
	var devtoolAddr string

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Run the TurnstoneDB server",
		Run: func(cmd *cobra.Command, args []string) {
			logLevel := slog.LevelInfo
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

			if _, err := os.Stat(homeDir); os.IsNotExist(err) {
				logger.Error("Home directory does not exist. Run 'turnstone init --home <path>' first.", "path", homeDir)
				os.Exit(1)
			}

			runServer(logger, devMode, devtoolAddr)
		},
	}

	cmd.Flags().BoolVar(&devMode, "dev", false, "Disable transaction timeouts and auto-promote all databases to PRIMARY")
	cmd.Flags().StringVar(&devtoolAddr, "devtool-addr", "", "Address for the devtool web UI (default 127.0.0.1:8080 when --dev is set)")

	return cmd
}

func runServer(logger *slog.Logger, devMode bool, devtoolAddr string) {
	if devMode {
		logger.Info("Starting in DEV mode: Transaction timeouts disabled, all DBs auto-promoted")
		if devtoolAddr == "" {
			devtoolAddr = "127.0.0.1:8080"
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	configPath := filepath.Join(homeDir, "turnstone.json")
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
	if err := config.ValidateConfig(cfg); err != nil {
		logger.Error("Invalid configuration", "err", err)
		os.Exit(1)
	}

	if cfg.Debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	if cfg.LogRetention == "" {
		cfg.LogRetention = "replication"
	}

	certFile := config.ResolvePath(homeDir, cfg.TLSCertFile)
	keyFile := config.ResolvePath(homeDir, cfg.TLSKeyFile)
	caFile := config.ResolvePath(homeDir, cfg.TLSCAFile)
	clientCertFile := config.ResolvePath(homeDir, cfg.TLSClientCertFile)
	clientKeyFile := config.ResolvePath(homeDir, cfg.TLSClientKeyFile)

	stores := make(map[string]*database.Database)
	var storesMu sync.Mutex

	openLimit := runtime.NumCPU()
	if openLimit < 1 {
		openLimit = 1
	}
	g, openCtx := errgroup.WithContext(ctx)
	g.SetLimit(openLimit)

	for i := 0; i <= cfg.NumberOfDatabases; i++ {
		name := strconv.Itoa(i)
		path := filepath.Join(homeDir, "data", name)
		dbLogger := logger.With("db", name)

		g.Go(func() error {
			if err := openCtx.Err(); err != nil {
				return err
			}
			dbLogger.Info("Opening database...")
			st, err := database.Open(openCtx, path, dbLogger, 0, cfg.LogRetention, cfg.MaxDiskUsagePercent)
			if err != nil {
				return fmt.Errorf("db %s: %w", name, err)
			}

			if devMode {
				st.SetMinReplicas(0)
				if err := st.Promote(); err != nil {
					_ = st.Close()
					return fmt.Errorf("db %s promote: %w", name, err)
				}
				dbLogger.Info("Dev Mode: Auto-promoted DB to PRIMARY")
			}

			storesMu.Lock()
			stores[name] = st
			storesMu.Unlock()
			dbLogger.Info("Database ready")
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		for _, st := range stores {
			_ = st.Close()
		}
		if errors.Is(err, context.Canceled) {
			logger.Info("Startup cancelled")
			os.Exit(130)
		}
		logger.Error("Failed to initialize stores", "err", err)
		os.Exit(1)
	}

	replTLS, err := tlsutil.LoadMTLS(caFile, clientCertFile, clientKeyFile)
	if err != nil {
		logger.Error("Failed to load replication TLS config", "err", err)
		os.Exit(1)
	}

	rm := repl.NewManager(cfg.ID, stores, replTLS, logger)

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
		devMode,
	)
	if err != nil {
		logger.Error("Failed to create server", "err", err)
		os.Exit(1)
	}

	if cfg.MetricsAddr != "" {
		metrics.StartMetricsServer(cfg.MetricsAddr, stores, srv, logger)
	}

	if devtoolAddr != "" {
		devtool.Start(devtool.Config{
			Addr:        devtoolAddr,
			MetricsAddr: cfg.MetricsAddr,
			Stores:      stores,
			ServerStats: srv,
			Logger:      logger,
		})
	}

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped unexpectedly", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownStart := time.Now()
	logger.Info("Shutting down...")

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		logger.Warn("Force exit")
		os.Exit(130)
	}()

	done := make(chan struct{})
	go func() {
		srv.CloseAll()
		close(done)
	}()
	const shutdownTimeout = 5 * time.Second
	select {
	case <-done:
		logger.Info("Shutdown complete", "elapsed_ms", time.Since(shutdownStart).Milliseconds())
	case <-time.After(shutdownTimeout):
		logger.Warn("Shutdown timeout, forcing exit", "timeout_ms", shutdownTimeout.Milliseconds())
		os.Exit(130)
	}
}

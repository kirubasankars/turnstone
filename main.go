package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	port := flag.String("port", DefaultPort, "Port")
	dir := flag.String("dir", DefaultDataDir, "Data dir")
	debugEnv := flag.Bool("debug", false, "Debug logs")
	maxConns := flag.Int("max-conns", 500, "Max connections")
	truncate := flag.Bool("truncate", false, "Repair corrupt file")
	skipCrc := flag.Bool("skip-crc", false, "Faster reads (unsafe)")
	requirePass := flag.String("requirepass", "", "Require password for access")

	// Fsync Configuration
	fsync := flag.Bool("fsync", true, "Enable fsync on every transaction commit (safest)")
	fsyncInterval := flag.Duration("fsync-interval", 500*time.Millisecond, "Background fsync interval if --fsync=false")

	flag.Parse()

	lvl := slog.LevelInfo
	if *debugEnv {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := NewStore(*dir, logger, *truncate, *skipCrc, *fsync, *fsyncInterval)
	if err != nil {
		logger.Error("Failed to init store", "err", err)
		os.Exit(1)
	}

	// Pass requirePass to NewServer
	srv := NewServer(*port, store, logger, *maxConns, *requirePass)

	errCh := make(chan error, 1)
	go func() {
		if err := srv.Run(ctx); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("Shutting down...")
		<-errCh
		if err := store.Close(); err != nil {
			logger.Error("Store close error", "err", err)
		}
	case err := <-errCh:
		if err != nil {
			logger.Error("Server error", "err", err)
			os.Exit(1)
		}
	}
}

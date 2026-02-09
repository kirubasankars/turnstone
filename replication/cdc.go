// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
	Partition   string
	StartID     uint64
	StateFile   string
	TextMode    bool
	MetricsAddr string
	Logger      *slog.Logger
}

// Event defines the JSON structure for CDC events.
type Event struct {
	Seq      uint64      `json:"seq"`           // Global Log Sequence
	Tx       uint64      `json:"tx"`            // Transaction ID
	Key      string      `json:"key"`           // Key
	Val      interface{} `json:"val,omitempty"` // Value
	IsDelete bool        `json:"del"`           // True if deletion
}

// Global metric for monitoring within the replication package
var currentSeq atomic.Uint64

// StartCDC initiates the Change Data Capture process.
// It handles state resumption, signal handling, and automatic retries.
func StartCDC(cfg CDCConfig) {
	// 1. Resume State
	lastSeq := cfg.StartID
	if content, err := os.ReadFile(cfg.StateFile); err == nil {
		if val, err := strconv.ParseUint(string(content), 10, 64); err == nil {
			lastSeq = val
			currentSeq.Store(lastSeq)
			fmt.Fprintf(os.Stderr, "Resuming from LogID %d (found in %s)\n", lastSeq, cfg.StateFile)
		}
	}

	// 2. Start Metrics Server
	go startMetricsServer(cfg.MetricsAddr)

	// 3. Setup Signal Handling
	ctx, cancel := context.WithCancel(context.Background())
	// Buffer of 2 to catch the second signal for force quit
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		// First signal: Graceful shutdown
		sig := <-sigCh
		fmt.Fprintf(os.Stderr, "\nReceived signal %s. Stopping... (Press Ctrl+C again to force quit)\n", sig)
		cancel()

		// Second signal: Hard exit
		<-sigCh
		fmt.Fprintln(os.Stderr, "\nForcing exit...")
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
				fmt.Fprintf(os.Stderr, "\n!!! FATAL REPLICATION ERROR !!!\n")
				fmt.Fprintf(os.Stderr, "Message: %v\n", err)
				fmt.Fprintf(os.Stderr, "Cause:   The server has purged the logs required to resume from ID %d.\n", lastSeq)
				fmt.Fprintf(os.Stderr, "Action:  Delete the state file '%s' to resync from the server's current head.\n", cfg.StateFile)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "CDC Disconnected: %v. Retrying in %v...\n", err, backoff)
		} else {
			fmt.Fprintf(os.Stderr, "Connection closed. Retrying in %v...\n", backoff)
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

	caPath := filepath.Join(cfg.Home, "certs", "ca.crt")
	certPath := filepath.Join(cfg.Home, "certs", "client.crt")
	keyPath := filepath.Join(cfg.Home, "certs", "client.key")

	if _, err := os.Stat(caPath); err == nil {
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

	return cl.Subscribe(cfg.Partition, startID, func(c client.Change) error {
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

		onProgress(c.LogSeq)

		// Atomic State Update
		tmpFile := cfg.StateFile + ".tmp"
		if err := os.WriteFile(tmpFile, []byte(strconv.FormatUint(c.LogSeq, 10)), 0o644); err == nil {
			_ = os.Rename(tmpFile, cfg.StateFile)
		}
		return nil
	})
}

func startMetricsServer(addr string) {
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
		fmt.Fprintf(os.Stderr, "Metrics server failed: %v\n", err)
	}
}

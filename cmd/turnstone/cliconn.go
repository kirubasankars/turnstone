// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"turnstone/client"
	"turnstone/internal/tlsutil"
)

func connectCLIClient(host string, debug bool, asAdmin bool) (*client.Client, error) {
	var logger *slog.Logger
	if debug {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	} else {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	role := tlsutil.RoleClient
	if asAdmin {
		role = tlsutil.RoleAdmin
	}

	caPath, certPath, keyPath := tlsutil.CertPaths(homeDir, role)

	var cl *client.Client
	var err error

	if _, statErr := os.Stat(caPath); statErr == nil {
		fmt.Printf("Connecting to %s via mTLS as %s (Home: %s)...\n", host, strings.ToUpper(string(role)), homeDir)
		cl, err = client.NewMTLSClientHelper(host, caPath, certPath, keyPath, logger)
	} else {
		fmt.Printf("Certificates not found at %s/certs. Connecting to %s via insecure TCP...\n", homeDir, host)
		cl, err = client.NewClient(client.Config{
			Address:        host,
			ConnectTimeout: 5 * time.Second,
			Logger:         logger,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if err := cl.Ping(); err != nil {
		cl.Close()
		return nil, fmt.Errorf("failed to ping server: %w", err)
	}

	return cl, nil
}

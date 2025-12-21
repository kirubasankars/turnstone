package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Config holds the application configuration
type Config struct {
	Port       string `json:"port"`
	Dir        string `json:"dir"`
	Debug      bool   `json:"debug"`
	MaxConns   int    `json:"max_conns"`
	MaxSyncs   int    `json:"max_syncs"`
	Fsync      bool   `json:"fsync"`
	Role       string `json:"role"`
	LeaderAddr string `json:"leader_addr"`
	CDCState   string `json:"cdc_state"`
	CDCBase64  bool   `json:"cdc_base64"`

	// Security (mTLS Mandatory)
	TLSCertFile       string `json:"tls_cert_file"`
	TLSKeyFile        string `json:"tls_key_file"`
	TLSCAFile         string `json:"tls_ca_file"`
	TLSClientCertFile string `json:"tls_client_cert_file"`
	TLSClientKeyFile  string `json:"tls_client_key_file"`

	// Sentinel / HA
	SentinelAddr string `json:"sentinel_addr"`

	// Monitoring
	MetricsAddr string `json:"metrics_addr"`
}

func main() {
	var homeDir string
	flag.StringVar(&homeDir, "home", "", "Home directory for configuration, data, and certificates (Required)")

	genConfig := flag.Bool("generate-config", false, "Generate a sample configuration file and certificates")
	flag.Parse()

	// Enforce required home directory
	if homeDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -home argument is required")
		flag.Usage()
		os.Exit(1)
	}

	// Helper to resolve paths relative to home directory
	// Enforces strict home containment
	resolvePath := func(path string) string {
		if path == "" {
			return homeDir
		}
		// Always join with homeDir to ensure containment, treating path as relative
		return filepath.Join(homeDir, path)
	}

	// Config file is strictly located at home/config.json
	resolvedConfigPath := filepath.Join(homeDir, "config.json")

	defaultCfg := Config{
		Port:              DefaultPort,
		Dir:               DefaultDataDir,
		Debug:             false,
		MaxConns:          500,
		MaxSyncs:          10,
		Fsync:             true,
		Role:              "leader",
		LeaderAddr:        "", // Explicitly included for visibility
		CDCState:          "cdc.state",
		CDCBase64:         false,
		TLSCertFile:       "certs/server.crt", // Defaults to standard certs structure
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		TLSClientCertFile: "certs/client.crt",
		TLSClientKeyFile:  "certs/client.key",
		SentinelAddr:      "",
		MetricsAddr:       "",
	}

	if *genConfig {
		// Ensure directory for config exists
		if err := os.MkdirAll(homeDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating home directory: %v\n", err)
			os.Exit(1)
		}

		// Create standard directory structure
		// This scaffolding ensures the user has the "whole" environment ready.
		// We use resolvePath to respect the -home flag.

		// 1. Data Directory
		if err := os.MkdirAll(resolvePath(defaultCfg.Dir), 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to create data directory: %v\n", err)
		}

		// 2. Certs Directory & Generation
		// Get directory relative to home (e.g., home/certs)
		certsDir := filepath.Dir(resolvePath(defaultCfg.TLSCertFile))
		if err := os.MkdirAll(certsDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to create certs directory: %v\n", err)
		}

		// Generate Certs immediately during config generation
		if err := generateCerts(certsDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating certs: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Certificates generated in: %s\n", certsDir)

		data, err := json.MarshalIndent(defaultCfg, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error generating config: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(resolvedConfigPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing config file: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Sample configuration written to %s\n", resolvedConfigPath)
		return
	}

	cfg := defaultCfg
	if fileData, err := os.ReadFile(resolvedConfigPath); err == nil {
		if err := json.Unmarshal(fileData, &cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing config file: %v\n", err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error reading config file: %v\n", err)
		os.Exit(1)
	} else {
		// Config not found. Proceed with defaults.
		// Ensure home exists before starting
		_ = os.MkdirAll(homeDir, 0o755)
	}

	// Apply path resolution to configuration fields to enforce containment
	cfg.Dir = resolvePath(cfg.Dir)
	cfg.CDCState = resolvePath(cfg.CDCState)
	cfg.TLSCertFile = resolvePath(cfg.TLSCertFile)
	cfg.TLSKeyFile = resolvePath(cfg.TLSKeyFile)
	cfg.TLSCAFile = resolvePath(cfg.TLSCAFile)
	cfg.TLSClientCertFile = resolvePath(cfg.TLSClientCertFile)
	cfg.TLSClientKeyFile = resolvePath(cfg.TLSClientKeyFile)

	lvl := slog.LevelInfo
	if cfg.Debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	if cfg.Role == "leader" || cfg.Role == "follower" {
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.TLSCAFile == "" {
			logger.Error("Security Critical: 'tls_cert_file', 'tls_key_file', and 'tls_ca_file' must be set.")
			os.Exit(1)
		}
	}

	if cfg.Role == "follower" || cfg.Role == "sentinel" || cfg.Role == "cdc-stdout" {
		if cfg.TLSClientCertFile == "" || cfg.TLSClientKeyFile == "" || cfg.TLSCAFile == "" {
			logger.Error("Security Critical: Client certificates must be set for client roles.")
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cfg.Role {
	case "leader":
		runServer(ctx, cfg, logger, false)
		<-ctx.Done()

	case "follower":
		if cfg.LeaderAddr == "" {
			logger.Error("Follower mode requires leader_addr in config")
			os.Exit(1)
		}
		// 1. Initialize Stores in Read-Only
		stores := runServer(ctx, cfg, logger, true)

		// 2. Start CDC Client for EACH database
		for i := 0; i < MaxDatabases; i++ {
			store := stores[i]
			dbIdx := i

			startGen, startOff, err := store.GetReplicationState()
			if err != nil {
				logger.Warn("Failed to load replication state", "db", dbIdx, "err", err)
			}

			logger.Info("Starting Replication", "db", dbIdx, "leader", cfg.LeaderAddr, "gen", startGen, "off", startOff)
			handler := NewReplicaHandler(store)

			client := NewCDCClient(cfg.LeaderAddr, handler, dbIdx, startGen, startOff, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)

			go func() {
				if err := client.Run(ctx); err != nil && err != context.Canceled {
					logger.Error("Replication failed", "db", dbIdx, "err", err)
					stop()
				}
			}()
		}

		<-ctx.Done()

	case "cdc-stdout":
		if cfg.LeaderAddr == "" {
			fmt.Println("Usage: config role=cdc-stdout requires leader_addr")
			os.Exit(1)
		}

		// For CDC-Stdout, we just replicate DB 0 by default, or maybe loop?
		// To keep it simple, we just replicate DB 0 for inspection.
		// Real CDC would likely need a flag to select DB.
		dbIdx := 0
		var startGen uint64
		var startOff int64
		// In multi-db, cdc.state would need to be db-specific.
		// Here we assume cdc.state maps to DB 0.
		if cfg.CDCState != "" {
			loadedGen, loadedOff, err := loadState(cfg.CDCState)
			if err == nil {
				startGen = loadedGen
				startOff = loadedOff
			}
		}

		if cfg.MetricsAddr != "" {
			StartMetricsServer(cfg.MetricsAddr, nil, logger)
		}

		handler := NewPipeHandler(cfg.CDCBase64, cfg.CDCState)
		client := NewCDCClient(cfg.LeaderAddr, handler, dbIdx, startGen, startOff, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)

		if err := client.Run(ctx); err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "CDC Error: %v\n", err)
			os.Exit(1)
		}

	case "sentinel":
		target := cfg.SentinelAddr
		if target == "" {
			host, port, err := net.SplitHostPort(cfg.Port)
			if err != nil {
				if strings.HasPrefix(cfg.Port, ":") {
					target = "127.0.0.1" + cfg.Port
				} else {
					target = cfg.Port
				}
			} else {
				if host == "" {
					host = "127.0.0.1"
				}
				target = net.JoinHostPort(host, port)
			}
		}

		err := runSentinelCheck(target, cfg.TLSClientCertFile, cfg.TLSClientKeyFile, cfg.TLSCAFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Sentinel Health Check Failed: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)

	default:
		fmt.Println("Unknown role.")
		os.Exit(1)
	}
}

// runServer initializes stores and starts the Server. Returns the list of stores.
func runServer(ctx context.Context, cfg Config, logger *slog.Logger, readOnly bool) []*Store {
	var stores []*Store

	// Initialize 16 Databases
	for i := 0; i < MaxDatabases; i++ {
		dbPath := filepath.Join(cfg.Dir, strconv.Itoa(i))
		store, err := NewStore(dbPath, logger, false, false, cfg.Fsync, 500*time.Millisecond)
		if err != nil {
			logger.Error("Failed to init store", "db", i, "err", err)
			os.Exit(1)
		}
		stores = append(stores, store)
	}

	srv := NewServer(cfg.Port, cfg.MetricsAddr, stores, logger, cfg.MaxConns, cfg.MaxSyncs, readOnly, cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)

	go func() {
		if err := srv.Run(ctx); err != nil {
			logger.Error("Server stopped", "err", err)
		}
		for _, s := range stores {
			s.Close()
		}
	}()

	return stores
}

func loadState(path string) (uint64, int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	var s CheckpointState
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, 0, err
	}
	return s.Generation, s.Offset, nil
}

func generateCerts(outDir string) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	// Helper to write PEM files
	writePEM := func(filename string, typeStr string, bytes []byte) error {
		path := filepath.Join(outDir, filename)
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return pem.Encode(f, &pem.Block{Type: typeStr, Bytes: bytes})
	}

	// 1. CA
	caPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"TurnstoneDB CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caBytes, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caPriv.PublicKey, caPriv)
	if err != nil {
		return err
	}
	if err := writePEM("ca.crt", "CERTIFICATE", caBytes); err != nil {
		return err
	}

	// 2. Server Cert
	srvPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	srvTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"TurnstoneDB Server"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}
	srvBytes, err := x509.CreateCertificate(rand.Reader, &srvTemplate, &caTemplate, &srvPriv.PublicKey, caPriv)
	if err != nil {
		return err
	}
	if err := writePEM("server.crt", "CERTIFICATE", srvBytes); err != nil {
		return err
	}
	if err := writePEM("server.key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(srvPriv)); err != nil {
		return err
	}

	// 3. Client Cert
	clientPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization: []string{"TurnstoneDB Client"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientBytes, err := x509.CreateCertificate(rand.Reader, &clientTemplate, &caTemplate, &clientPriv.PublicKey, caPriv)
	if err != nil {
		return err
	}
	if err := writePEM("client.crt", "CERTIFICATE", clientBytes); err != nil {
		return err
	}
	if err := writePEM("client.key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientPriv)); err != nil {
		return err
	}

	return nil
}

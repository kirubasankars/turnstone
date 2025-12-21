package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
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

// ResolvePath resolves a path relative to the home directory if it's not absolute.
func ResolvePath(homeDir, path string) string {
	if path == "" {
		return homeDir
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(homeDir, path)
}

func validateSecurityConfig(cfg Config, logger *slog.Logger) {
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
}

func generateConfigArtifacts(homeDir string, defaultCfg Config, configPath string) {
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating home directory: %v\n", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(ResolvePath(homeDir, defaultCfg.Dir), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to create data directory: %v\n", err)
	}

	certsDir := filepath.Dir(ResolvePath(homeDir, defaultCfg.TLSCertFile))
	if err := os.MkdirAll(certsDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to create certs directory: %v\n", err)
	}

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
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing config file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Sample configuration written to %s\n", configPath)
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

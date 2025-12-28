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

// Config represents the server configuration loaded from a JSON file.
type Config struct {
	// Port is the TCP port the server listens on (e.g., ":6379").
	Port string `json:"port"`

	// Debug enables verbose logging if true.
	Debug bool `json:"debug"`

	// MaxConns limits the number of concurrent client connections.
	MaxConns int `json:"max_conns"`

	// Fsync controls whether to call fsync() on the WAL after every batch write.
	// Setting this to true guarantees durability but increases latency.
	Fsync bool `json:"fsync"`

	// AllowRecoveryTruncate determines if the server should silently truncate
	// corrupted trailing bytes in the WAL during startup recovery.
	AllowRecoveryTruncate bool `json:"allow_recovery_truncate"`

	// IndexType specifies the backend for the index ("memory", "sharded", or "leveldb").
	// Default is "leveldb".
	IndexType string `json:"index_type"`

	// TLSCertFile is the path to the server's TLS certificate.
	TLSCertFile string `json:"tls_cert_file"`

	// TLSKeyFile is the path to the server's private key.
	TLSKeyFile string `json:"tls_key_file"`

	// TLSCAFile is the path to the Certificate Authority file used to verify client certificates (mTLS).
	TLSCAFile string `json:"tls_ca_file"`

	// TLSClientCertFile is the path to the client certificate used for replication (follower mode).
	TLSClientCertFile string `json:"tls_client_cert_file"`

	// TLSClientKeyFile is the path to the client key used for replication (follower mode).
	TLSClientKeyFile string `json:"tls_client_key_file"`

	// MetricsAddr is the address to bind the Prometheus metrics server (e.g., ":9090").
	MetricsAddr string `json:"metrics_addr"`

	// Role indicates the server role ("leader" or "follower").
	// Note: This is largely kept for compatibility; actual behavior is determined by ReplicaOf.
	Role string `json:"role"`

	// ReplicaOf, if set, specifies the address of the leader node this server should replicate from.
	ReplicaOf string `json:"replica_of"`

	// MinReplicas specifies the number of replicas that must acknowledge a write
	// before it is considered committed. 0 disables synchronous replication.
	MinReplicas int `json:"min_replicas"`
}

// ResolvePath returns an absolute path.
// If path is relative, it is joined with homeDir. If path is already absolute, it is returned as is.
func ResolvePath(homeDir, path string) string {
	if path == "" {
		return homeDir
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(homeDir, path)
}

// validateSecurityConfig ensures that critical security parameters are present.
// It terminates the application if TLS certificates are missing.
func validateSecurityConfig(cfg Config, logger *slog.Logger) {
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.TLSCAFile == "" {
		logger.Error("Security Critical: 'tls_cert_file', 'tls_key_file', and 'tls_ca_file' must be set.")
		os.Exit(1)
	}
}

// generateConfigArtifacts creates a sample directory structure, configuration file,
// and self-signed certificates for development purposes.
func generateConfigArtifacts(homeDir string, defaultCfg Config, configPath string) {
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating home directory: %v\n", err)
		os.Exit(1)
	}

	// Create hardcoded data directory
	if err := os.MkdirAll(ResolvePath(homeDir, "data"), 0o755); err != nil {
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

// generateCerts creates a self-signed CA, a server certificate, and a client certificate.
// This is intended for testing/development to easily enable mTLS.
func generateCerts(outDir string) error {
	writePEM := func(filename string, typeStr string, bytes []byte) error {
		path := filepath.Join(outDir, filename)
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return pem.Encode(f, &pem.Block{Type: typeStr, Bytes: bytes})
	}

	// 1. Generate CA
	caPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	caTemplate := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"TurnstoneDB CA"}},
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

	// 2. Generate Server Cert
	srvPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	srvTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"TurnstoneDB Server"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
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

	// 3. Generate Client Cert
	clientPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{Organization: []string{"TurnstoneDB Client"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
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

package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Config represents the server configuration.
// Simplified for in-memory mode.
type Config struct {
	ID                   string `json:"id"` // Unique identifier for this instance
	Port                 string `json:"port"`
	Debug                bool   `json:"debug"`
	MaxConns             int    `json:"max_conns"`
	TLSCertFile          string `json:"tls_cert_file"`
	TLSKeyFile           string `json:"tls_key_file"`
	TLSCAFile            string `json:"tls_ca_file"`
	TLSClientCertFile    string `json:"tls_client_cert_file"`
	TLSClientKeyFile     string `json:"tls_client_key_file"`
	MetricsAddr          string `json:"metrics_addr"`
	NumberOfDatabases    int    `json:"number_of_databases"`
	WALRetention         string `json:"wal_retention"`          // Duration string e.g. "2h"
	WALRetentionStrategy string `json:"wal_retention_strategy"` // "time" or "replication"
	MaxDiskUsagePercent  int    `json:"max_disk_usage_percent"`
	BlockCacheSize       string `json:"block_cache_size"` // Size string e.g. "64MB"
}

// ResolvePath returns an absolute path relative to the home directory if strictly necessary.
func ResolvePath(homeDir, path string) string {
	if path == "" {
		return homeDir
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(homeDir, path)
}

// ValidateSecurityConfig ensures that critical security parameters are present.
func ValidateSecurityConfig(cfg Config) error {
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" || cfg.TLSCAFile == "" {
		return fmt.Errorf("security critical: 'tls_cert_file', 'tls_key_file', and 'tls_ca_file' must be set")
	}
	return nil
}

// GenerateConfigArtifacts creates a sample directory structure and certificates.
// Changed extraHosts to variadic to maintain backward compatibility with existing tests.
func GenerateConfigArtifacts(homeDir string, defaultCfg Config, configPath string, extraHosts ...string) error {
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		return fmt.Errorf("error creating home directory: %w", err)
	}

	for _, d := range []string{"certs"} {
		if err := os.MkdirAll(ResolvePath(homeDir, d), 0o755); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", d, err)
		}
	}

	// Create data directories for databases.
	// Database 0 is reserved for internal purposes.
	// If NumberOfDatabases is N, we create databases 0, 1, ..., N.
	for i := 0; i <= defaultCfg.NumberOfDatabases; i++ {
		dbID := strconv.Itoa(i)
		dbPath := filepath.Join(homeDir, "data", dbID)
		if err := os.MkdirAll(dbPath, 0o755); err != nil {
			return fmt.Errorf("failed to create data directory for database %s: %w", dbID, err)
		}
	}

	certsDir := filepath.Dir(ResolvePath(homeDir, defaultCfg.TLSCertFile))
	if err := generateCerts(certsDir, extraHosts); err != nil {
		return fmt.Errorf("error generating certs: %w", err)
	}
	fmt.Printf("Certificates generated in: %s\n", certsDir)

	// Update default config to use server cert for replication client (needs high privs)
	// instead of the restricted 'client' cert.
	defaultCfg.TLSClientCertFile = "certs/server.crt"
	defaultCfg.TLSClientKeyFile = "certs/server.key"
	defaultCfg.WALRetention = "2h" // Default retention
	if defaultCfg.WALRetentionStrategy == "" {
		defaultCfg.WALRetentionStrategy = "time"
	}
	// Default to 90% if not set (0 is treated as disabled, so we set explicit default here if needed)
	if defaultCfg.MaxDiskUsagePercent == 0 {
		defaultCfg.MaxDiskUsagePercent = 90
	}
	// Default Block Cache
	if defaultCfg.BlockCacheSize == "" {
		defaultCfg.BlockCacheSize = "64MB"
	}

	// Set default ID if not provided
	if defaultCfg.ID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "server"
		}
		defaultCfg.ID = fmt.Sprintf("%s-%d", hostname, time.Now().Unix())
	}

	data, err := json.MarshalIndent(defaultCfg, "", "  ")
	if err != nil {
		return fmt.Errorf("error generating config json: %w", err)
	}
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		return fmt.Errorf("error writing config file: %w", err)
	}
	fmt.Printf("Sample configuration written to %s\n", configPath)
	return nil
}

func generateCerts(outDir string, extraHosts []string) error {
	writePEM := func(filename, typeStr string, bytes []byte) error {
		path := filepath.Join(outDir, filename)
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return pem.Encode(f, &pem.Block{Type: typeStr, Bytes: bytes})
	}

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

	genLeaf := func(role string, sn int64, hosts []string) error {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return err
		}
		// Organization field used for Role Based Access Control (RBAC)
		tmpl := x509.Certificate{
			SerialNumber: big.NewInt(sn),
			Subject:      pkix.Name{Organization: []string{"TurnstoneDB " + role}, CommonName: role},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(365 * 24 * time.Hour),
			KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			DNSNames:     []string{},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		}

		// Correctly categorize hosts into IPs or DNS Names
		for _, h := range hosts {
			if ip := net.ParseIP(h); ip != nil {
				tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			} else {
				tmpl.DNSNames = append(tmpl.DNSNames, h)
			}
		}

		b, err := x509.CreateCertificate(rand.Reader, &tmpl, &caTemplate, &priv.PublicKey, caPriv)
		if err != nil {
			return err
		}
		if err := writePEM(role+".crt", "CERTIFICATE", b); err != nil {
			return err
		}
		return writePEM(role+".key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv))
	}

	// Always include localhost + any extra hosts provided via CLI
	serverHosts := []string{"localhost"}
	serverHosts = append(serverHosts, extraHosts...)

	if err := genLeaf("server", 2, serverHosts); err != nil {
		return err
	}
	if err := genLeaf("client", 3, nil); err != nil {
		return err
	}
	if err := genLeaf("admin", 4, nil); err != nil {
		return err
	}
	return genLeaf("cdc", 5, nil)
}

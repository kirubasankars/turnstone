// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

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
	WALRetentionStrategy string `json:"wal_retention_strategy"` // "replication" or "checkpoint"
	MaxDiskUsagePercent  int    `json:"max_disk_usage_percent"`
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

// ValidateConfig sanity-checks user-supplied config values that would
// otherwise be accepted silently and then misbehave downstream. In
// particular, MaxDiskUsagePercent is compared directly against a 0-100
// disk-usage percentage (stonedb.DB.runDiskMonitor); only 0 is special
// (disables the monitor entirely) -- anything negative silently disables
// the monitor too (since the "is it positive" gate never passes), and
// anything above 100 makes the "usage > limit" comparison permanently
// false, silently turning off the disk-full write guard instead of the
// hard startup failure an obviously-wrong value should produce.
func ValidateConfig(cfg Config) error {
	if cfg.MaxDiskUsagePercent < 0 || cfg.MaxDiskUsagePercent > 100 {
		return fmt.Errorf("max_disk_usage_percent must be between 0 and 100 (0 disables the check), got %d", cfg.MaxDiskUsagePercent)
	}
	return nil
}

// GenerateConfigArtifacts creates a sample directory structure and certificates.
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
	// DB 0 is a normal database now.
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
	defaultCfg.TLSClientCertFile = "certs/server.crt"
	defaultCfg.TLSClientKeyFile = "certs/server.key"

	if defaultCfg.WALRetentionStrategy == "" {
		defaultCfg.WALRetentionStrategy = "replication"
	}
	// Default to 90% if not set (0 is treated as disabled, so we set explicit default here if needed)
	if defaultCfg.MaxDiskUsagePercent == 0 {
		defaultCfg.MaxDiskUsagePercent = 90
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
	// perm is explicit per-file: certificates are public and stay at the
	// usual 0644, but private keys must not be group/world-readable.
	// os.Create() always opens at 0666 (masked by umask), which on a
	// typical 022 umask yields 0644 for keys too -- letting any other
	// local user/process read the mTLS private keys these files hold.
	writePEM := func(filename, typeStr string, bytes []byte, perm os.FileMode) error {
		path := filepath.Join(outDir, filename)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
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
	if err := writePEM("ca.crt", "CERTIFICATE", caBytes, 0o644); err != nil {
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
		if err := writePEM(role+".crt", "CERTIFICATE", b, 0o644); err != nil {
			return err
		}
		return writePEM(role+".key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv), 0o600)
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

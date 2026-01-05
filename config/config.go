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
	"time"
)

// Config represents the server configuration.
// Simplified for in-memory mode.
type Config struct {
	Port               string `json:"port"`
	Debug              bool   `json:"debug"`
	MaxConns           int    `json:"max_conns"`
	TLSCertFile        string `json:"tls_cert_file"`
	TLSKeyFile         string `json:"tls_key_file"`
	TLSCAFile          string `json:"tls_ca_file"`
	TLSClientCertFile  string `json:"tls_client_cert_file"`
	TLSClientKeyFile   string `json:"tls_client_key_file"`
	MetricsAddr        string `json:"metrics_addr"`
	NumberOfPartitions int    `json:"number_of_partitions"`
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
func GenerateConfigArtifacts(homeDir string, defaultCfg Config, configPath string) error {
	if err := os.MkdirAll(homeDir, 0o755); err != nil {
		return fmt.Errorf("error creating home directory: %w", err)
	}

	for _, d := range []string{"certs"} {
		if err := os.MkdirAll(ResolvePath(homeDir, d), 0o755); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", d, err)
		}
	}

	certsDir := filepath.Dir(ResolvePath(homeDir, defaultCfg.TLSCertFile))
	if err := generateCerts(certsDir); err != nil {
		return fmt.Errorf("error generating certs: %w", err)
	}
	fmt.Printf("Certificates generated in: %s\n", certsDir)

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

func generateCerts(outDir string) error {
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

	genLeaf := func(name string, sn int64, hosts []string) error {
		priv, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return err
		}
		tmpl := x509.Certificate{
			SerialNumber: big.NewInt(sn),
			Subject:      pkix.Name{Organization: []string{"TurnstoneDB " + name}},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(365 * 24 * time.Hour),
			KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			DNSNames:     hosts,
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		}
		b, err := x509.CreateCertificate(rand.Reader, &tmpl, &caTemplate, &priv.PublicKey, caPriv)
		if err != nil {
			return err
		}
		if err := writePEM(name+".crt", "CERTIFICATE", b); err != nil {
			return err
		}
		return writePEM(name+".key", "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(priv))
	}

	if err := genLeaf("server", 2, []string{"localhost"}); err != nil {
		return err
	}
	return genLeaf("client", 3, nil)
}

package config

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestResolvePath verifies absolute and relative path resolution logic.
func TestResolvePath(t *testing.T) {
	home := "/app/home"

	tests := []struct {
		name     string
		homeDir  string
		path     string
		expected string
	}{
		{
			name:     "Empty Path",
			homeDir:  home,
			path:     "",
			expected: home,
		},
		{
			name:     "Absolute Path",
			homeDir:  home,
			path:     "/etc/config",
			expected: "/etc/config",
		},
		{
			name:     "Relative Path",
			homeDir:  home,
			path:     "data/db",
			expected: filepath.Join(home, "data/db"),
		},
		{
			name:     "Dot Path",
			homeDir:  home,
			path:     ".",
			expected: home,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolvePath(tt.homeDir, tt.path)
			if got != tt.expected {
				t.Errorf("ResolvePath(%q, %q) = %q; want %q", tt.homeDir, tt.path, got, tt.expected)
			}
		})
	}
}

// TestGenerateConfigArtifacts verifies that the helper function correctly creates
// the directory structure, configuration file, and TLS certificates.
func TestGenerateConfigArtifacts(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	defaultCfg := Config{
		Port:               ":9999",
		TLSCertFile:        "certs/server.crt", // Relative paths
		TLSKeyFile:         "certs/server.key",
		TLSCAFile:          "certs/ca.crt",
		NumberOfPartitions: 1, // Should create partition 0 and 1
	}

	// Execute
	if err := GenerateConfigArtifacts(tmpDir, defaultCfg, configPath); err != nil {
		t.Fatalf("GenerateConfigArtifacts failed: %v", err)
	}

	// 1. Verify Directories
	// Partition 0 (Internal)
	if _, err := os.Stat(filepath.Join(tmpDir, "data", "0")); os.IsNotExist(err) {
		t.Error("Data directory for partition '0' not created")
	}
	// Partition 1 (User)
	if _, err := os.Stat(filepath.Join(tmpDir, "data", "1")); os.IsNotExist(err) {
		t.Error("Data directory for partition '1' not created")
	}

	if _, err := os.Stat(filepath.Join(tmpDir, "certs")); os.IsNotExist(err) {
		t.Error("Certs directory not created")
	}

	// 2. Verify Config File
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("Failed to read config file: %v", err)
	}
	var loadedCfg Config
	if err := json.Unmarshal(data, &loadedCfg); err != nil {
		t.Fatalf("Failed to parse generated config: %v", err)
	}
	if loadedCfg.Port != ":9999" {
		t.Errorf("Config mismatch. Want :9999, got %s", loadedCfg.Port)
	}
	if loadedCfg.NumberOfPartitions != 1 {
		t.Error("NumberOfPartitions mismatch")
	}

	// 3. Verify Certificates
	requiredCerts := []string{
		"certs/ca.crt",
		"certs/server.crt",
		"certs/server.key",
		"certs/client.crt",
		"certs/client.key",
		"certs/admin.crt",
		"certs/admin.key",
		"certs/cdc.crt",
		"certs/cdc.key",
	}
	for _, f := range requiredCerts {
		path := filepath.Join(tmpDir, f)
		info, err := os.Stat(path)
		if os.IsNotExist(err) {
			t.Errorf("Certificate artifact missing: %s", f)
		} else if info.Size() == 0 {
			t.Errorf("Certificate artifact is empty: %s", f)
		}
	}
}

// TestRBACCertificates verifies that generated certificates have correct Roles in Organization field.
func TestRBACCertificates(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	defaultCfg := Config{TLSCertFile: "certs/server.crt", TLSKeyFile: "certs/server.key", TLSCAFile: "certs/ca.crt"}

	if err := GenerateConfigArtifacts(tmpDir, defaultCfg, configPath); err != nil {
		t.Fatalf("Generate failed: %v", err)
	}

	tests := []struct {
		file         string
		expectedRole string
	}{
		{"certs/client.crt", "TurnstoneDB client"},
		{"certs/admin.crt", "TurnstoneDB admin"},
		{"certs/cdc.crt", "TurnstoneDB cdc"},
		{"certs/server.crt", "TurnstoneDB server"},
	}

	for _, tt := range tests {
		t.Run(tt.expectedRole, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join(tmpDir, tt.file))
			if err != nil {
				t.Fatalf("Read cert failed: %v", err)
			}
			block, _ := pem.Decode(content)
			if block == nil {
				t.Fatal("PEM decode failed")
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				t.Fatalf("Parse cert failed: %v", err)
			}

			if len(cert.Subject.Organization) == 0 {
				t.Error("Certificate has no Organization field")
			} else {
				got := cert.Subject.Organization[0]
				if got != tt.expectedRole {
					t.Errorf("Role mismatch in %s. Got '%s', want '%s'", tt.file, got, tt.expectedRole)
				}
			}

			// Verify key usage includes ClientAuth for clients/admin/cdc
			if !strings.Contains(tt.file, "server.crt") { // Server can be both, but strictly checking clients here
				hasClientAuth := false
				for _, usage := range cert.ExtKeyUsage {
					if usage == x509.ExtKeyUsageClientAuth {
						hasClientAuth = true
					}
				}
				if !hasClientAuth {
					t.Errorf("%s missing ExtKeyUsageClientAuth", tt.file)
				}
			}
		})
	}
}

// TestValidateSecurityConfig checks valid/invalid config validation.
func TestValidateSecurityConfig(t *testing.T) {
	validCfg := Config{
		TLSCertFile: "cert",
		TLSKeyFile:  "key",
		TLSCAFile:   "ca",
	}
	if err := ValidateSecurityConfig(validCfg); err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	badCfg := Config{
		TLSCertFile: "", // Missing
		TLSKeyFile:  "key",
		TLSCAFile:   "ca",
	}
	if err := ValidateSecurityConfig(badCfg); err == nil {
		t.Error("Expected error for missing cert file, got nil")
	}
}

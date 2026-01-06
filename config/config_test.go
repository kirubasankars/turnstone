package config

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// TestValidateSecurityConfig verifies that valid configs pass silently.
func TestValidateSecurityConfig_Success(t *testing.T) {
	validCfg := Config{
		TLSCertFile: "cert",
		TLSKeyFile:  "key",
		TLSCAFile:   "ca",
	}
	if err := ValidateSecurityConfig(validCfg); err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}
}

// TestValidateSecurityConfig_Error verifies that the function returns an error
// if critical security config is missing.
func TestValidateSecurityConfig_Error(t *testing.T) {
	badCfg := Config{
		TLSCertFile: "", // Missing
		TLSKeyFile:  "key",
		TLSCAFile:   "ca",
	}
	if err := ValidateSecurityConfig(badCfg); err == nil {
		t.Error("Expected error for missing cert file, got nil")
	}
}

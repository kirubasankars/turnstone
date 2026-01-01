package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
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
		Port:        ":9999",
		TLSCertFile: "certs/server.crt", // Relative paths
		TLSKeyFile:  "certs/server.key",
		TLSCAFile:   "certs/ca.crt",
	}

	// Capture stdout/stderr to avoid polluting test logs
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	os.Stdout, _ = os.Open(os.DevNull)
	os.Stderr, _ = os.Open(os.DevNull)
	defer func() {
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	// Execute
	generateConfigArtifacts(tmpDir, defaultCfg, configPath)

	// 1. Verify Directories
	if _, err := os.Stat(filepath.Join(tmpDir, "data")); os.IsNotExist(err) {
		t.Error("Data directory not created")
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
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	validCfg := Config{
		TLSCertFile: "cert",
		TLSKeyFile:  "key",
		TLSCAFile:   "ca",
	}
	// Should not panic or exit
	validateSecurityConfig(validCfg, logger)
}

// TestValidateSecurityConfig_Exit verifies that the application exits with status 1
// if critical security config is missing.
// We use a subprocess to test os.Exit.
func TestValidateSecurityConfig_Exit(t *testing.T) {
	if os.Getenv("BE_CRASHER") == "1" {
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		badCfg := Config{
			TLSCertFile: "", // Missing
			TLSKeyFile:  "key",
			TLSCAFile:   "ca",
		}
		validateSecurityConfig(badCfg, logger)
		return
	}

	// Re-run the test process with the special environment variable
	cmd := exec.Command(os.Args[0], "-test.run=TestValidateSecurityConfig_Exit")
	cmd.Env = append(os.Environ(), "BE_CRASHER=1")
	err := cmd.Run()

	// Check exit code
	if e, ok := err.(*exec.ExitError); ok && !e.Success() {
		return // Success: the process exited with non-zero code
	}
	t.Fatalf("Process ran with err %v, expected exit status 1", err)
}

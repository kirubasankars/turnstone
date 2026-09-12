// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

// Role identifies which client certificate to load from a Turnstone home directory.
type Role string

const (
	RoleClient Role = "client"
	RoleAdmin  Role = "admin"
)

// CertPaths returns CA, certificate, and key paths for a role under home/certs/.
func CertPaths(home string, role Role) (caFile, certFile, keyFile string) {
	certsDir := filepath.Join(home, "certs")
	return filepath.Join(certsDir, "ca.crt"),
		filepath.Join(certsDir, string(role)+".crt"),
		filepath.Join(certsDir, string(role)+".key")
}

// LoadMTLS loads a client TLS configuration from PEM files.
func LoadMTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate PEM from %s", caFile)
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load keypair %s/%s: %w", certFile, keyFile, err)
	}

	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, nil
}

// LoadFromHome loads mTLS credentials for a role from a Turnstone home directory.
func LoadFromHome(home string, role Role) (*tls.Config, error) {
	caFile, certFile, keyFile := CertPaths(home, role)
	return LoadMTLS(caFile, certFile, keyFile)
}

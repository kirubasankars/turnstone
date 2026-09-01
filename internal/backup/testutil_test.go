// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"compress/gzip"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/config"
	"turnstone/engine"
)

func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	if err := config.GenerateConfigArtifacts(dir, config.Config{
		TLSCertFile:       "certs/server.crt",
		TLSKeyFile:        "certs/server.key",
		TLSCAFile:         "certs/ca.crt",
		NumberOfDatabases: 1,
	}, filepath.Join(dir, "config.json")); err != nil {
		t.Fatal(err)
	}
	certsDir := filepath.Join(dir, "certs")
	serverCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "server.crt"), filepath.Join(certsDir, "server.key"))
	caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &tls.Config{Certificates: []tls.Certificate{serverCert}, RootCAs: pool, InsecureSkipVerify: true}
}

func writeEngineWAL(t *testing.T, keys ...string) []byte {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		tx := db.NewTransaction(true)
		if err := tx.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	seg, endOff, err := db.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_ = endOff
	return seg
}

func writeBackupArtifact(t *testing.T, dir, dbName string, typ string, baseLSN, endLSN uint64, wal []byte, compressed bool, parentSHA string) Meta {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}

	hasher := sha256.New()
	walPath := ResolveWALFile(dir, DefaultWALFile, compressed)
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if compressed {
		gw := gzip.NewWriter(ioMultiWriter(f, hasher))
		if _, err := gw.Write(wal); err != nil {
			t.Fatal(err)
		}
		if err := gw.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := f.Write(wal); err != nil {
			t.Fatal(err)
		}
		hasher.Write(wal)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	meta := Meta{
		Timestamp:    time.Now(),
		Database:     dbName,
		Type:         typ,
		BaseLSN:      baseLSN,
		EndLSN:       endLSN,
		ParentSHA256: parentSHA,
		Compressed:   compressed,
		SHA256:       hex.EncodeToString(hasher.Sum(nil)),
	}
	if err := SaveMeta(dir, meta); err != nil {
		t.Fatal(err)
	}
	return meta
}

type multiWriter struct {
	writers []interface{ Write([]byte) (int, error) }
}

func ioMultiWriter(writers ...interface{ Write([]byte) (int, error) }) *multiWriter {
	return &multiWriter{writers: writers}
}

func (m *multiWriter) Write(p []byte) (int, error) {
	for _, w := range m.writers {
		if _, err := w.Write(p); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func writeRawMeta(t *testing.T, dir string, raw string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, DefaultMetaFile), []byte(raw), 0644); err != nil {
		t.Fatal(err)
	}
}

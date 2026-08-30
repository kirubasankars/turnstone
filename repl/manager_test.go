// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"turnstone/config"
	"turnstone/database"
	"turnstone/protocol"
)

func setupReplTestEnv(t *testing.T) (string, map[string]*database.Database, *tls.Config) {
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

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	stores := make(map[string]*database.Database)
	st, err := database.Open(context.Background(), filepath.Join(dir, "data", "0"), logger, 0, "none", 90)
	if err != nil {
		t.Fatal(err)
	}
	stores["0"] = st

	certsDir := filepath.Join(dir, "certs")
	serverCert, _ := tls.LoadX509KeyPair(filepath.Join(certsDir, "server.crt"), filepath.Join(certsDir, "server.key"))
	caCert, _ := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	tlsConf := &tls.Config{Certificates: []tls.Certificate{serverCert}, RootCAs: pool, InsecureSkipVerify: true}

	return dir, stores, tlsConf
}

func writeReplFrame(conn net.Conn, opCode uint8, body []byte) error {
	crc := crc32.Checksum(body, protocol.Crc32Table)
	payload := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(payload[:4], crc)
	copy(payload[4:], body)

	header := make([]byte, 5)
	header[0] = opCode
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func startFakeLeader(t *testing.T, tlsConf *tls.Config, handler func(net.Conn)) (string, func()) {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", tlsConf)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

func readReplHello(conn net.Conn) error {
	head := make([]byte, 5)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[0] != protocol.OpCodeReplHello {
		return fmt.Errorf("expected ReplHello, got %d", head[0])
	}
	ln := binary.BigEndian.Uint32(head[1:])
	body := make([]byte, ln)
	_, err := io.ReadFull(conn, body)
	return err
}

func TestManager_ApplyLogRange_OffsetMismatch(t *testing.T) {
	_, stores, tlsConf := setupReplTestEnv(t)
	defer stores["0"].Close()

	addr, stop := startFakeLeader(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readReplHello(conn)
		_ = writeStatusOK(conn)

		body := new(bytes.Buffer)
		binary.Write(body, binary.BigEndian, uint32(len("0")))
		body.WriteString("0")
		binary.Write(body, binary.BigEndian, uint32(0))
		binary.Write(body, binary.BigEndian, uint64(999))
		binary.Write(body, binary.BigEndian, uint64(999))
		_ = writeReplFrame(conn, protocol.OpCodeReplLogRange, body.Bytes())
	})
	defer stop()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewManager("follower", stores, tlsConf, logger)
	if err := rm.Follow("0", addr, "0"); err != nil {
		t.Fatalf("Follow returned immediately: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := stores["0"].Get("repl-key"); err == nil {
			t.Fatal("offset mismatch must not apply data")
		}
		if stores["0"].LastLogOffset() > 0 {
			t.Fatal("offset mismatch must not advance follower log")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestManager_ApplyLogRange_MalformedPacket(t *testing.T) {
	_, stores, tlsConf := setupReplTestEnv(t)
	defer stores["0"].Close()

	addr, stop := startFakeLeader(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readReplHello(conn)
		_ = writeStatusOK(conn)
		_ = writeReplFrame(conn, protocol.OpCodeReplLogRange, []byte{0, 0, 0, 1, '0'})
	})
	defer stop()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewManager("follower", stores, tlsConf, logger)
	if err := rm.Follow("0", addr, "0"); err != nil {
		t.Fatalf("Follow returned immediately: %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	if stores["0"].LastLogOffset() > 0 {
		t.Fatal("malformed packet must not advance follower log")
	}
}

func TestManager_ApplyLogRange_ValidSegment(t *testing.T) {
	leaderDir := t.TempDir()
	leaderDB, err := database.Open(context.Background(), filepath.Join(leaderDir, "data"), slog.New(slog.NewTextHandler(io.Discard, nil)), 0, "none", 90)
	if err != nil {
		t.Fatal(err)
	}
	tx := leaderDB.NewTransaction(true)
	tx.Put([]byte("repl-key"), []byte("repl-val"))
	tx.Commit()
	seg, endOff, err := leaderDB.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	leaderDB.Close()

	_, stores, tlsConf := setupReplTestEnv(t)
	defer stores["0"].Close()

	addr, stop := startFakeLeader(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readReplHello(conn)
		_ = writeStatusOK(conn)

		body := new(bytes.Buffer)
		binary.Write(body, binary.BigEndian, uint32(len("0")))
		body.WriteString("0")
		binary.Write(body, binary.BigEndian, uint32(0))
		binary.Write(body, binary.BigEndian, uint64(0))
		binary.Write(body, binary.BigEndian, endOff)
		body.Write(seg)
		_ = writeReplFrame(conn, protocol.OpCodeReplLogRange, body.Bytes())
		<-time.After(2 * time.Second)
	})
	defer stop()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rm := NewManager("follower", stores, tlsConf, logger)
	if err := rm.Follow("0", addr, "0"); err != nil {
		t.Fatalf("Follow failed: %v", err)
	}

	waitForKey(t, stores["0"], "repl-key", "repl-val")
}

func writeStatusOK(conn net.Conn) error {
	header := make([]byte, 5)
	header[0] = protocol.ResStatusOK
	binary.BigEndian.PutUint32(header[1:], 0)
	_, err := conn.Write(header)
	return err
}

func waitForKey(t *testing.T, db *database.Database, key, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		val, err := db.Get(key)
		if err == nil && string(val) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("key %q not replicated, want %q", key, want)
}

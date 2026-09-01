// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package backup

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"testing"
	"time"

	"turnstone/protocol"
)

func startFakeBackupServer(t *testing.T, tlsConf *tls.Config, handler func(net.Conn)) (string, func()) {
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
		<-done
	}
}

func readBackupHello(conn net.Conn) error {
	head := make([]byte, 5)
	if _, err := io.ReadFull(conn, head); err != nil {
		return err
	}
	if head[0] != protocol.OpCodeReplHello {
		return fmt.Errorf("expected ReplHello, got 0x%02x", head[0])
	}
	ln := binary.BigEndian.Uint32(head[1:])
	body := make([]byte, ln)
	_, err := io.ReadFull(conn, body)
	return err
}

func writeBackupStatus(conn net.Conn, status uint8, msg string) error {
	body := []byte(msg)
	header := make([]byte, 5)
	header[0] = status
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if len(body) > 0 {
		_, err := conn.Write(body)
		return err
	}
	return nil
}

func writeBackupLogRange(conn net.Conn, dbName string, startOff, endOff uint64, data []byte) error {
	body := new(bytes.Buffer)
	binary.Write(body, binary.BigEndian, uint32(len(dbName)))
	body.WriteString(dbName)
	binary.Write(body, binary.BigEndian, uint32(0))
	binary.Write(body, binary.BigEndian, startOff)
	binary.Write(body, binary.BigEndian, endOff)
	body.Write(data)

	crc := crc32.Checksum(body.Bytes(), protocol.Crc32Table)
	payload := make([]byte, 4+body.Len())
	binary.BigEndian.PutUint32(payload[:4], crc)
	copy(payload[4:], body.Bytes())

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplLogRange
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func TestStreamLogRange_RequiresTLS(t *testing.T) {
	_, err := StreamLogRange(context.Background(), StreamOptions{}, io.Discard)
	if err == nil || err.Error() != "TLS config is required" {
		t.Fatalf("expected TLS required, got %v", err)
	}
}

func TestStreamLogRange_HandshakeRejected(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusErr, "Invalid replication cursor for DB '1'")
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		StartLSN: 1,
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected handshake rejection")
	}
}

func TestStreamLogRange_ServerErrorDuringStream(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")
		_ = writeBackupStatus(conn, protocol.ResStatusErr, "log unavailable")
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected server error during stream")
	}
}

func TestStreamLogRange_CRCMismatch(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")

		header := make([]byte, 5)
		header[0] = protocol.OpCodeReplLogRange
		badPayload := []byte{0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}
		binary.BigEndian.PutUint32(header[1:], uint32(len(badPayload)))
		_, _ = conn.Write(header)
		_, _ = conn.Write(badPayload)
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected CRC mismatch error")
	}
}

func TestStreamLogRange_UnexpectedDatabase(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")
		_ = writeBackupLogRange(conn, "2", 0, 3, []byte{1, 2, 3})
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected unexpected database error")
	}
}

func TestStreamLogRange_LogRangeOffsetMismatch(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")
		_ = writeBackupLogRange(conn, "1", 0, 99, []byte{1, 2, 3})
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected offset mismatch error")
	}
}

func TestStreamLogRange_MalformedPacket(t *testing.T) {
	tlsConf := testTLSConfig(t)
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")

		body := []byte{0, 0, 0, 1, '1'}
		crc := crc32.Checksum(body, protocol.Crc32Table)
		payload := make([]byte, 4+len(body))
		binary.BigEndian.PutUint32(payload[:4], crc)
		copy(payload[4:], body)

		header := make([]byte, 5)
		header[0] = protocol.OpCodeReplLogRange
		binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
		_, _ = conn.Write(header)
		_, _ = conn.Write(payload)
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, io.Discard)
	if err == nil {
		t.Fatal("expected malformed packet error")
	}
}

func TestStreamLogRange_WriterFailure(t *testing.T) {
	tlsConf := testTLSConfig(t)
	wal := writeEngineWAL(t, "k")
	addr, stop := startFakeBackupServer(t, tlsConf, func(conn net.Conn) {
		defer conn.Close()
		_ = readBackupHello(conn)
		_ = writeBackupStatus(conn, protocol.ResStatusOK, "")
		_ = writeBackupLogRange(conn, "1", 0, uint64(len(wal)), wal)
	})
	defer stop()

	_, err := StreamLogRange(context.Background(), StreamOptions{
		Host:     addr,
		DBName:   "1",
		WaitIdle: 50 * time.Millisecond,
		TLS:      tlsConf,
	}, failingWriter{})
	if err == nil {
		t.Fatal("expected writer failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("disk full")
}

func TestParseLogRangePayload_Errors(t *testing.T) {
	if _, _, err := parseLogRangePayload(nil, "1"); err == nil {
		t.Fatal("expected malformed packet error")
	}
	if _, _, err := parseLogRangePayload([]byte{0, 0, 0, 1, '2', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3}, "1"); err == nil {
		t.Fatal("expected unexpected database error")
	}
}

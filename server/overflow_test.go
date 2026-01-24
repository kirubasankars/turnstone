package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
	"turnstone/protocol"
)

func TestMGet_IntegerOverflow_Safety(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(50 * time.Millisecond) // Wait for listener

	// We use a raw TCP connection to send a crafted malicious payload
	conn, err := tls.Dial("tcp", srv.listener.Addr().String(), getClientTLS(t, dir))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// 1. Select Database
	dbName := []byte("0")
	sel := make([]byte, 5+len(dbName))
	sel[0] = protocol.OpCodeSelect
	binary.BigEndian.PutUint32(sel[1:], uint32(len(dbName)))
	copy(sel[5:], dbName)
	if _, err := conn.Write(sel); err != nil {
		t.Fatal(err)
	}
	readResponse(t, conn) // Consume Select response

	// 2. Begin Transaction
	begin := make([]byte, 5)
	begin[0] = protocol.OpCodeBegin
	if _, err := conn.Write(begin); err != nil {
		t.Fatal(err)
	}
	readResponse(t, conn) // Consume Begin response

	// 3. Crafted MGET Payload
	// NumKeys = 1
	// KeyLen = 0xFFFFFFFF (4294967295) -> -1 as signed 32-bit int
	// If cast to int32 without check, it becomes -1.
	// offset + -1 < len(payload). Panic on slice.
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))          // NumKeys
	binary.Write(buf, binary.BigEndian, uint32(0xFFFFFFFF)) // KeyLen (Huge/Negative)
	// We don't provide the key bytes. Payload ends here.

	header := make([]byte, 5)
	header[0] = protocol.OpCodeMGet
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}

	// 3. Expect Error (Graceful rejection), NOT Panic (EOF or Timeout)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	respHead := make([]byte, 5)
	if _, err := io.ReadFull(conn, respHead); err != nil {
		if err == io.EOF {
			t.Fatal("Connection closed unexpectedly (possible panic)")
		}
		t.Fatalf("Read failed: %v", err)
	}

	if respHead[0] != protocol.ResStatusErr {
		t.Errorf("Expected Error Status (0x01), got 0x%x", respHead[0])
	}

	// Ensure server is still alive
	conn.SetReadDeadline(time.Time{})
	ln := binary.BigEndian.Uint32(respHead[1:])
	body := make([]byte, ln)
	io.ReadFull(conn, body)
	t.Logf("Server Response: %s", string(body))
}

func readResponse(t *testing.T, conn net.Conn) {
	t.Helper()
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("Read response header failed: %v", err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length > 0 {
		body := make([]byte, length)
		if _, err := io.ReadFull(conn, body); err != nil {
			t.Fatalf("Read response body failed: %v", err)
		}
	}
}

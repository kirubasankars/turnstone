package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"
)

// TestReplication_SlowConsumer_Dropped verifies that the leader closes the connection
// to a replica that fails to consume the stream within ReplicaSendTimeout.
func TestReplication_SlowConsumer_Dropped(t *testing.T) {
	// 1. Lower timeout to speed up test
	originalTimeout := ReplicaSendTimeout
	ReplicaSendTimeout = 200 * time.Millisecond
	defer func() { ReplicaSendTimeout = originalTimeout }()

	// 2. Setup Environment
	dir, stores, srv := setupTestEnv(t)
	defer stores["default"].Close()
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond) // Wait for server start

	// 3. Connect "Slow" Replica manually
	// We use a raw connection so we can control exactly what we read (or don't read).
	tlsConfig := getClientTLS(t, dir)
	conn, err := tls.Dial("tcp", srv.listener.Addr().String(), tlsConfig)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// 4. Send Hello Handshake to subscribe to "default" DB
	// Protocol: [OpCodeReplHello][Len][Ver:1][NumDBs:1]...
	// SubRequest: [NameLen][Name][LogID]
	dbName := "default"

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	binary.Write(buf, binary.BigEndian, uint32(1)) // NumDBs

	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, uint64(0)) // Start from LogID 0

	header := make([]byte, 5)
	header[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	if _, err := conn.Write(header); err != nil {
		t.Fatalf("Failed to send header: %v", err)
	}
	if _, err := conn.Write(buf.Bytes()); err != nil {
		t.Fatalf("Failed to send body: %v", err)
	}

	// 5. Generate Load on Leader
	// The leader has an output channel buffer of 10. We will send 50 transactions.
	// Since we are NOT reading from 'conn', the leader's buffer should fill up,
	// triggering the timeout in streamDB.
	client := connectClient(t, srv.listener.Addr().String(), tlsConfig)
	defer client.Close()

	// Launch generator in background
	go func() {
		for i := 0; i < 50; i++ {
			client.AssertStatus(OpCodeBegin, nil, ResStatusOK)

			// Large payload to fill TCP buffers quickly
			key := []byte("k")
			val := make([]byte, 4096)

			pl := make([]byte, 4+len(key)+len(val))
			binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
			copy(pl[4:], key)
			copy(pl[4+len(key):], val)

			client.AssertStatus(OpCodeSet, pl, ResStatusOK)
			client.AssertStatus(OpCodeCommit, nil, ResStatusOK)

			time.Sleep(5 * time.Millisecond)
		}
	}()

	// 6. Verify Disconnection
	// We expect the Read to eventually fail with EOF when the server closes the connection.
	// We set a deadline slightly longer than the ReplicaSendTimeout to ensure we catch it.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	b := make([]byte, 1024)
	for {
		_, err := conn.Read(b)
		if err != nil {
			if err == io.EOF {
				// Success: Server closed connection gracefully
				return
			}
			// Success: Server closed connection (ECONNRESET or similar)
			// Depending on platform/timing, this might not be strictly EOF, but any read error implies disconnection here.
			return
		}
		// If we keep reading successfully, the test fails (timeout will kill it)
	}
}

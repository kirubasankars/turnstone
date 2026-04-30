package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"
	"turnstone/protocol"
)

// TestMemoryLeak_Reproduction simulates a massive replication stream with no commit marker.
// The consumer buffers everything in memory until it potentially OOMs or we verify the size.
func TestMemoryLeak_ReplicationConsumer(t *testing.T) {
	dir, _, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect as CDC/Replica
	cdc := connectClient(t, srv.Addr().String(), getRoleTLS(t, dir, "cdc"))
	defer cdc.Close()

	// 1. We act as the Leader. The Server connects TO us.
	// But in this test setup with 'connectClient', we are connecting TO the server.
	// The Server accepts connections for Admin/Client/CDC logic.
	// However, the "Replication Consumer" logic runs when the Server Dials OUT to a leader.
	// WE cannot easily make the Server Dial US without configuring it to do so.

	// ALTERNATIVE: Use `HandleReplicaConnection` (Server as Leader) ?
	// No, the bug report says "Memory Leak in Replication Consumer".
	// The Consumer is `replication/replication.go`.

	// We need to configure the server to replicate from a "fake leader".
	// 1. Create a TLS listener (Fake Leader).
	tlsConfig := getRoleTLS(t, dir, "server")
	fakeLeader, err := tls.Listen("tcp", ":0", tlsConfig)
	if err != nil {
		t.Fatalf("Failed to start fake leader: %v", err)
	}
	defer fakeLeader.Close()
	// fakeLeader.Addr() reports "0.0.0.0:port" (wildcard bind address), which
	// isn't reliably dialable as a destination; substitute the loopback IP
	// the dialer should actually connect to.
	fakeLeaderAddr := fmt.Sprintf("127.0.0.1:%d", fakeLeader.Addr().(*net.TCPAddr).Port)

	// 2. Configure Server to replicate from Fake Leader
	// We use the Admin client to send `REPLICAOF` command.
	admin := connectClient(t, srv.Addr().String(), getRoleTLS(t, dir, "admin"))
	defer admin.Close()

	// REPLICAOF localhost:port 1
	// Payload: [AddrLen][Addr][RemoteDB]
	cmd := new(bytes.Buffer)
	addrBytes := []byte(fakeLeaderAddr)
	binary.Write(cmd, binary.BigEndian, uint32(len(addrBytes)))
	cmd.Write(addrBytes)
	cmd.WriteString("1") // Remote DB Name

	// StepDown first because setupTestEnv promotes to Primary
	admin.Send(protocol.OpCodeStepDown, nil)
	statusSD, _ := admin.Read()
	if statusSD != protocol.ResStatusOK {
		t.Fatalf("Failed to step down: %x", statusSD)
	}

	// StepDown disconnects all connections on this db (including the
	// current admin session itself); reconnect before issuing further
	// commands rather than racing the delayed connection-kill.
	admin.Close()
	admin = connectClient(t, srv.Addr().String(), getRoleTLS(t, dir, "admin"))
	defer admin.Close()

	// REPLICAOF first performs a short verification handshake on its own
	// connection (which it closes) before spawning the persistent streaming
	// connection. Accept and ack that verification connection in the
	// background while we wait for the ReplicaOf response.
	verifyDone := make(chan struct{})
	go func() {
		defer close(verifyDone)
		vconn, err := fakeLeader.Accept()
		if err != nil {
			return
		}
		defer vconn.Close()
		buf := make([]byte, 1024)
		if _, err := vconn.Read(buf); err != nil {
			return
		}
		if buf[0] != protocol.OpCodeReplHello {
			return
		}
		// Ack with OK/empty response so verifyHandshake succeeds.
		ack := make([]byte, 5)
		ack[0] = protocol.ResStatusOK
		_, _ = vconn.Write(ack)
	}()

	admin.Send(protocol.OpCodeReplicaOf, cmd.Bytes())
	status, _ := admin.Read()
	if status != protocol.ResStatusOK {
		t.Fatalf("Failed to configure replication: %x", status)
	}
	<-verifyDone

	// 3. Accept the persistent streaming connection from the server (Consumer)
	conn, err := fakeLeader.Accept()
	if err != nil {
		t.Fatalf("Fake Leader accept failed: %v", err)
	}
	defer conn.Close()

	// 4. Handle Handshake
	// Expect Hello: [Ver][IDLen][ID][NumDBs]...
	buf := make([]byte, 1024)
	_, err = conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	// Verify it's a Hello (We skip detailed parsing for brevity, assuming it works)
	if buf[0] != protocol.OpCodeReplHello {
		t.Fatalf("Expected Hello (0x50), got 0x%x", buf[0])
	}

	// 5. Send Infinite Transaction (Flood)
	// We send batches of LogEntries. Each batch has NO Commit marker.
	// Consumer should append to buffer indefinitely.

	// Entry: [LogID][TxID][Op][KLen][Key][VLen][Val]
	// Size: 8+8+1+4+1+4+1000 = ~1026 bytes per entry
	val := make([]byte, 1000) // 1KB value

	// Send 70MB of data (approx 70,000 entries)
	// If limit is 64MB, this should fail early.

	bytesSentCh := make(chan int64)

	go func() {
		defer conn.Close()
		seq := uint64(1)
		var totalSent int64

		for i := 0; i < 70000; i++ {
			batch := new(bytes.Buffer)

			// Entry
			binary.Write(batch, binary.BigEndian, seq)       // LogID
			binary.Write(batch, binary.BigEndian, seq)       // TxID (Same Tx)
			batch.WriteByte(protocol.OpJournalSet)           // Op
			binary.Write(batch, binary.BigEndian, uint32(3)) // KLen
			batch.WriteString("key")
			binary.Write(batch, binary.BigEndian, uint32(len(val))) // VLen
			batch.Write(val)

			payload := new(bytes.Buffer)
			dbName := "1"
			binary.Write(payload, binary.BigEndian, uint32(len(dbName)))
			payload.WriteString(dbName)
			binary.Write(payload, binary.BigEndian, uint32(1)) // Count
			payload.Write(batch.Bytes())

			header := make([]byte, 5)
			header[0] = protocol.OpCodeReplBatch
			binary.BigEndian.PutUint32(header[1:], uint32(payload.Len()))

			n, err := conn.Write(header)
			if err != nil {
				bytesSentCh <- totalSent
				return
			}
			totalSent += int64(n)

			n, err = conn.Write(payload.Bytes())
			if err != nil {
				bytesSentCh <- totalSent
				return
			}
			totalSent += int64(n)
			seq++
		}
		bytesSentCh <- totalSent
	}()

	// 6. Wait for result
	select {
	case sent := <-bytesSentCh:
		// We expect the server to cut us off BEFORE 70MB (e.g. at 64MB + TCP Buffer)
		// 70,000 * 1024 ~= 71MB.
		// Server rejected at 64MB. TCP buffer might hold 1-2MB.
		// If we sent < 69MB, we consider it passed (cut off).
		limit := int64(69 * 1024 * 1024)
		if sent > limit {
			t.Fatalf("Memory Leak: Server accepted %d bytes (Limit 64MB not enforced)", sent)
		}
		t.Logf("Pass: Server cut connection after %d bytes", sent)
	case <-time.After(10 * time.Second):
		t.Fatal("Test Timeout: Connection stalled")
	}
}

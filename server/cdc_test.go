package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"turnstone/protocol"
)

// TestCDC_Streaming_WithIdle verifies that a CDC client receives updates
// and the connection remains open during idle periods.
func TestCDC_Streaming_WithIdle(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	cdcTLS := getRoleTLS(t, baseDir, "cdc")

	_, addr, cancel := startServerNode(t, baseDir, "cdc_node", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr)

	// 0. Primer: Write initial data to stabilize WAL state
	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "primer", "val_0")

	// 1. Connect CDC Client
	cdcConn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial CDC: %v", err)
	}
	defer cdcConn.Close()

	// 2. Send Hello Handshake (StartSeq 0)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	clientID := "cdc-test-idle"
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Count
	dbName := "1"
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, uint64(0)) // StartSeq

	h := make([]byte, 5)
	h[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(h[1:], uint32(buf.Len()))
	cdcConn.Write(h)
	cdcConn.Write(buf.Bytes())

	// Helper to read a batch
	readBatch := func() []byte {
		t.Helper()
		header := make([]byte, 5)
		for {
			cdcConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			if _, err := io.ReadFull(cdcConn, header); err != nil {
				t.Fatalf("Read header failed: %v", err)
			}

			// Handle potential errors from server
			if header[0] == protocol.ResStatusErr {
				length := binary.BigEndian.Uint32(header[1:])
				payload := make([]byte, length)
				io.ReadFull(cdcConn, payload)
				t.Fatalf("Server returned error: %s", string(payload))
			}

			length := binary.BigEndian.Uint32(header[1:])
			payload := make([]byte, length)
			if _, err := io.ReadFull(cdcConn, payload); err != nil {
				t.Fatalf("Read payload failed: %v", err)
			}

			if header[0] == protocol.OpCodeReplTimeline {
				// Skip timeline updates
				continue
			}

			if header[0] != protocol.OpCodeReplBatch {
				t.Fatalf("Expected OpCodeReplBatch (0x%x), got 0x%x", protocol.OpCodeReplBatch, header[0])
			}
			return payload
		}
	}

	// 3. Verify Primer receipt (since we started at 0)
	payload0 := readBatch()
	if !bytes.Contains(payload0, []byte("primer")) {
		t.Errorf("Expected 'primer' in first batch, got: %q", payload0)
	}

	// 4. Write Data live
	writeKeyVal(t, client, "cdc_key_1", "val_1")

	// 5. Verify CDC receipt
	payload := readBatch()
	if !bytes.Contains(payload, []byte("cdc_key_1")) {
		t.Errorf("Expected 'cdc_key_1' in batch, got: %q", payload)
	}

	// 6. Test Idle / Keep-Alive
	time.Sleep(2 * time.Second)

	// 7. Write more data
	writeKeyVal(t, client, "cdc_key_2", "val_2")

	// 8. Verify receipt after idle
	payload2 := readBatch()
	if !bytes.Contains(payload2, []byte("cdc_key_2")) {
		t.Errorf("Expected 'cdc_key_2' in batch after idle, got: %q", payload2)
	}
}

// TestCDC_MessageContent verifies the structural integrity and correctness
// of CDC messages for Set and Delete operations.
func TestCDC_MessageContent(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	cdcTLS := getRoleTLS(t, baseDir, "cdc")

	_, addr, cancel := startServerNode(t, baseDir, "cdc_content", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr)

	// 0. Primer (to stabilize WAL)
	client := connectClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "primer", "val_0")

	// 1. Connect CDC Client
	cdcConn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial CDC: %v", err)
	}
	defer cdcConn.Close()

	// 2. Handshake (StartSeq 0)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	clientID := "cdc-test-content"
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)
	binary.Write(buf, binary.BigEndian, uint32(1))
	dbName := "1"
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, uint64(0))

	h := make([]byte, 5)
	h[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(h[1:], uint32(buf.Len()))
	cdcConn.Write(h)
	cdcConn.Write(buf.Bytes())

	// 3. Perform Operations
	// Op 1: Set
	writeKeyVal(t, client, "test_key", "test_val")

	// Op 2: Delete
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeDel, []byte("test_key"), protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 4. Read and Parse CDC Stream
	// We might receive the primer first, then our ops.
	foundSet := false
	foundDel := false

	deadline := time.Now().Add(5 * time.Second)
	cdcConn.SetReadDeadline(deadline)

	for !foundSet || !foundDel {
		if time.Now().After(deadline) {
			t.Fatal("Timeout waiting for CDC events")
		}

		// Read Header
		header := make([]byte, 5)
		if _, err := io.ReadFull(cdcConn, header); err != nil {
			if err == io.EOF {
				t.Fatal("CDC connection closed unexpectedly")
			}
			t.Fatalf("Read CDC header failed: %v", err)
		}

		if header[0] == protocol.ResStatusErr {
			length := binary.BigEndian.Uint32(header[1:])
			payload := make([]byte, length)
			io.ReadFull(cdcConn, payload)
			t.Fatalf("Server returned error: %s", string(payload))
		}

		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(cdcConn, payload); err != nil {
			t.Fatalf("Read CDC payload failed: %v", err)
		}

		if header[0] != protocol.OpCodeReplBatch {
			continue // Skip other messages
		}

		// Parse Batch: [DBNameLen][DBName][Count][Entries...]
		cursor := 0
		if cursor+4 > len(payload) {
			continue
		}
		dbLen := int(binary.BigEndian.Uint32(payload[cursor:]))
		cursor += 4 + dbLen
		if cursor+4 > len(payload) {
			continue
		}
		count := int(binary.BigEndian.Uint32(payload[cursor:]))
		cursor += 4

		for i := 0; i < count; i++ {
			// Entry Header: [LogID(8)][TxID(8)][Op(1)]
			if cursor+17 > len(payload) {
				break
			}
			opType := payload[cursor+16]
			cursor += 17

			// Key
			if cursor+4 > len(payload) {
				break
			}
			kLen := int(binary.BigEndian.Uint32(payload[cursor:]))
			cursor += 4
			key := string(payload[cursor : cursor+kLen])
			cursor += kLen

			// Val
			if cursor+4 > len(payload) {
				break
			}
			vLen := int(binary.BigEndian.Uint32(payload[cursor:]))
			cursor += 4
			val := string(payload[cursor : cursor+vLen])
			cursor += vLen

			if key == "test_key" {
				if opType == protocol.OpJournalSet {
					if val == "test_val" {
						foundSet = true
					} else {
						t.Errorf("Received Set event for 'test_key' but wrong value: %s", val)
					}
				} else if opType == protocol.OpJournalDelete {
					foundDel = true
				}
			}
		}
	}

	if !foundSet {
		t.Error("Did not receive Set event for 'test_key'")
	}
	if !foundDel {
		t.Error("Did not receive Delete event for 'test_key'")
	}
}

// TestCDC_PurgedWAL_TriggersSnapshot verifies that if the requested logs are missing,
// the server initiates a Full Sync (Snapshot) instead of returning an error.
func TestCDC_PurgedWAL_TriggersSnapshot(t *testing.T) {
	dir, stores, srv, cleanup := setupTestEnv(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()
	promoteNode(t, dir, addr, "1")
	clientTLS := getClientTLS(t, dir)
	cdcTLS := getRoleTLS(t, dir, "cdc")
	adminTLS := getRoleTLS(t, dir, "admin")

	// Helper to create SET payload
	makeSet := func(k, v string) []byte {
		kb, vb := []byte(k), []byte(v)
		buf := make([]byte, 4+len(kb)+len(vb))
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(kb)))
		copy(buf[4:], kb)
		copy(buf[4+len(kb):], vb)
		return buf
	}

	// 1. Write initial data to Database "1" to generate WAL entry (OpID 1)
	c1 := connectClient(t, addr, clientTLS)
	defer c1.Close()
	selectDatabase(t, c1, "1")

	c1.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeSet, makeSet("k1", "v1"), protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 2. Force WAL Rotation using Promote() (File 1 -> File 2)
	// Note: We use the internal store method to bypass the "Must be UNDEFINED" check
	// because we just want to force a rotation for testing, not change topology.
	st1 := stores["1"]
	
	// FIX: StepDown first before calling Promote via admin client
	cAdmin := connectClient(t, addr, adminTLS)
	selectDatabase(t, cAdmin, "1")
	cAdmin.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cAdmin.Close()

	// Re-promote (Forces new timeline/rotation)
	cAdmin2 := connectClient(t, addr, adminTLS)
	selectDatabase(t, cAdmin2, "1")
	cAdmin2.AssertStatus(protocol.OpCodePromote, make([]byte, 4), protocol.ResStatusOK)
	cAdmin2.Close()

	// 3. Write more data (OpID 2) into File 2
	c1.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeSet, makeSet("k2", "v2"), protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 4. Force WAL Rotation again (File 2 -> File 3)
	// Again, StepDown first
	cAdmin3 := connectClient(t, addr, adminTLS)
	selectDatabase(t, cAdmin3, "1")
	cAdmin3.AssertStatus(protocol.OpCodeStepDown, nil, protocol.ResStatusOK)
	cAdmin3.Close()

	cAdmin4 := connectClient(t, addr, adminTLS)
	selectDatabase(t, cAdmin4, "1")
	cAdmin4.AssertStatus(protocol.OpCodePromote, make([]byte, 4), protocol.ResStatusOK)
	cAdmin4.Close()

	// 5. Purge logs older than OpID 2. This deletes File 1.
	if err := st1.DB.PurgeWAL(2); err != nil {
		t.Fatal(err)
	}

	// 6. Connect CDC requesting StartSeq 0.
	cdcConn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial CDC: %v", err)
	}
	defer cdcConn.Close()

	// Handshake
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Ver
	clientid := "test-cdc-snapshot"
	binary.Write(buf, binary.BigEndian, uint32(len(clientid)))
	buf.WriteString(clientid)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Count
	part := "1"
	binary.Write(buf, binary.BigEndian, uint32(len(part)))
	buf.WriteString(part)
	binary.Write(buf, binary.BigEndian, uint64(0)) // Request StartSeq 0

	header := make([]byte, 5)
	header[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(header[1:], uint32(buf.Len()))

	cdcConn.Write(header)
	cdcConn.Write(buf.Bytes())

	// 7. Read response. Expect Snapshot Packet.
	respHead := make([]byte, 5)

	// Loop to skip Timeline updates
	for {
		cdcConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(cdcConn, respHead); err != nil {
			t.Fatalf("Read CDC response failed: %v", err)
		}

		if respHead[0] == protocol.OpCodeReplTimeline {
			// Consume payload and continue
			ln := binary.BigEndian.Uint32(respHead[1:])
			io.ReadFull(cdcConn, make([]byte, ln))
			continue
		}
		break
	}

	// Expect Snapshot OpCode
	if respHead[0] != protocol.OpCodeReplSnapshot {
		if respHead[0] == protocol.ResStatusErr {
			ln := binary.BigEndian.Uint32(respHead[1:])
			body := make([]byte, ln)
			io.ReadFull(cdcConn, body)
			t.Fatalf("Unexpected error from server: %s", string(body))
		}
		t.Fatalf("Expected OpCodeReplSnapshot (0x53), got 0x%x", respHead[0])
	}

	// Consume Snapshot Data
	ln := binary.BigEndian.Uint32(respHead[1:])
	body := make([]byte, ln)
	if _, err := io.ReadFull(cdcConn, body); err != nil {
		t.Fatalf("Read snapshot body failed: %v", err)
	}

	// Verify K1 and K2 are present in the snapshot
	if !bytes.Contains(body, []byte("k1")) || !bytes.Contains(body, []byte("v1")) {
		t.Error("Snapshot missing k1/v1")
	}
	if !bytes.Contains(body, []byte("k2")) || !bytes.Contains(body, []byte("v2")) {
		t.Error("Snapshot missing k2/v2")
	}

	// 8. Expect Snapshot Done Signal (Updated for framing)
	if _, err := io.ReadFull(cdcConn, respHead); err != nil {
		t.Fatalf("Read Done header failed: %v", err)
	}
	if respHead[0] != protocol.OpCodeReplSnapshotDone {
		t.Fatalf("Expected OpCodeReplSnapshotDone (0x54), got 0x%x", respHead[0])
	}
	ln = binary.BigEndian.Uint32(respHead[1:])

	// Payload contains [DBNameLen(4)][DBName][Count(4)][Data(16)]
	// DBName="1" (1 byte) -> Total wrapper = 9 bytes. Total payload = 25 bytes.
	if ln < 9+16 {
		t.Fatalf("Expected at least 25 byte payload for SnapshotDone (wrapper+data), got %d", ln)
	}

	payload := make([]byte, ln)
	if _, err := io.ReadFull(cdcConn, payload); err != nil {
		t.Fatalf("Read Done payload failed: %v", err)
	}

	// Skip framing
	cursor := 0
	nLen := int(binary.BigEndian.Uint32(payload[cursor:]))
	cursor += 4 + nLen + 4 // DBNameLen + DBName + Count

	data := payload[cursor:]
	if len(data) != 16 {
		t.Fatalf("Expected 16 byte data (TxID+OpID), got %d", len(data))
	}

	// 9. Consume Resent Timeline Packet (New behavior in fixed logic)
	// Server sends current timeline immediately after snapshot to ensure client is aligned.
	if _, err := io.ReadFull(cdcConn, respHead); err != nil {
		t.Fatalf("Read Next header failed: %v", err)
	}
	// Consume if it is a timeline packet
	if respHead[0] == protocol.OpCodeReplTimeline {
		ln = binary.BigEndian.Uint32(respHead[1:])
		io.ReadFull(cdcConn, make([]byte, ln))
	}

	// 10. Write new data to verify live streaming
	c1.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeSet, makeSet("k3", "v3"), protocol.ResStatusOK)
	c1.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 11. Read Next Packet - Should be Batch (0x51)
	// We loop to skip potential SafePoint packets
	for {
		if _, err := io.ReadFull(cdcConn, respHead); err != nil {
			t.Fatalf("Read Batch header failed: %v", err)
		}
		if respHead[0] == protocol.OpCodeReplBatch {
			break
		}
		// Skip other packets
		ln = binary.BigEndian.Uint32(respHead[1:])
		io.ReadFull(cdcConn, make([]byte, ln))
	}

	if respHead[0] != protocol.OpCodeReplBatch {
		t.Fatalf("Expected OpCodeReplBatch (0x51), got 0x%x", respHead[0])
	}
}

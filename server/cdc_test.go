package server

import (
	"bytes"
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

		if header[0] != protocol.OpCodeReplBatch {
			t.Fatalf("Expected OpCodeReplBatch (0x%x), got 0x%x", protocol.OpCodeReplBatch, header[0])
		}
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(cdcConn, payload); err != nil {
			t.Fatalf("Read payload failed: %v", err)
		}
		return payload
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

		if header[0] != protocol.OpCodeReplBatch {
			continue // Skip other messages
		}
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(cdcConn, payload); err != nil {
			t.Fatalf("Read CDC payload failed: %v", err)
		}

		// Parse Batch: [DBNameLen][DBName][Count][Entries...]
		cursor := 0
		if cursor+4 > len(payload) { continue }
		dbLen := int(binary.BigEndian.Uint32(payload[cursor:]))
		cursor += 4 + dbLen
		if cursor+4 > len(payload) { continue }
		count := int(binary.BigEndian.Uint32(payload[cursor:]))
		cursor += 4

		for i := 0; i < count; i++ {
			// Entry Header: [LogID(8)][TxID(8)][Op(1)]
			if cursor+17 > len(payload) { break }
			opType := payload[cursor+16]
			cursor += 17

			// Key
			if cursor+4 > len(payload) { break }
			kLen := int(binary.BigEndian.Uint32(payload[cursor:]))
			cursor += 4
			key := string(payload[cursor : cursor+kLen])
			cursor += kLen

			// Val
			if cursor+4 > len(payload) { break }
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

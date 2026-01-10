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
	// CDC functionality requires the CDC role certificate
	cdcTLS := getRoleTLS(t, baseDir, "cdc")

	// Start a node
	_, addr, cancel := startServerNode(t, baseDir, "cdc_node", clientTLS)
	defer cancel()

	// 1. Connect CDC Client via TLS (using CDC Cert)
	cdcConn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial CDC: %v", err)
	}
	defer cdcConn.Close()

	// 2. Send Hello Handshake
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version

	clientID := "cdc-test-idle"
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)

	binary.Write(buf, binary.BigEndian, uint32(1)) // Database Count

	dbName := "1"
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, uint64(0)) // StartSeq

	helloHeader := make([]byte, 5)
	helloHeader[0] = protocol.OpCodeReplHello
	binary.BigEndian.PutUint32(helloHeader[1:], uint32(buf.Len()))

	if _, err := cdcConn.Write(helloHeader); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}
	if _, err := cdcConn.Write(buf.Bytes()); err != nil {
		t.Fatalf("Failed to write body: %v", err)
	}

	// 3. Helper to read a batch
	readBatch := func() []byte {
		t.Helper()
		header := make([]byte, 5)
		// Set a deadline for the test read, so we don't hang forever if something breaks
		cdcConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(cdcConn, header); err != nil {
			t.Fatalf("Read header failed: %v", err)
		}
		if header[0] != protocol.OpCodeReplBatch {
			t.Fatalf("Expected OpCodeReplBatch, got 0x%x", header[0])
		}
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(cdcConn, payload); err != nil {
			t.Fatalf("Read payload failed: %v", err)
		}
		return payload
	}

	// 4. Write Data via standard client
	client := connectReplClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")
	writeKeyVal(t, client, "cdc_key_1", "val_1")

	// 5. Verify CDC receipt
	payload := readBatch()
	if !bytes.Contains(payload, []byte("cdc_key_1")) {
		t.Errorf("Expected 'cdc_key_1' in batch, got: %q", payload)
	}

	// 6. Test Idle / Keep-Alive
	// We wait 2 seconds. The server's default read deadline (set in handleConnection) would normally
	// apply if we didn't clear it in HandleReplicaConnection.
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
// of CDC messages for Set and Delete operations by parsing the binary stream.
func TestCDC_MessageContent(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	cdcTLS := getRoleTLS(t, baseDir, "cdc")

	_, addr, cancel := startServerNode(t, baseDir, "cdc_content", clientTLS)
	defer cancel()

	// 1. Connect CDC Client
	cdcConn, err := tls.Dial("tcp", addr, cdcTLS)
	if err != nil {
		t.Fatalf("Failed to dial CDC: %v", err)
	}
	defer cdcConn.Close()

	// 2. Handshake (StartSeq 0)
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
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
	client := connectReplClient(t, addr, clientTLS)
	defer client.Close()
	selectDatabase(t, client, "1")

	// Op 1: Set
	writeKeyVal(t, client, "test_key", "test_val")

	// Op 2: Delete
	client.AssertStatus(protocol.OpCodeBegin, nil, protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeDel, []byte("test_key"), protocol.ResStatusOK)
	client.AssertStatus(protocol.OpCodeCommit, nil, protocol.ResStatusOK)

	// 4. Read and Parse CDC Stream
	// We expect potentially 1 or 2 batches depending on timing/flushing.
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
		if header[0] != protocol.OpCodeReplBatch {
			continue // Skip other messages (e.g. heartbeats/acks if any)
		}
		length := binary.BigEndian.Uint32(header[1:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(cdcConn, payload); err != nil {
			t.Fatalf("Read CDC payload failed: %v", err)
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

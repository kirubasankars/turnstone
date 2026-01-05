package client

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// --- Test Infrastructure ---

// mockServer is a simple TCP server that mimics TurnstoneDB behavior for testing.
type mockServer struct {
	listener net.Listener
	handler  func(conn net.Conn)
	wg       sync.WaitGroup
}

// newMockServer starts a listener on a random port and accepts a SINGLE connection
// for testing isolation.
func newMockServer(t *testing.T, handler func(conn net.Conn)) *mockServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	ms := &mockServer{listener: ln, handler: handler}
	ms.wg.Add(1)
	go func() {
		defer ms.wg.Done()
		conn, err := ln.Accept()
		if err != nil {
			// Listener closed or error
			return
		}
		defer conn.Close()
		handler(conn)
	}()
	return ms
}

func (ms *mockServer) Addr() string {
	return ms.listener.Addr().String()
}

func (ms *mockServer) Close() {
	ms.listener.Close()
	ms.wg.Wait()
}

// readRequest reads a header and payload from the connection.
func readRequest(conn net.Conn) (byte, []byte, error) {
	header := make([]byte, ProtoHeaderSize)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}
	op := header[0]
	length := binary.BigEndian.Uint32(header[1:])
	var payload []byte
	if length > 0 {
		payload = make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}
	return op, payload, nil
}

// writeResponse sends a header and payload to the connection.
func writeResponse(conn net.Conn, status byte, body []byte) error {
	header := make([]byte, ProtoHeaderSize)
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

// --- Tests ---

func TestClient_Ping(t *testing.T) {
	ms := newMockServer(t, func(conn net.Conn) {
		op, _, err := readRequest(conn)
		if err != nil {
			t.Errorf("Read error: %v", err)
			return
		}
		if op != OpCodePing {
			t.Errorf("Expected OpCodePing (0x%02x), got 0x%02x", OpCodePing, op)
		}
		writeResponse(conn, ResStatusOK, []byte("PONG"))
	})
	defer ms.Close()

	c, err := NewClient(Config{Address: ms.Addr()})
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	if err := c.Ping(); err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestClient_GetSet_Protocol(t *testing.T) {
	ms := newMockServer(t, func(conn net.Conn) {
		// 1. Expect SET
		op, payload, _ := readRequest(conn)
		if op != OpCodeSet {
			t.Errorf("Expected Set, got 0x%02x", op)
		}
		// Decode Payload: [KeyLen(4)][Key][Val]
		if len(payload) < 4 {
			t.Fatal("Set payload too short")
		}
		kLen := binary.BigEndian.Uint32(payload[:4])
		key := string(payload[4 : 4+kLen])
		val := string(payload[4+kLen:])
		if key != "user:1" || val != "alice" {
			t.Errorf("Set payload mismatch: key=%q val=%q", key, val)
		}
		writeResponse(conn, ResStatusOK, nil)

		// 2. Expect GET
		op, payload, _ = readRequest(conn)
		if op != OpCodeGet {
			t.Errorf("Expected Get, got 0x%02x", op)
		}
		if string(payload) != "user:1" {
			t.Errorf("Get key mismatch: %q", string(payload))
		}
		writeResponse(conn, ResStatusOK, []byte("alice"))
	})
	defer ms.Close()

	c, err := NewClient(Config{Address: ms.Addr()})
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer c.Close()

	if err := c.Set("user:1", []byte("alice")); err != nil {
		t.Errorf("Set failed: %v", err)
	}

	val, err := c.Get("user:1")
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if string(val) != "alice" {
		t.Errorf("Get value mismatch: %q", string(val))
	}
}

func TestClient_ReplicaOf_Protocol(t *testing.T) {
	ms := newMockServer(t, func(conn net.Conn) {
		op, payload, _ := readRequest(conn)
		if op != OpCodeReplicaOf {
			t.Errorf("Expected OpCodeReplicaOf, got 0x%02x", op)
		}
		// Protocol: [AddrLen(4)][AddrBytes][RemoteDBNameBytes]
		if len(payload) < 4 {
			t.Fatal("Payload too short")
		}
		addrLen := binary.BigEndian.Uint32(payload[:4])
		addr := string(payload[4 : 4+addrLen])
		remoteDB := string(payload[4+addrLen:])

		if addr != "10.0.0.5:6379" {
			t.Errorf("Addr mismatch: %q", addr)
		}
		if remoteDB != "primary_db" {
			t.Errorf("DB mismatch: %q", remoteDB)
		}
		writeResponse(conn, ResStatusOK, []byte("OK"))
	})
	defer ms.Close()

	c, err := NewClient(Config{Address: ms.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.ReplicaOf("10.0.0.5:6379", "primary_db"); err != nil {
		t.Errorf("ReplicaOf failed: %v", err)
	}
}

func TestClient_ErrorMapping(t *testing.T) {
	tests := []struct {
		name        string
		serverCode  byte
		serverBody  string
		expectedErr error
	}{
		{"NotFound", ResStatusNotFound, "", ErrNotFound},
		{"TxConflict", ResStatusTxConflict, "", ErrTxConflict},
		{"TxRequired", ResStatusTxRequired, "", ErrTxRequired},
		{"TxTimeout", ResStatusTxTimeout, "", ErrTxTimeout},
		{"ServerBusy", ResStatusServerBusy, "", ErrServerBusy},
		{"CustomError", ResStatusErr, "internal failure", nil}, // Special check for message
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newMockServer(t, func(conn net.Conn) {
				readRequest(conn) // consume request
				writeResponse(conn, tt.serverCode, []byte(tt.serverBody))
			})
			defer ms.Close()

			c, err := NewClient(Config{Address: ms.Addr()})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			// Perform any operation
			_, err = c.Get("foo")

			if tt.expectedErr != nil {
				if err != tt.expectedErr {
					t.Errorf("Expected error %v, got %v", tt.expectedErr, err)
				}
			} else {
				// Check ServerError message
				var se *ServerError
				if errors.As(err, &se) {
					if se.Message != tt.serverBody {
						t.Errorf("Expected ServerError message %q, got %q", tt.serverBody, se.Message)
					}
				} else {
					t.Errorf("Expected ServerError type, got %T: %v", err, err)
				}
			}
		})
	}
}

func TestClient_ConnectionFail(t *testing.T) {
	// connect to a port that is likely closed
	_, err := NewClient(Config{
		Address:        "127.0.0.1:59999",
		ConnectTimeout: 10 * time.Millisecond,
	})
	if err == nil {
		t.Error("Expected connection error, got nil")
	}
}

func TestClient_TransactionSequence(t *testing.T) {
	ms := newMockServer(t, func(conn net.Conn) {
		// 1. Begin
		op, _, _ := readRequest(conn)
		if op != OpCodeBegin {
			t.Errorf("Expected Begin, got 0x%02x", op)
		}
		writeResponse(conn, ResStatusOK, nil)

		// 2. Commit
		op, _, _ = readRequest(conn)
		if op != OpCodeCommit {
			t.Errorf("Expected Commit, got 0x%02x", op)
		}
		writeResponse(conn, ResStatusOK, nil)
	})
	defer ms.Close()

	c, err := NewClient(Config{Address: ms.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Begin(); err != nil {
		t.Errorf("Begin failed: %v", err)
	}
	if err := c.Commit(); err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

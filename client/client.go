// Package client provides a thread-safe, mTLS-aware TCP client for TurnstoneDB.
// It maps the binary wire protocol to idiomatic Go methods and handles
// connection lifecycle, timeouts, and error mapping.
package client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// --- Protocol Constants ---

// ProtoHeaderSize is the fixed size (5 bytes) of the request/response header.
// Format: [ OpCode/Status (1 byte) | PayloadLength (4 bytes) ]
const ProtoHeaderSize = 5

// OpCodes define the available commands in the TurnstoneDB wire protocol.
// These must match the server-side definitions in constants.go.
const (
	OpCodePing      = 0x01 // Health check. Expects "PONG".
	OpCodeGet       = 0x02 // Retrieve a value. Payload: Key bytes.
	OpCodeSet       = 0x03 // Store a value. Payload: [KeyLen(4)|Key|Value].
	OpCodeDel       = 0x04 // Delete a value. Payload: Key bytes.
	OpCodeSelect    = 0x05 // Select the active partition. Payload: Partition Name bytes.
	OpCodeBegin     = 0x10 // Start a new transaction context.
	OpCodeCommit    = 0x11 // Commit the current transaction.
	OpCodeAbort     = 0x12 // Rollback the current transaction.
	OpCodeStat      = 0x20 // Retrieve server statistics.
	OpCodeReplicaOf = 0x32 // Set replication source. Payload: [AddrLen][Addr][RemotePartition].
	OpCodeReplHello = 0x50 // Replication Handshake (Internal/Client CDC).
	OpCodeReplBatch = 0x51 // Replication Batch (Internal/Client CDC).
	OpCodeReplAck   = 0x52 // Replication Ack (Internal/Client CDC).
	OpCodeQuit      = 0xFF // Gracefully close the connection.
)

const (
	OpJournalSet    = 1
	OpJournalDelete = 2
)

// Response Codes indicate the status of a request.
// These are returned in the first byte of the response header.
const (
	ResStatusOK             = 0x00 // Operation successful.
	ResStatusErr            = 0x01 // Generic server-side error.
	ResStatusNotFound       = 0x02 // Key does not exist.
	ResStatusTxRequired     = 0x03 // Operation requires an active transaction.
	ResStatusTxTimeout      = 0x04 // Transaction exceeded MaxTxDuration.
	ResStatusTxConflict     = 0x05 // Write conflict or snapshot invalidation.
	ResStatusServerBusy     = 0x06 // Server reached MaxConns.
	ResStatusEntityTooLarge = 0x07 // Payload exceeds MaxCommandSize.
	ResStatusMemoryLimit    = 0x08 // Server OOM protection triggered.
)

// --- Errors ---

var (
	// ErrNotFound is returned when a requested key does not exist.
	ErrNotFound = errors.New("key not found")
	// ErrInvalidKey is returned when the key contains non-ASCII characters.
	ErrInvalidKey = errors.New("key must contain only ASCII characters")
	// ErrTxRequired is returned when attempting a read/write without calling Begin().
	ErrTxRequired = errors.New("transaction required for this operation")
	// ErrTxTimeout is returned when the transaction exceeds the server's MaxTxDuration.
	ErrTxTimeout = errors.New("transaction timed out")
	// ErrTxConflict is returned when an Optimistic Concurrency Control conflict occurs.
	ErrTxConflict = errors.New("transaction conflict detected")
	// ErrServerBusy is returned when the server is at maximum capacity.
	ErrServerBusy = errors.New("server is busy")
	// ErrEntityTooLarge is returned when the payload exceeds the server's buffer limits.
	ErrEntityTooLarge = errors.New("entity too large")
	// ErrMemoryLimit is returned when the server rejects a write to protect memory usage.
	ErrMemoryLimit = errors.New("server memory limit exceeded")
	// ErrConnection is returned for network-level failures (timeouts, resets).
	ErrConnection = errors.New("connection error")
)

// ServerError represents a generic error returned by the server (ResStatusErr)
// containing a human-readable message.
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

// mapStatusToError converts a raw protocol status byte into a typed Go error.
func mapStatusToError(status byte, body []byte) error {
	switch status {
	case ResStatusOK:
		return nil
	case ResStatusErr:
		return &ServerError{Message: string(body)}
	case ResStatusNotFound:
		return ErrNotFound
	case ResStatusTxRequired:
		return ErrTxRequired
	case ResStatusTxTimeout:
		return ErrTxTimeout
	case ResStatusTxConflict:
		return ErrTxConflict
	case ResStatusServerBusy:
		return ErrServerBusy
	case ResStatusEntityTooLarge:
		return ErrEntityTooLarge
	case ResStatusMemoryLimit:
		return ErrMemoryLimit
	default:
		return fmt.Errorf("unknown server status code: 0x%02x, body: %s", status, string(body))
	}
}

// --- Client Implementation ---

// Config holds the connection settings for the client.
type Config struct {
	// Address is the host:port of the TurnstoneDB server (e.g., "localhost:6379").
	Address string

	// ConnectTimeout limits how long the client waits to establish the TCP connection.
	// Default: 5 seconds.
	ConnectTimeout time.Duration

	// ReadTimeout limits how long the client waits for a server response.
	// Default: 0 (No timeout).
	ReadTimeout time.Duration

	// WriteTimeout limits how long the client waits to send a request.
	// Default: 0 (No timeout).
	WriteTimeout time.Duration

	// TLSConfig contains the mTLS credentials.
	// If nil, the client attempts a plaintext TCP connection (which the server may reject).
	TLSConfig *tls.Config

	// Logger allows injecting a structured logger (slog).
	// If nil, logging is discarded.
	Logger *slog.Logger
}

// Client is a thread-safe, synchronous Key-Value store client.
// It manages a single persistent TCP connection. Use a pool of Clients for high concurrency.
type Client struct {
	conn   net.Conn
	mu     sync.Mutex
	config Config
	logger *slog.Logger
}

// NewClient creates a new client and attempts to connect immediately.
// Returns an error if the connection fails.
func NewClient(cfg Config) (*Client, error) {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}

	// Default to discard logger if nil
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	client := &Client{
		config: cfg,
		logger: logger,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// NewMTLSClientHelper is a convenience constructor for creating an mTLS-secured client.
// It handles reading the CA, Cert, and Key files from disk and setting up the TLS config.
func NewMTLSClientHelper(addr, caFile, certFile, keyFile string, logger *slog.Logger) (*Client, error) {
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate PEM")
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client keypair: %w", err)
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{cert},
	}

	return NewClient(Config{
		Address:   addr,
		TLSConfig: tlsConfig,
		Logger:    logger,
	})
}

// connect establishes the underlying TCP/TLS connection.
func (c *Client) connect() error {
	dialer := net.Dialer{Timeout: c.config.ConnectTimeout}
	var err error
	var conn net.Conn

	if c.config.TLSConfig != nil {
		conn, err = tls.DialWithDialer(&dialer, "tcp", c.config.Address, c.config.TLSConfig)
	} else {
		conn, err = dialer.Dial("tcp", c.config.Address)
	}

	if err != nil {
		c.logger.Error("Connection failed", "addr", c.config.Address, "err", err)
		return err
	}

	c.conn = conn
	c.logger.Info("Connected", "addr", c.config.Address)
	return nil
}

// Close gracefully terminates the TCP connection.
// It is safe to call Close multiple times.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.logger.Info("Closing connection", "addr", c.config.Address)
		return c.conn.Close()
	}
	return nil
}

// roundTrip handles the low-level request/response cycle.
// It serializes the request, sends it, reads the response, and maps status codes to errors.
// This method is protected by a mutex to ensure atomic command execution on the shared connection.
func (c *Client) roundTrip(op byte, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return nil, ErrConnection
	}

	// Set Deadlines if configured
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}

	// 1. Write Header: [ OpCode(1) | Length(4) ]
	reqHeader := make([]byte, ProtoHeaderSize)
	reqHeader[0] = op
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(len(payload)))

	if _, err := c.conn.Write(reqHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: write header failed: %v", ErrConnection, err)
	}

	// 2. Write Body (if any)
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: write payload failed: %v", ErrConnection, err)
		}
	}

	// 3. Read Response Header: [ Status(1) | Length(4) ]
	respHeader := make([]byte, ProtoHeaderSize)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: read header failed: %v", ErrConnection, err)
	}

	status := respHeader[0]
	length := binary.BigEndian.Uint32(respHeader[1:])

	// 4. Read Response Body
	var body []byte
	if length > 0 {
		body = make([]byte, length)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: read body failed: %v", ErrConnection, err)
		}
	}

	// 5. Map Status to Error
	return body, mapStatusToError(status, body)
}

// --- Commands ---

// Ping sends a health check request. Returns nil if the server responds "PONG".
func (c *Client) Ping() error {
	_, err := c.roundTrip(OpCodePing, nil)
	return err
}

// Select changes the active partition for the current connection.
func (c *Client) Select(partitionName string) error {
	_, err := c.roundTrip(OpCodeSelect, []byte(partitionName))
	return err
}

// ReplicaOf configures the currently selected partition to replicate from a remote source.
// sourceAddr is "host:port", sourcePartition is the name of the partition on the remote server.
func (c *Client) ReplicaOf(sourceAddr, sourcePartition string) error {
	// Protocol: [AddrLen(4)][AddrBytes][RemotePartitionNameBytes]
	addrBytes := []byte(sourceAddr)
	partBytes := []byte(sourcePartition)
	payload := make([]byte, 4+len(addrBytes)+len(partBytes))

	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], partBytes)

	_, err := c.roundTrip(OpCodeReplicaOf, payload)
	return err
}

// Get retrieves the value for a given key.
// Requires an active transaction (call Begin() first).
func (c *Client) Get(key string) ([]byte, error) {
	if !isASCII(key) {
		return nil, ErrInvalidKey
	}
	return c.roundTrip(OpCodeGet, []byte(key))
}

// Set creates or updates a key-value pair.
// Requires an active transaction.
func (c *Client) Set(key string, value []byte) error {
	if !isASCII(key) {
		return ErrInvalidKey
	}
	kBytes := []byte(key)
	// Protocol format for SET:
	// [ 4 bytes KeyLen ] [ Key Bytes ] [ Value Bytes ]
	payload := make([]byte, 4+len(kBytes)+len(value))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(kBytes)))
	copy(payload[4:], kBytes)
	copy(payload[4+len(kBytes):], value)

	_, err := c.roundTrip(OpCodeSet, payload)
	return err
}

// Del removes a key.
// Requires an active transaction.
func (c *Client) Del(key string) error {
	if !isASCII(key) {
		return ErrInvalidKey
	}
	_, err := c.roundTrip(OpCodeDel, []byte(key))
	return err
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > 127 {
			return false
		}
	}
	return true
}

// --- Transaction Support ---

// Begin starts a new transaction context on the server.
// The client enters a transaction state; subsequent commands will be buffered
// or snapshot-isolated until Commit() or Abort() is called.
func (c *Client) Begin() error {
	_, err := c.roundTrip(OpCodeBegin, nil)
	return err
}

// Commit attempts to apply the buffered transaction operations.
// Returns ErrTxConflict if the snapshot is invalid due to concurrent writes.
func (c *Client) Commit() error {
	_, err := c.roundTrip(OpCodeCommit, nil)
	return err
}

// Abort rolls back the current transaction and releases server-side resources.
func (c *Client) Abort() error {
	_, err := c.roundTrip(OpCodeAbort, nil)
	return err
}

// --- Admin ---

// Stat returns the internal server statistics (key count, memory usage, uptime).
func (c *Client) Stat() (string, error) {
	resp, err := c.roundTrip(OpCodeStat, nil)
	return string(resp), err
}

// --- CDC / Replication Consumer ---

// Change represents a single data modification event from the CDC stream.
type Change struct {
	LogSeq   uint64 // The global operation sequence number
	TxID     uint64 // The transaction ID this change belongs to
	Key      []byte
	Value    []byte // nil if IsDelete is true
	IsDelete bool
}

// Subscribe connects to the server and starts listening for changes on the specified partition.
// This function blocks until the connection is closed or an error occurs.
// It invokes 'handler' for every change received.
// Note: This method consumes the Client connection. You should create a dedicated Client instance for CDC.
func (c *Client) Subscribe(partition string, startSeq uint64, handler func(Change) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		return ErrConnection
	}

	// 1. Send Hello Handshake
	// Format: [Ver:4][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version
	binary.Write(buf, binary.BigEndian, uint32(1)) // Count (1 DB)

	binary.Write(buf, binary.BigEndian, uint32(len(partition)))
	buf.WriteString(partition)
	binary.Write(buf, binary.BigEndian, startSeq)

	// Send Header
	reqHeader := make([]byte, ProtoHeaderSize)
	reqHeader[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(buf.Len()))

	if _, err := c.conn.Write(reqHeader); err != nil {
		return err
	}
	// Send Body
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		return err
	}

	// 2. Loop Read
	respHeader := make([]byte, ProtoHeaderSize)
	for {
		// Read Header
		if _, err := io.ReadFull(c.conn, respHeader); err != nil {
			return err
		}

		opCode := respHeader[0]
		length := binary.BigEndian.Uint32(respHeader[1:])

		// Read Payload
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return err
		}

		if opCode == OpCodeReplBatch {
			// Parse Batch: [DBNameLen][DBName][Count][Data...]
			cursor := 0
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed batch: short header")
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			if cursor+nLen > len(payload) {
				return fmt.Errorf("malformed batch: short name")
			}
			// dbName := string(payload[cursor : cursor+nLen]) // verify partition match?
			cursor += nLen

			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed batch: short count")
			}
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4

			data := payload[cursor:]
			dCursor := 0
			var maxLogSeq uint64

			for i := 0; i < int(count); i++ {
				// Entry: [LogID(8)][TxID(8)][Op(1)][KLen(4)][Key][VLen(4)][Val]
				if dCursor+17 > len(data) {
					return fmt.Errorf("malformed entry header")
				}
				logSeq := binary.BigEndian.Uint64(data[dCursor : dCursor+8])
				txID := binary.BigEndian.Uint64(data[dCursor+8 : dCursor+16])
				opType := data[dCursor+16]
				dCursor += 17

				kLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				if dCursor+kLen > len(data) {
					return fmt.Errorf("malformed entry key")
				}
				key := data[dCursor : dCursor+kLen]
				dCursor += kLen

				vLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				if dCursor+vLen > len(data) {
					return fmt.Errorf("malformed entry val")
				}
				val := data[dCursor : dCursor+vLen]
				dCursor += vLen

				change := Change{
					LogSeq:   logSeq,
					TxID:     txID,
					Key:      key,
					Value:    val,
					IsDelete: opType == OpJournalDelete,
				}

				if err := handler(change); err != nil {
					return err
				}

				if logSeq > maxLogSeq {
					maxLogSeq = logSeq
				}
			}

			// Send ACK
			// ACK Format: [DBNameLen][DBName][LogID]
			ackBuf := new(bytes.Buffer)
			binary.Write(ackBuf, binary.BigEndian, uint32(len(partition)))
			ackBuf.WriteString(partition)
			binary.Write(ackBuf, binary.BigEndian, maxLogSeq)

			ackHeader := make([]byte, ProtoHeaderSize)
			ackHeader[0] = OpCodeReplAck
			binary.BigEndian.PutUint32(ackHeader[1:], uint32(ackBuf.Len()))

			if _, err := c.conn.Write(ackHeader); err != nil {
				return err
			}
			if _, err := c.conn.Write(ackBuf.Bytes()); err != nil {
				return err
			}

		} else {
			// Ignore other opcodes or handle error/quit
			if opCode == ResStatusErr {
				return &ServerError{Message: string(payload)}
			}
		}
	}
}

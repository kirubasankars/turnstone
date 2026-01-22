// Package client provides a thread-safe, mTLS-aware TCP client for TurnstoneDB.
// It maps the binary wire protocol to idiomatic Go methods and handles
// connection lifecycle, timeouts, and error mapping.
package client

import (
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
	OpCodeReplHello = 0x50 // Replication Handshake (Internal).
	OpCodeReplBatch = 0x51 // Replication Batch (Internal).
	OpCodeReplAck   = 0x52 // Replication Ack (Internal).
	OpCodeQuit      = 0xFF // Gracefully close the connection.
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

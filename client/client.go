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
	OpCodePing             = 0x01 // Health check. Expects "PONG".
	OpCodeGet              = 0x02 // Retrieve a value. Payload: Key bytes.
	OpCodeSet              = 0x03 // Store a value. Payload: [KeyLen(4)|Key|Value].
	OpCodeDel              = 0x04 // Delete a value. Payload: Key bytes.
	OpCodeSelect           = 0x05 // Select the active database. Payload: Database Name bytes.
	OpCodeMGet             = 0x06 // Multi-Get. Payload: [NumKeys(4)][KLen(4)|Key]...
	OpCodeMSet             = 0x07 // Multi-Set. Payload: [NumPairs(4)][KLen(4)|Key|VLen(4)|Val]...
	OpCodeMDel             = 0x08 // Multi-Del. Payload: [NumKeys(4)][KLen(4)|Key]...
	OpCodeBegin            = 0x10 // Start a new transaction context.
	OpCodeCommit           = 0x11 // Commit the current transaction.
	OpCodeAbort            = 0x12 // Rollback the current transaction.
	OpCodeReplicaOf        = 0x32 // Set replication source. Payload: [AddrLen][Addr][RemoteDB].
	OpCodeReplHello        = 0x50 // Replication Handshake (Internal/Client CDC).
	OpCodeReplBatch        = 0x51 // Replication Batch (Internal/Client CDC).
	OpCodeReplAck          = 0x52 // Replication Ack (Internal/Client CDC).
	OpCodeReplSnapshot     = 0x53 // Snapshot Batch (Full Sync).
	OpCodeReplSnapshotDone = 0x54 // Snapshot Complete Signal.
	OpCodeQuit             = 0xFF // Gracefully close the connection.
)

const (
	OpJournalSet    = 1
	OpJournalDelete = 2
	OpJournalCommit = 3
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
	ResStatusTxInProgress   = 0x06 // Transaction is already active.
	ResStatusTxCommitted    = 0x0A // Transaction committed successfully.
	ResStatusServerBusy     = 0x07 // Server reached MaxConns.
	ResStatusEntityTooLarge = 0x08 // Payload exceeds MaxCommandSize.
	ResStatusMemoryLimit    = 0x09 // Server OOM protection triggered.
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
	// ErrTxInProgress is returned when trying to Begin a transaction while one is already active.
	ErrTxInProgress = errors.New("transaction already in progress")
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
	case ResStatusTxInProgress:
		return ErrTxInProgress
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

	// ClientID is the unique identifier for this client (used for replication/CDC).
	// If empty, it defaults to "client-unknown".
	ClientID string

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
	conn     net.Conn
	mu       sync.Mutex
	config   Config
	logger   *slog.Logger
	pool     *Pool
	poisoned bool
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
// If the client was obtained from a Pool, it returns the connection to the pool (if healthy).
// It is safe to call Close multiple times.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If belonging to a pool, try to return it
	if c.pool != nil {
		if c.poisoned {
			c.pool.reportClosed() // Decrement count
			if c.conn != nil {
				c.logger.Info("Closing poisoned connection", "addr", c.config.Address)
				return c.conn.Close()
			}
			return nil
		}
		// Return to pool
		select {
		case c.pool.idle <- c:
			// successfully returned
			return nil
		default:
			// Pool is full (shouldn't happen if logic is correct, but safe fallback)
			c.pool.reportClosed()
			if c.conn != nil {
				return c.conn.Close()
			}
			return nil
		}
	}

	if c.conn != nil {
		c.logger.Info("Closing connection", "addr", c.config.Address)
		return c.conn.Close()
	}
	return nil
}

// markPoisoned marks the client as unusable, forcing it to be closed instead of returned to the pool.
func (c *Client) markPoisoned() {
	c.poisoned = true
}

// --- Connection Pool ---

// Pool manages a set of reusable Client connections.
type Pool struct {
	config Config
	idle   chan *Client
	maxCap int

	mu         sync.Mutex
	currentCap int
	closed     bool
}

// NewClientPool creates a connection pool with a maximum capacity.
func NewClientPool(cfg Config, maxConns int) (*Pool, error) {
	if maxConns <= 0 {
		return nil, errors.New("maxConns must be > 0")
	}
	// Wrap logger
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Pool{
		config: cfg,
		idle:   make(chan *Client, maxConns),
		maxCap: maxConns,
	}, nil
}

// Get retrieves a Client from the pool.
// If an idle connection is available, it is returned.
// If not, and the pool is not full, a new connection is created.
// If the pool is full, this method blocks until a connection is returned.
func (p *Pool) Get() (*Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("pool is closed")
	}

	// 1. Try to get idle
	select {
	case c := <-p.idle:
		p.mu.Unlock()
		return c, nil
	default:
		// No idle connections
	}

	// 2. Check capacity
	if p.currentCap < p.maxCap {
		p.currentCap++
		p.mu.Unlock()

		// Create new client
		c, err := NewClient(p.config)
		if err != nil {
			p.reportClosed() // Revert count
			return nil, err
		}
		c.pool = p
		return c, nil
	}

	p.mu.Unlock()

	// 3. Wait for idle
	select {
	case c, ok := <-p.idle:
		if !ok {
			return nil, errors.New("pool is closed")
		}
		return c, nil
		// TODO: Add timeout support for Get? For now block forever as per standard chan semantics
	}
}

// reportClosed decrements the active connection count.
func (p *Pool) reportClosed() {
	p.mu.Lock()
	p.currentCap--
	p.mu.Unlock()
}

// Close closes the pool and all idle connections.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.idle) // interrupt wait in Get()
	p.mu.Unlock()

	for c := range p.idle {
		if c.conn != nil {
			c.conn.Close()
		}
	}
}

// roundTrip handles the low-level request/response cycle.
// It serializes the request, sends it, reads the response, and maps status codes to errors.
// This method is protected by a mutex to ensure atomic command execution on the shared connection.
func (c *Client) roundTrip(op byte, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn == nil {
		c.markPoisoned()
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
		c.markPoisoned()
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: write header failed: %v", ErrConnection, err)
	}

	// 2. Write Body (if any)
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.markPoisoned()
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: write payload failed: %v", ErrConnection, err)
		}
	}

	// 3. Read Response Header: [ Status(1) | Length(4) ]
	respHeader := make([]byte, ProtoHeaderSize)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.markPoisoned()
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
			c.markPoisoned()
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

// Select changes the active database for the current connection.
func (c *Client) Select(dbName string) error {
	_, err := c.roundTrip(OpCodeSelect, []byte(dbName))
	return err
}

// ReplicaOf configures the currently selected database to replicate from a remote source.
// sourceAddr is "host:port", sourceDB is the name of the database on the remote server.
func (c *Client) ReplicaOf(sourceAddr, sourceDB string) error {
	// Protocol: [AddrLen(4)][AddrBytes][RemoteDBNameBytes]
	addrBytes := []byte(sourceAddr)
	dbBytes := []byte(sourceDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))

	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)

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

// MGet retrieves values for multiple keys.
// Requires an active transaction.
// Returns a slice of byte slices matching the order of input keys.
// If a key does not exist, the corresponding slice is nil.
func (c *Client) MGet(keys ...string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(keys)))
	for _, k := range keys {
		if !isASCII(k) {
			return nil, ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
	}

	payload, err := c.roundTrip(OpCodeMGet, buf.Bytes())
	if err != nil {
		return nil, err
	}

	// Parse Response: [NumValues(4)] [ [Len(4)][Val] ... ]
	if len(payload) < 4 {
		return nil, errors.New("invalid mget response")
	}
	count := binary.BigEndian.Uint32(payload[0:4])
	if int(count) != len(keys) {
		return nil, fmt.Errorf("mget count mismatch: expected %d, got %d", len(keys), count)
	}

	results := make([][]byte, count)
	offset := 4
	for i := 0; i < int(count); i++ {
		if offset+4 > len(payload) {
			return nil, errors.New("malformed mget response")
		}
		valLen := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4

		// Sentinel for NIL (0xFFFFFFFF)
		if valLen == 0xFFFFFFFF {
			results[i] = nil
			continue
		}

		if offset+int(valLen) > len(payload) {
			return nil, errors.New("malformed mget response value")
		}
		results[i] = make([]byte, valLen)
		copy(results[i], payload[offset:offset+int(valLen)])
		offset += int(valLen)
	}

	return results, nil
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

// MSet creates or updates multiple key-value pairs.
// Requires an active transaction.
func (c *Client) MSet(entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(entries)))

	for k, v := range entries {
		if !isASCII(k) {
			return ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
		binary.Write(buf, binary.BigEndian, uint32(len(v)))
		buf.Write(v)
	}

	_, err := c.roundTrip(OpCodeMSet, buf.Bytes())
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

// MDel removes multiple keys.
// Requires an active transaction.
// Returns the number of keys actually deleted (that existed).
func (c *Client) MDel(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(keys)))
	for _, k := range keys {
		if !isASCII(k) {
			return 0, ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
	}

	payload, err := c.roundTrip(OpCodeMDel, buf.Bytes())
	if err != nil {
		return 0, err
	}

	if len(payload) < 4 {
		return 0, errors.New("invalid mdel response")
	}
	count := binary.BigEndian.Uint32(payload[0:4])
	return int(count), nil
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

// --- CDC / Replication Consumer ---

// Change represents a single data modification event from the CDC stream.
type Change struct {
	LogSeq   uint64 // The global operation sequence number (0 for snapshot entries)
	TxID     uint64 // The transaction ID this change belongs to
	Key      []byte
	Value    []byte // nil if IsDelete is true
	IsDelete bool

	// IsSnapshotDone indicates that the full-sync snapshot is complete.
	// When true, LogSeq contains the ResumeSeq to start the WAL stream from.
	IsSnapshotDone bool
}

// Subscribe connects to the server and starts listening for changes on the specified database.
// This function blocks until the connection is closed or an error occurs.
// It invokes 'handler' for every change received.
// Note: This method consumes the Client connection. You should create a dedicated Client instance for CDC.
func (c *Client) Subscribe(dbName string, startSeq uint64, handler func(Change) error) error {
	c.mu.Lock()
	// Removed defer c.mu.Unlock() to prevent deadlock during Close()

	if c.conn == nil {
		c.mu.Unlock() // Release lock
		return ErrConnection
	}

	clientID := c.config.ClientID
	if clientID == "" {
		clientID = "client-unknown"
	}

	// 1. Send Hello Handshake
	// Format: [Ver:4][IDLen:4][ID][NumDBs:4] ... [NameLen:4][Name][LogID:8]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1)) // Version

	binary.Write(buf, binary.BigEndian, uint32(len(clientID))) // ID Len
	buf.WriteString(clientID)                                  // ID Bytes

	binary.Write(buf, binary.BigEndian, uint32(1)) // Count (1 DB)

	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, startSeq)

	// Send Header
	reqHeader := make([]byte, ProtoHeaderSize)
	reqHeader[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(buf.Len()))

	if _, err := c.conn.Write(reqHeader); err != nil {
		c.mu.Unlock() // Release lock
		return err
	}
	// Send Body
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.mu.Unlock() // Release lock
		return err
	}

	// Release lock BEFORE entering the blocking loop
	c.mu.Unlock()

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
			// dbName := string(payload[cursor : cursor+nLen]) // verify database match?
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

				// Skip Commit Markers in the event stream as they are just signals
				if opType == OpJournalCommit { // OpJournalCommit from Protocol
					if logSeq > maxLogSeq {
						maxLogSeq = logSeq
					}
					continue
				}

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
			binary.Write(ackBuf, binary.BigEndian, uint32(len(dbName)))
			ackBuf.WriteString(dbName)
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

		} else if opCode == OpCodeReplSnapshot {
			// Parse Snapshot Batch: [DBNameLen][DBName][Count][Data...]
			// Data format: [KLen][Key][VLen][Val] ...
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]
			dCursor := 0

			for i := 0; i < int(count); i++ {
				kLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				key := data[dCursor : dCursor+kLen]
				dCursor += kLen
				vLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				val := data[dCursor : dCursor+vLen]
				dCursor += vLen

				// Snapshot events have LogSeq 0
				change := Change{
					LogSeq:   0,
					TxID:     0,
					Key:      key,
					Value:    val,
					IsDelete: false, // Snapshots only contain live data
				}
				if err := handler(change); err != nil {
					return err
				}
			}

		} else if opCode == OpCodeReplSnapshotDone {
			// Snapshot Done Signal: [DBNameLen][DBName][0][Data...]
			// Data from server is [TxID(8)][OpID(8)]. We need OpID as the resume sequence.
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen + 4 // Skip Name + Count(0)

			// FIX: Handle the 16-byte payload from server [TxID][OpID] correctly.
			// The server sends [TxID(8)][OpID(8)]. We want OpID.
			if cursor+16 <= len(payload) {
				resumeSeq := binary.BigEndian.Uint64(payload[cursor+8:])
				c.logger.Info("Snapshot sync complete", "resume_seq", resumeSeq)

				// Notify handler so it can persist the state
				if err := handler(Change{IsSnapshotDone: true, LogSeq: resumeSeq}); err != nil {
					return err
				}
			} else if cursor+8 <= len(payload) {
				// Fallback: If server only sent 8 bytes (Old version?), use it.
				resumeSeq := binary.BigEndian.Uint64(payload[cursor:])
				c.logger.Info("Snapshot sync complete (short payload)", "resume_seq", resumeSeq)
				if err := handler(Change{IsSnapshotDone: true, LogSeq: resumeSeq}); err != nil {
					return err
				}
			}

		} else {
			// Ignore other opcodes or handle error/quit
			if opCode == ResStatusErr {
				return &ServerError{Message: string(payload)}
			}
		}
	}
}

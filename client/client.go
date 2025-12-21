package client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// --- Protocol Constants ---

// OpCodes (Must match server)
const (
	OpCodePing    = 0x01
	OpCodeGet     = 0x02
	OpCodeSet     = 0x03
	OpCodeDel     = 0x04
	OpCodeBegin   = 0x10
	OpCodeCommit  = 0x11
	OpCodeAbort   = 0x12
	OpCodeSelect  = 0x13
	OpCodeStat    = 0x20
	OpCodeCompact = 0x21
	OpCodeQuit    = 0xFF
)

// Response Codes
const (
	ResStatusOK             = 0x00
	ResStatusErr            = 0x01
	ResStatusNotFound       = 0x02
	ResStatusTxRequired     = 0x03
	ResStatusTxTimeout      = 0x04
	ResStatusTxConflict     = 0x05
	ResStatusServerBusy     = 0x06
	ResStatusEntityTooLarge = 0x07
	ResStatusGenMismatch    = 0x09
)

// --- Errors ---

// Predefined errors for application logic handling
var (
	ErrNotFound       = errors.New("key not found")
	ErrTxRequired     = errors.New("transaction required for this operation")
	ErrTxTimeout      = errors.New("transaction timed out")
	ErrTxConflict     = errors.New("transaction conflict detected")
	ErrServerBusy     = errors.New("server is busy")
	ErrEntityTooLarge = errors.New("entity too large")
	ErrGenMismatch    = errors.New("generation mismatch")
	ErrConnection     = errors.New("connection error")
)

// ServerError represents a generic error returned by the server (ResStatusErr)
type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

// mapStatusToError converts protocol status bytes to Go errors
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
	case ResStatusGenMismatch:
		return ErrGenMismatch
	default:
		return fmt.Errorf("unknown server status code: 0x%02x, body: %s", status, string(body))
	}
}

// --- Client Implementation ---

// Config holds the connection settings
type Config struct {
	Address        string        // e.g. "localhost:6379"
	ConnectTimeout time.Duration // Default: 5s
	ReadTimeout    time.Duration // Default: no timeout
	WriteTimeout   time.Duration // Default: no timeout
	TLSConfig      *tls.Config   // If nil, uses insecure TCP
}

// Client is a thread-safe KV store client
type Client struct {
	conn   net.Conn
	mu     sync.Mutex // Protects concurrent access to the connection
	config Config
}

// NewClient creates a new client and connects immediately
func NewClient(cfg Config) (*Client, error) {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}

	client := &Client{config: cfg}
	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// NewMTLSClientHelper is a utility to load certificates and create a client easily
func NewMTLSClientHelper(addr, caFile, certFile, keyFile string) (*Client, error) {
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
	})
}

// connect handles the raw network dialing
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
		return err
	}
	c.conn = conn
	return nil
}

// Close terminates the connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// roundTrip handles the low-level request/response cycle safely
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

	// 1. Write Header
	reqHeader := make([]byte, 5)
	reqHeader[0] = op
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(len(payload)))

	if _, err := c.conn.Write(reqHeader); err != nil {
		c.conn.Close() // Force reconnect on next usage logic (advanced) or just fail
		return nil, fmt.Errorf("%w: write header failed: %v", ErrConnection, err)
	}

	// 2. Write Body
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.conn.Close()
			return nil, fmt.Errorf("%w: write payload failed: %v", ErrConnection, err)
		}
	}

	// 3. Read Response Header
	respHeader := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
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
			return nil, fmt.Errorf("%w: read body failed: %v", ErrConnection, err)
		}
	}

	// 5. Map Status to Error
	return body, mapStatusToError(status, body)
}

// --- Commands ---

// Ping checks server connectivity
func (c *Client) Ping() error {
	_, err := c.roundTrip(OpCodePing, nil)
	return err
}

// Select changes the active database index (0-255)
func (c *Client) Select(dbIndex int) error {
	if dbIndex < 0 || dbIndex > 255 {
		return fmt.Errorf("invalid db index: %d", dbIndex)
	}
	_, err := c.roundTrip(OpCodeSelect, []byte{byte(dbIndex)})
	return err
}

// Get retrieves a value by key. Returns ErrNotFound if missing.
func (c *Client) Get(key string) ([]byte, error) {
	return c.roundTrip(OpCodeGet, []byte(key))
}

// Set stores a key-value pair.
func (c *Client) Set(key string, value []byte) error {
	kBytes := []byte(key)
	// Protocol format for SET: [4 bytes KeyLen][KeyBytes][ValueBytes]
	payload := make([]byte, 4+len(kBytes)+len(value))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(kBytes)))
	copy(payload[4:], kBytes)
	copy(payload[4+len(kBytes):], value)

	_, err := c.roundTrip(OpCodeSet, payload)
	return err
}

// Del removes a key.
func (c *Client) Del(key string) error {
	_, err := c.roundTrip(OpCodeDel, []byte(key))
	return err
}

// --- Transaction Support ---

func (c *Client) Begin() error {
	_, err := c.roundTrip(OpCodeBegin, nil)
	return err
}

func (c *Client) Commit() error {
	_, err := c.roundTrip(OpCodeCommit, nil)
	return err
}

func (c *Client) Abort() error {
	_, err := c.roundTrip(OpCodeAbort, nil)
	return err
}

// --- Admin ---

func (c *Client) Stat() (string, error) {
	resp, err := c.roundTrip(OpCodeStat, nil)
	return string(resp), err
}

func (c *Client) Compact() error {
	_, err := c.roundTrip(OpCodeCompact, nil)
	return err
}

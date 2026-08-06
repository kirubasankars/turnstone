// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package client

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"turnstone/internal/tlsutil"
	"turnstone/protocol"
)

// defaultIOTimeout is used for Config.ReadTimeout/WriteTimeout when the
// caller leaves them unset (zero). Without a default, a slow or
// network-partitioned server leaves roundTrip() blocked in
// I/O forever with no way for the caller to notice or recover.
const defaultIOTimeout = 30 * time.Second

var (
	ErrNotFound       = errors.New("key not found")
	ErrInvalidKey     = errors.New("key must contain only ASCII characters")
	ErrTxRequired     = errors.New("transaction required for this operation")
	ErrTxTimeout      = errors.New("transaction timed out")
	ErrTxConflict     = errors.New("transaction conflict detected")
	ErrTxInProgress   = errors.New("transaction already in progress")
	ErrServerBusy     = errors.New("server is busy")
	ErrEntityTooLarge = errors.New("entity too large")
	ErrMemoryLimit    = errors.New("server memory limit exceeded")
	ErrConnection     = errors.New("connection error")
)

type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

func mapStatusToError(status byte, body []byte) error {
	switch status {
	case protocol.ResStatusOK:
		return nil
	case protocol.ResStatusErr:
		return &ServerError{Message: string(body)}
	case protocol.ResStatusNotFound:
		return ErrNotFound
	case protocol.ResStatusTxRequired:
		return ErrTxRequired
	case protocol.ResStatusTxTimeout:
		return ErrTxTimeout
	case protocol.ResStatusTxConflict:
		return ErrTxConflict
	case protocol.ResStatusTxInProgress:
		return ErrTxInProgress
	case protocol.ResStatusServerBusy:
		return ErrServerBusy
	case protocol.ResStatusEntityTooLarge:
		return ErrEntityTooLarge
	case protocol.ResStatusMemoryLimit:
		return ErrMemoryLimit
	default:
		return fmt.Errorf("unknown server status code: 0x%02x, body: %s", status, string(body))
	}
}

type Config struct {
	Address        string
	ClientID       string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	TLSConfig      *tls.Config
	Logger         *slog.Logger
}

type Client struct {
	conn   net.Conn
	mu     sync.Mutex
	config Config
	logger *slog.Logger
	closed bool
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultIOTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultIOTimeout
	}
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

func NewMTLSClientHelper(addr, caFile, certFile, keyFile string, logger *slog.Logger) (*Client, error) {
	tlsConfig, err := tlsutil.LoadMTLS(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return NewClient(Config{
		Address:   addr,
		TLSConfig: tlsConfig,
		Logger:    logger,
	})
}

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

// Close closes the underlying connection. It is idempotent.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.conn != nil {
		c.logger.Info("Closing connection", "addr", c.config.Address)
		return c.conn.Close()
	}
	return nil
}

func (c *Client) roundTrip(op byte, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil, ErrConnection
	}
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}
	if _, err := c.conn.Write(protocol.EncodeFrame(op, payload)); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: write frame failed: %v", ErrConnection, err)
	}
	respHeader := make([]byte, protocol.ProtoHeaderSize)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: read header failed: %v", ErrConnection, err)
	}
	status := respHeader[0]
	length := binary.BigEndian.Uint32(respHeader[1:])
	var body []byte
	if length > 0 {
		body = make([]byte, length)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: read body failed: %v", ErrConnection, err)
		}
	}
	return body, mapStatusToError(status, body)
}

func (c *Client) Ping() error {
	_, err := c.roundTrip(protocol.OpCodePing, nil)
	return err
}

func (c *Client) Select(dbName string) error {
	_, err := c.roundTrip(protocol.OpCodeSelect, []byte(dbName))
	return err
}

func (c *Client) ReplicaOf(sourceAddr, sourceDB string) error {
	addrBytes := []byte(sourceAddr)
	dbBytes := []byte(sourceDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)
	_, err := c.roundTrip(protocol.OpCodeReplicaOf, payload)
	return err
}

func (c *Client) Promote(minReplicas int) error {
	// A negative value wraps around through the uint32 cast below (e.g.
	// -1 becomes 4294967295), silently requesting a quorum the server can
	// never satisfy. Reject it here instead of sending nonsense over the
	// wire.
	if minReplicas < 0 {
		return fmt.Errorf("minReplicas must be >= 0, got %d", minReplicas)
	}
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(minReplicas))
	_, err := c.roundTrip(protocol.OpCodePromote, payload)
	return err
}

func (c *Client) StepDown() error {
	_, err := c.roundTrip(protocol.OpCodeStepDown, nil)
	return err
}

func (c *Client) FlushDB() error {
	_, err := c.roundTrip(protocol.OpCodeFlushDB, nil)
	return err
}

func (c *Client) Stat() ([]byte, error) {
	return c.roundTrip(protocol.OpCodeStat, nil)
}

func (c *Client) Get(key string) ([]byte, error) {
	if !protocol.IsASCII(key) {
		return nil, ErrInvalidKey
	}
	return c.roundTrip(protocol.OpCodeGet, []byte(key))
}

func (c *Client) MGet(keys ...string) ([][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(keys)))
	for _, k := range keys {
		if !protocol.IsASCII(k) {
			return nil, ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
	}
	payload, err := c.roundTrip(protocol.OpCodeMGet, buf.Bytes())
	if err != nil {
		return nil, err
	}
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

func (c *Client) Set(key string, value []byte) error {
	if !protocol.IsASCII(key) {
		return ErrInvalidKey
	}
	kBytes := []byte(key)
	payload := make([]byte, 4+len(kBytes)+len(value))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(kBytes)))
	copy(payload[4:], kBytes)
	copy(payload[4+len(kBytes):], value)
	_, err := c.roundTrip(protocol.OpCodeSet, payload)
	return err
}

func (c *Client) MSet(entries map[string][]byte) error {
	if len(entries) == 0 {
		return nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(entries)))
	for k, v := range entries {
		if !protocol.IsASCII(k) {
			return ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
		binary.Write(buf, binary.BigEndian, uint32(len(v)))
		buf.Write(v)
	}
	_, err := c.roundTrip(protocol.OpCodeMSet, buf.Bytes())
	return err
}

func (c *Client) Del(key string) error {
	if !protocol.IsASCII(key) {
		return ErrInvalidKey
	}
	_, err := c.roundTrip(protocol.OpCodeDel, []byte(key))
	return err
}

func (c *Client) MDel(keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(len(keys)))
	for _, k := range keys {
		if !protocol.IsASCII(k) {
			return 0, ErrInvalidKey
		}
		binary.Write(buf, binary.BigEndian, uint32(len(k)))
		buf.WriteString(k)
	}
	payload, err := c.roundTrip(protocol.OpCodeMDel, buf.Bytes())
	if err != nil {
		return 0, err
	}
	if len(payload) < 4 {
		return 0, errors.New("invalid mdel response")
	}
	count := binary.BigEndian.Uint32(payload[0:4])
	return int(count), nil
}

func (c *Client) Begin() error {
	_, err := c.roundTrip(protocol.OpCodeBegin, nil)
	return err
}

func (c *Client) BeginReadOnly() error {
	_, err := c.roundTrip(protocol.OpCodeBegin, []byte{protocol.BeginReadOnly})
	return err
}

func (c *Client) Commit() error {
	_, err := c.roundTrip(protocol.OpCodeCommit, nil)
	return err
}

func (c *Client) Abort() error {
	_, err := c.roundTrip(protocol.OpCodeAbort, nil)
	return err
}

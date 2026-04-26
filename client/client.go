package client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// --- Protocol Constants ---

const ProtoHeaderSize = 5

const (
	OpCodePing             = 0x01
	OpCodeGet              = 0x02
	OpCodeSet              = 0x03
	OpCodeDel              = 0x04
	OpCodeSelect           = 0x05
	OpCodeMGet             = 0x06
	OpCodeMSet             = 0x07
	OpCodeMDel             = 0x08
	OpCodeBegin            = 0x10
	OpCodeCommit           = 0x11
	OpCodeAbort            = 0x12
	OpCodeStat             = 0x20
	OpCodeReplicaOf        = 0x32
	OpCodePromote          = 0x34
	OpCodeStepDown         = 0x35
	OpCodeCheckpoint       = 0x36
	OpCodeFlushDB          = 0x37
	OpCodeReplHello        = 0x50
	OpCodeReplBatch        = 0x51
	OpCodeReplAck          = 0x52
	OpCodeReplSnapshot     = 0x53
	OpCodeReplSnapshotDone = 0x54
	OpCodeReplSafePoint    = 0x55
	OpCodeReplTimeline     = 0x56
	OpCodeQuit             = 0xFF
)

const (
	OpJournalSet    = 1
	OpJournalDelete = 2
	OpJournalCommit = 3
)

const (
	ResStatusOK             = 0x00
	ResStatusErr            = 0x01
	ResStatusNotFound       = 0x02
	ResStatusTxRequired     = 0x03
	ResStatusTxTimeout      = 0x04
	ResStatusTxConflict     = 0x05
	ResStatusTxInProgress   = 0x06
	ResStatusTxCommitted    = 0x0A
	ResStatusServerBusy     = 0x07
	ResStatusEntityTooLarge = 0x08
	ResStatusMemoryLimit    = 0x09
)

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

var Crc32Table = crc32.MakeTable(crc32.Castagnoli)

type ServerError struct {
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error: %s", e.Message)
}

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
	conn     net.Conn
	mu       sync.Mutex
	config   Config
	logger   *slog.Logger
	pool     *Pool
	poisoned bool
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5 * time.Second
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

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pool != nil {
		if c.poisoned {
			c.pool.reportClosed()
			if c.conn != nil {
				c.logger.Info("Closing poisoned connection", "addr", c.config.Address)
				return c.conn.Close()
			}
			return nil
		}
		select {
		case c.pool.idle <- c:
			return nil
		default:
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

func (c *Client) markPoisoned() {
	c.poisoned = true
}

type Pool struct {
	config     Config
	idle       chan *Client
	maxCap     int
	mu         sync.Mutex
	currentCap int
	closed     bool
}

func NewClientPool(cfg Config, maxConns int) (*Pool, error) {
	if maxConns <= 0 {
		return nil, errors.New("maxConns must be > 0")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Pool{
		config: cfg,
		idle:   make(chan *Client, maxConns),
		maxCap: maxConns,
	}, nil
}

func (p *Pool) Get() (*Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("pool is closed")
	}
	select {
	case c := <-p.idle:
		p.mu.Unlock()
		return c, nil
	default:
	}
	if p.currentCap < p.maxCap {
		p.currentCap++
		p.mu.Unlock()
		c, err := NewClient(p.config)
		if err != nil {
			p.reportClosed()
			return nil, err
		}
		c.pool = p
		return c, nil
	}
	p.mu.Unlock()
	select {
	case c, ok := <-p.idle:
		if !ok {
			return nil, errors.New("pool is closed")
		}
		return c, nil
	}
}

func (p *Pool) reportClosed() {
	p.mu.Lock()
	p.currentCap--
	p.mu.Unlock()
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.idle)
	p.mu.Unlock()
	for c := range p.idle {
		if c.conn != nil {
			c.conn.Close()
		}
	}
}

func (c *Client) roundTrip(op byte, payload []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		c.markPoisoned()
		return nil, ErrConnection
	}
	if c.config.WriteTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	}
	if c.config.ReadTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	}
	reqHeader := make([]byte, ProtoHeaderSize)
	reqHeader[0] = op
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(len(payload)))
	if _, err := c.conn.Write(reqHeader); err != nil {
		c.markPoisoned()
		c.conn.Close()
		c.conn = nil
		return nil, fmt.Errorf("%w: write header failed: %v", ErrConnection, err)
	}
	if len(payload) > 0 {
		if _, err := c.conn.Write(payload); err != nil {
			c.markPoisoned()
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: write payload failed: %v", ErrConnection, err)
		}
	}
	respHeader := make([]byte, ProtoHeaderSize)
	if _, err := io.ReadFull(c.conn, respHeader); err != nil {
		c.markPoisoned()
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
			c.markPoisoned()
			c.conn.Close()
			c.conn = nil
			return nil, fmt.Errorf("%w: read body failed: %v", ErrConnection, err)
		}
	}
	return body, mapStatusToError(status, body)
}

func (c *Client) Ping() error {
	_, err := c.roundTrip(OpCodePing, nil)
	return err
}

func (c *Client) Select(dbName string) error {
	_, err := c.roundTrip(OpCodeSelect, []byte(dbName))
	return err
}

func (c *Client) ReplicaOf(sourceAddr, sourceDB string) error {
	addrBytes := []byte(sourceAddr)
	dbBytes := []byte(sourceDB)
	payload := make([]byte, 4+len(addrBytes)+len(dbBytes))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(addrBytes)))
	copy(payload[4:], addrBytes)
	copy(payload[4+len(addrBytes):], dbBytes)
	_, err := c.roundTrip(OpCodeReplicaOf, payload)
	return err
}

func (c *Client) Promote(minReplicas int) error {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(minReplicas))
	_, err := c.roundTrip(OpCodePromote, payload)
	return err
}

func (c *Client) StepDown() error {
	_, err := c.roundTrip(OpCodeStepDown, nil)
	return err
}

func (c *Client) Checkpoint() error {
	_, err := c.roundTrip(OpCodeCheckpoint, nil)
	return err
}

func (c *Client) FlushDB() error {
	_, err := c.roundTrip(OpCodeFlushDB, nil)
	return err
}

func (c *Client) Stat() ([]byte, error) {
	return c.roundTrip(OpCodeStat, nil)
}

func (c *Client) Get(key string) ([]byte, error) {
	if !isASCII(key) {
		return nil, ErrInvalidKey
	}
	return c.roundTrip(OpCodeGet, []byte(key))
}

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
	if !isASCII(key) {
		return ErrInvalidKey
	}
	kBytes := []byte(key)
	payload := make([]byte, 4+len(kBytes)+len(value))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(kBytes)))
	copy(payload[4:], kBytes)
	copy(payload[4+len(kBytes):], value)
	_, err := c.roundTrip(OpCodeSet, payload)
	return err
}

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

func (c *Client) Del(key string) error {
	if !isASCII(key) {
		return ErrInvalidKey
	}
	_, err := c.roundTrip(OpCodeDel, []byte(key))
	return err
}

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

type Change struct {
	LogSeq         uint64
	TxID           uint64
	Key            []byte
	Value          []byte
	IsDelete       bool
	IsSnapshotDone bool
}

// Subscribe connects to the server and starts listening for changes.
func (c *Client) Subscribe(dbName string, startSeq uint64, handler func(Change) error) error {
	c.mu.Lock()
	if c.conn == nil {
		c.mu.Unlock()
		return ErrConnection
	}
	clientID := c.config.ClientID
	if clientID == "" {
		clientID = "client-unknown"
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(clientID)))
	buf.WriteString(clientID)
	binary.Write(buf, binary.BigEndian, uint32(1))
	binary.Write(buf, binary.BigEndian, uint32(len(dbName)))
	buf.WriteString(dbName)
	binary.Write(buf, binary.BigEndian, startSeq)
	reqHeader := make([]byte, ProtoHeaderSize)
	reqHeader[0] = OpCodeReplHello
	binary.BigEndian.PutUint32(reqHeader[1:], uint32(buf.Len()))
	if _, err := c.conn.Write(reqHeader); err != nil {
		c.mu.Unlock()
		return err
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.mu.Unlock()
		return err
	}
	c.mu.Unlock()

	respHeader := make([]byte, ProtoHeaderSize)
	for {
		if c.config.ReadTimeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
		}

		if _, err := io.ReadFull(c.conn, respHeader); err != nil {
			return err
		}
		opCode := respHeader[0]
		length := binary.BigEndian.Uint32(respHeader[1:])

		// SAFETY CHECK: Prevent OOM on huge/corrupted length
		if length > 512*1024*1024 {
			return ErrEntityTooLarge
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(c.conn, payload); err != nil {
			return err
		}

		// Handle Heartbeats (SafePoint/Timeline) to avoid fallback to error
		if opCode == OpCodeReplSafePoint || opCode == OpCodeReplTimeline {
			continue
		}

		if opCode == OpCodeReplBatch || opCode == OpCodeReplSnapshot || opCode == OpCodeReplSnapshotDone {
			// PATCH: Verify CRC32
			if len(payload) < 4 {
				return errors.New("replication packet too short for crc")
			}
			crcReceived := binary.BigEndian.Uint32(payload[:4])
			rawBody := payload[4:]
			if crc32.Checksum(rawBody, Crc32Table) != crcReceived {
				return errors.New("crc mismatch on replication stream")
			}
			payload = rawBody
		}

		if opCode == OpCodeReplBatch {
			cursor := 0
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed batch: short header")
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4
			if cursor+nLen > len(payload) {
				return fmt.Errorf("malformed batch: short name")
			}
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
				if opType == OpJournalCommit {
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
			cursor := 0
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed snapshot: header")
			}
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen
			if cursor+4 > len(payload) {
				return fmt.Errorf("malformed snapshot: count")
			}
			count := binary.BigEndian.Uint32(payload[cursor : cursor+4])
			cursor += 4
			data := payload[cursor:]
			dCursor := 0
			for i := 0; i < int(count); i++ {
				if dCursor+4 > len(data) {
					return fmt.Errorf("malformed snapshot entry: klen")
				}
				kLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				if dCursor+kLen > len(data) {
					return fmt.Errorf("malformed snapshot entry: key")
				}
				key := data[dCursor : dCursor+kLen]
				dCursor += kLen
				if dCursor+4 > len(data) {
					return fmt.Errorf("malformed snapshot entry: vlen")
				}
				vLen := int(binary.BigEndian.Uint32(data[dCursor : dCursor+4]))
				dCursor += 4
				if dCursor+vLen > len(data) {
					return fmt.Errorf("malformed snapshot entry: val")
				}
				val := data[dCursor : dCursor+vLen]
				dCursor += vLen
				change := Change{
					LogSeq:   0,
					TxID:     0,
					Key:      key,
					Value:    val,
					IsDelete: false,
				}
				if err := handler(change); err != nil {
					return err
				}
			}

			// PATCH: Send ACK for snapshot chunks to act as Heartbeat
			// This prevents the server from disconnecting the client as a zombie during long snapshots.
			// We send LogSeq=0 because we don't have the final seq yet, but it updates LastSeen on server.
			ackBuf := new(bytes.Buffer)
			binary.Write(ackBuf, binary.BigEndian, uint32(len(dbName)))
			ackBuf.WriteString(dbName)
			binary.Write(ackBuf, binary.BigEndian, uint64(0)) // Seq 0 for keepalive

			ackHeader := make([]byte, ProtoHeaderSize)
			ackHeader[0] = OpCodeReplAck
			binary.BigEndian.PutUint32(ackHeader[1:], uint32(ackBuf.Len()))

			if _, err := c.conn.Write(ackHeader); err != nil {
				return err
			}
			if _, err := c.conn.Write(ackBuf.Bytes()); err != nil {
				return err
			}

		} else if opCode == OpCodeReplSnapshotDone {
			cursor := 0
			nLen := int(binary.BigEndian.Uint32(payload[cursor : cursor+4]))
			cursor += 4 + nLen + 4
			if cursor+16 <= len(payload) {
				resumeSeq := binary.BigEndian.Uint64(payload[cursor+8:])
				if err := handler(Change{IsSnapshotDone: true, LogSeq: resumeSeq}); err != nil {
					return err
				}
			} else if cursor+8 <= len(payload) {
				resumeSeq := binary.BigEndian.Uint64(payload[cursor:])
				if err := handler(Change{IsSnapshotDone: true, LogSeq: resumeSeq}); err != nil {
					return err
				}
			}
		} else {
			if opCode == ResStatusErr {
				return &ServerError{Message: string(payload)}
			}
		}
	}
}

package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
	"time"
)

// Constants specific to CDC
const (
	OpCodeSync         = 0x30
	ResStatusGenSwitch = 0x0A // New status for transparent transition

	HeaderSize = 20         // KeyLen(4) + ValLen(4) + TxStart(8) + CRC(4)
	Tombstone  = ^uint32(0) // Max Uint32 indicates deletion

	// Safety Limits
	MaxResponseSize = 32 * 1024 * 1024 // 32MB Limit to prevent OOM on corrupt packets
)

// EventHandler defines the contract for processing CDC events.
// Methods return error to stop the client (e.g., on broken pipe or disk full).
type EventHandler interface {
	OnSet(key string, value []byte, offset int64) error
	OnDelete(key string, offset int64) error
	OnReset(newGeneration uint64) error
	OnCheckpoint(generation uint64, offset int64) error
	Flush() error
}

// Client handles the low-level replication protocol with TurnstoneDB.
type Client struct {
	addr        string
	tlsConfig   *tls.Config
	conn        net.Conn
	handler     EventHandler
	generation  uint64
	offset      int64
	idleTimeout time.Duration
	retryDelay  time.Duration
}

// NewClient creates a client that will pump events to the provided handler.
func NewClient(addr string, tlsConfig *tls.Config, handler EventHandler, startGen uint64, startOffset int64, idleTimeout time.Duration) *Client {
	return &Client{
		addr:        addr,
		tlsConfig:   tlsConfig,
		handler:     handler,
		generation:  startGen,
		offset:      startOffset,
		idleTimeout: idleTimeout,
		retryDelay:  2 * time.Second,
	}
}

// Run starts the replication loop with automatic reconnection.
func (c *Client) Run(ctx context.Context) error {
	// Reconnection Loop
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := c.connect(ctx); err != nil {
			log.Printf("[CDC] Connection failed: %v. Retrying in %v...", err, c.retryDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay):
				continue
			}
		}

		// Sync Loop (Inner loop runs until error, context cancel, or handler error)
		err := c.syncLoop(ctx)
		c.closeConn()

		// If the error was from the handler (e.g. Broken Pipe), we should exit immediately
		if IsBrokenPipe(err) {
			return fmt.Errorf("output stream broken (consumer stopped?): %w", err)
		}

		if ctx.Err() != nil {
			return ctx.Err() // Graceful shutdown
		}

		log.Printf("[CDC] Disconnected: %v. Retrying in %v...", err, c.retryDelay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.retryDelay):
			continue
		}
	}
}

func (c *Client) connect(ctx context.Context) error {
	var conn net.Conn
	var err error

	dialer := net.Dialer{Timeout: 5 * time.Second}

	if c.tlsConfig != nil {
		conn, err = tls.DialWithDialer(&dialer, "tcp", c.addr, c.tlsConfig)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", c.addr)
	}

	if err != nil {
		return err
	}
	c.conn = conn

	log.Printf("[CDC] Connected to %s. Syncing from Gen: %d, Offset: %d", c.addr, c.generation, c.offset)
	return nil
}

func (c *Client) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) syncLoop(ctx context.Context) error {
	headerBuf := make([]byte, 5)
	lastDataTime := time.Now()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Check Idle Timeout
		if c.idleTimeout > 0 && time.Since(lastDataTime) > c.idleTimeout {
			return fmt.Errorf("idle timeout exceeded: no data received for %v", c.idleTimeout)
		}

		// 1. Send Sync Request
		reqLen := 16
		reqBuf := make([]byte, 5+reqLen)

		reqBuf[0] = OpCodeSync
		binary.BigEndian.PutUint32(reqBuf[1:5], uint32(reqLen))
		binary.BigEndian.PutUint64(reqBuf[5:13], c.generation)
		binary.BigEndian.PutUint64(reqBuf[13:21], uint64(c.offset))

		// Set IO deadline to detect dead connections (increased to 30s)
		if err := c.conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return fmt.Errorf("set deadline error: %w", err)
		}

		if _, err := c.conn.Write(reqBuf); err != nil {
			return fmt.Errorf("write error: %w", err)
		}

		// 2. Read Response Header
		if _, err := io.ReadFull(c.conn, headerBuf); err != nil {
			return fmt.Errorf("read header error: %w", err)
		}

		status := headerBuf[0]
		payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

		// Safeguard: Sanity Check Payload Size
		if payloadLen > MaxResponseSize {
			return fmt.Errorf("response too large: %d > %d (protocol error or corrupt stream)", payloadLen, MaxResponseSize)
		}

		// 3. Read Response Body
		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			return fmt.Errorf("read body error: %w", err)
		}

		// 4. Handle Response
		hasData, err := c.handleResponse(status, body)
		if err != nil {
			// If it's a pipe error, stop immediately
			if IsBrokenPipe(err) {
				return err
			}

			log.Printf("[CDC] Protocol error: %v", err)
			time.Sleep(1 * time.Second)
			// Don't return error here, retry unless it's critical
		}

		if hasData {
			lastDataTime = time.Now()
		}
	}
}

// handleResponse processes the server response. Returns true if meaningful data was received.
func (c *Client) handleResponse(status byte, body []byte) (bool, error) {
	hasData := false

	switch status {
	case ResStatusGenMismatch:
		serverGen := binary.BigEndian.Uint64(body)
		if err := c.handler.OnReset(serverGen); err != nil {
			return false, err
		}
		c.generation = serverGen
		c.offset = 0
		hasData = true

	case ResStatusGenSwitch:
		serverGen := binary.BigEndian.Uint64(body)
		log.Printf("[CDC] Seamless switch to Gen %d", serverGen)
		c.generation = serverGen
		c.offset = 0
		hasData = true

	case ResStatusOK:
		if len(body) < 16 {
			return false, fmt.Errorf("protocol error: empty OK body")
		}

		serverGen := binary.BigEndian.Uint64(body[0:8])
		rawData := body[16:]

		var bytesConsumed int64
		if len(rawData) > 0 {
			var err error
			bytesConsumed, err = c.parseBatch(rawData)
			if err != nil {
				return false, err
			}
			hasData = true
		}

		c.offset += bytesConsumed
		c.generation = serverGen

		if bytesConsumed > 0 {
			if err := c.handler.OnCheckpoint(c.generation, c.offset); err != nil {
				return false, err
			}
		} else {
			// Throttle polling if no new data
			time.Sleep(100 * time.Millisecond)
		}

	default:
		return false, fmt.Errorf("server error status: %d msg: %s", status, string(body))
	}

	return hasData, nil
}

func (c *Client) parseBatch(data []byte) (int64, error) {
	var consumed int
	totalLen := len(data)
	currentEntryOffset := c.offset

	for {
		if totalLen-consumed < HeaderSize {
			break // Partial header
		}

		header := data[consumed : consumed+HeaderSize]
		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])
		// TxStart is at header[8:16], CRC is at header[16:20]

		isDelete := valLen == Tombstone
		var payloadLen int
		if isDelete {
			payloadLen = int(keyLen)
		} else {
			payloadLen = int(keyLen) + int(valLen)
		}

		entrySize := HeaderSize + payloadLen

		if totalLen-consumed < entrySize {
			break // Partial payload
		}

		payload := data[consumed+HeaderSize : consumed+entrySize]
		key := string(payload[:keyLen])

		if isDelete {
			if err := c.handler.OnDelete(key, currentEntryOffset); err != nil {
				return 0, err
			}
		} else {
			val := payload[keyLen:]
			if err := c.handler.OnSet(key, val, currentEntryOffset); err != nil {
				return 0, err
			}
		}

		consumed += entrySize
		currentEntryOffset += int64(entrySize)
	}

	return int64(consumed), nil
}

// IsBrokenPipe checks for pipe errors (e.g. consumer closed stdout)
func IsBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	// Check for broken pipe error (syscall.EPIPE)
	opErr, ok := err.(*net.OpError)
	if ok {
		if syscallErr, ok := opErr.Err.(*os.SyscallError); ok {
			return syscallErr.Err == syscall.EPIPE
		}
	}
	// Direct syscall error check
	if syscallErr, ok := err.(*os.SyscallError); ok {
		return syscallErr.Err == syscall.EPIPE
	}
	// Generic checks
	return err == syscall.EPIPE || err == io.ErrClosedPipe || err.Error() == "write: broken pipe"
}

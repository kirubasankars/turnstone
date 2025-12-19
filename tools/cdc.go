package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Constants replicated from the server definition
const (
	OpCodeAuth           = 0x23
	OpCodeSync           = 0x30
	ResStatusOK          = 0x00
	ResStatusGenMismatch = 0x09
	ResStatusGenSwitch   = 0x0A // New status for transparent transition

	HeaderSize = 12         // KeyLen(4) + ValLen(4) + CRC(4)
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
	// OnCheckpoint is called after a batch is processed to indicate safe resume point.
	OnCheckpoint(generation uint64, offset int64) error
	// Flush forces any buffered output to be written.
	Flush() error
}

// CDCClient handles the low-level replication protocol with TurnstoneDB.
type CDCClient struct {
	addr        string
	authPass    string
	conn        net.Conn
	handler     EventHandler
	generation  uint64
	offset      int64
	idleTimeout time.Duration
	retryDelay  time.Duration
}

// NewCDCClient creates a client that will pump events to the provided handler.
func NewCDCClient(addr string, authPass string, handler EventHandler, startGen uint64, startOffset int64, idleTimeout time.Duration) *CDCClient {
	return &CDCClient{
		addr:        addr,
		authPass:    authPass,
		handler:     handler,
		generation:  startGen,
		offset:      startOffset,
		idleTimeout: idleTimeout,
		retryDelay:  2 * time.Second,
	}
}

// Run starts the replication loop with automatic reconnection.
func (c *CDCClient) Run(ctx context.Context) error {
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
		// rather than reconnecting.
		if isBrokenPipe(err) {
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

func (c *CDCClient) connect(ctx context.Context) error {
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return err
	}
	c.conn = conn

	// Perform Auth if password is provided
	if c.authPass != "" {
		if err := c.authenticate(); err != nil {
			c.conn.Close()
			return err
		}
	}

	log.Printf("[CDC] Connected to %s. Syncing from Gen: %d, Offset: %d", c.addr, c.generation, c.offset)
	return nil
}

func (c *CDCClient) authenticate() error {
	passBytes := []byte(c.authPass)
	reqLen := len(passBytes)
	reqBuf := make([]byte, 5+reqLen)

	reqBuf[0] = OpCodeAuth
	binary.BigEndian.PutUint32(reqBuf[1:5], uint32(reqLen))
	copy(reqBuf[5:], passBytes)

	// Set a short deadline for auth
	if err := c.conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return fmt.Errorf("failed to set deadline for auth: %w", err)
	}

	if _, err := c.conn.Write(reqBuf); err != nil {
		return fmt.Errorf("auth write error: %w", err)
	}

	headerBuf := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, headerBuf); err != nil {
		return fmt.Errorf("auth read header error: %w", err)
	}

	status := headerBuf[0]
	payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

	body := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return fmt.Errorf("auth read body error: %w", err)
	}

	if status != ResStatusOK {
		return fmt.Errorf("authentication failed: %s", string(body))
	}

	// Reset deadline for normal operation
	return c.conn.SetDeadline(time.Time{})
}

func (c *CDCClient) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *CDCClient) syncLoop(ctx context.Context) error {
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
			if isBrokenPipe(err) {
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
func (c *CDCClient) handleResponse(status byte, body []byte) (bool, error) {
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

func (c *CDCClient) parseBatch(data []byte) (int64, error) {
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

// --- Pipe-Friendly Implementation ---

// PipeHandler formats output for downstream processing.
// Data commands go to STDOUT. Logs go to STDERR.
type PipeHandler struct {
	EncodeBase64 bool
	writer       *bufio.Writer
}

func NewPipeHandler(base64 bool) *PipeHandler {
	return &PipeHandler{
		EncodeBase64: base64,
		writer:       bufio.NewWriter(os.Stdout),
	}
}

func (h *PipeHandler) OnReset(newGen uint64) error {
	if err := h.Flush(); err != nil {
		return err
	}
	log.Printf("[CDC] RESET DETECTED. New Generation: %d", newGen)
	_, err := fmt.Fprintf(h.writer, "RESET %d\n", newGen)
	return err
}

func (h *PipeHandler) OnSet(key string, val []byte, offset int64) error {
	var valueStr string
	if h.EncodeBase64 {
		valueStr = base64.StdEncoding.EncodeToString(val)
	} else {
		valueStr = string(val)
	}
	_, err := fmt.Fprintf(h.writer, "SET %s %s\n", key, valueStr)
	return err
}

func (h *PipeHandler) OnDelete(key string, offset int64) error {
	_, err := fmt.Fprintf(h.writer, "DEL %s\n", key)
	return err
}

func (h *PipeHandler) OnCheckpoint(generation uint64, offset int64) error {
	if _, err := fmt.Fprintf(h.writer, "CP %d %d\n", generation, offset); err != nil {
		return err
	}
	return h.Flush() // Flush on checkpoints to ensure consumer sees progress
}

func (h *PipeHandler) Flush() error {
	return h.writer.Flush()
}

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	// Check for broken pipe error (syscall.EPIPE)
	// This usually happens when the downstream process (e.g. grep, jq) closes stdout.
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
	// Check for generic "broken pipe" string (Go standard lib behavior in some contexts)
	return err == syscall.EPIPE || err == io.ErrClosedPipe || err.Error() == "write: broken pipe"
}

func main() {
	addr := flag.String("addr", ":6379", "TurnstoneDB server address")
	gen := flag.Uint64("gen", 0, "Start generation (resume state)")
	offset := flag.Int64("offset", 0, "Start offset (resume state)")
	timeout := flag.Duration("timeout", 0, "Exit if no messages received for this duration (0 = forever)")
	useBase64 := flag.Bool("base64", false, "Encode values as base64 in output")
	auth := flag.String("auth", "", "Password for authentication")

	flag.Parse()

	// Use Context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewPipeHandler(*useBase64)
	client := NewCDCClient(*addr, *auth, handler, *gen, *offset, *timeout)

	// Safe Shutdown Handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\n[CDC] Interrupt received, shutting down...")
		cancel()
	}()

	if err := client.Run(ctx); err != nil && err != context.Canceled {
		// Log fatal errors to stderr
		log.Printf("Fatal Error: %v", err)
		os.Exit(1)
	}

	// Ensure any remaining buffer is flushed on exit
	_ = handler.Flush()
}

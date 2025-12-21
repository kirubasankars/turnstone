package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
	"time"
)

// CheckpointState represents the persistent state for resumption.
type CheckpointState struct {
	Generation uint64 `json:"generation"`
	Offset     int64  `json:"offset"`
}

// EventHandler defines the contract for processing CDC events.
type EventHandler interface {
	OnSet(key string, value []byte, offset int64) error
	OnDelete(key string, offset int64) error
	OnReset(newGeneration uint64) error
	OnCheckpoint(generation uint64, offset int64) error
	Flush() error
}

// CDCClient handles the low-level replication protocol with TurnstoneDB.
type CDCClient struct {
	addr       string
	conn       net.Conn
	handler    EventHandler
	dbIndex    int
	generation uint64
	offset     int64
	retryDelay time.Duration

	// TLS Config
	clientCertFile string
	clientKeyFile  string
	serverCAFile   string
}

func NewCDCClient(addr string, handler EventHandler, dbIdx int, startGen uint64, startOffset int64, clientCert, clientKey, serverCA string) *CDCClient {
	return &CDCClient{
		addr:           addr,
		handler:        handler,
		dbIndex:        dbIdx,
		generation:     startGen,
		offset:         startOffset,
		retryDelay:     2 * time.Second,
		clientCertFile: clientCert,
		clientKeyFile:  clientKey,
		serverCAFile:   serverCA,
	}
}

func (c *CDCClient) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := c.connect(ctx); err != nil {
			log.Printf("[CDC DB:%d] Connect failed: %v. Retry in %v", c.dbIndex, err, c.retryDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.retryDelay):
				continue
			}
		}

		err := c.syncLoop(ctx)
		c.closeConn()

		if isBrokenPipe(err) {
			return fmt.Errorf("output stream broken: %w", err)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		log.Printf("[CDC DB:%d] Disconnected: %v. Retry in %v...", c.dbIndex, err, c.retryDelay)
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

	// Always use mTLS
	tlsConfig := &tls.Config{}

	// Load CA
	caCert, err := os.ReadFile(c.serverCAFile)
	if err != nil {
		return fmt.Errorf("failed to load Server CA: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	// Load Client Certs
	cert, err := tls.LoadX509KeyPair(c.clientCertFile, c.clientKeyFile)
	if err != nil {
		return fmt.Errorf("failed to load client certs for mTLS: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	conn, err := tls.DialWithDialer(&d, "tcp", c.addr, tlsConfig)
	if err != nil {
		return err
	}
	c.conn = conn

	log.Printf("[CDC DB:%d] Connected to %s (mTLS). Syncing from Gen: %d, Offset: %d", c.dbIndex, c.addr, c.generation, c.offset)
	return nil
}

func (c *CDCClient) closeConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *CDCClient) syncLoop(ctx context.Context) error {
	headerBuf := make([]byte, 5)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		reqLen := 16
		reqBuf := make([]byte, 5+reqLen)
		reqBuf[0] = OpCodeSync
		binary.BigEndian.PutUint32(reqBuf[1:5], uint32(reqLen))
		binary.BigEndian.PutUint64(reqBuf[5:13], c.generation)
		binary.BigEndian.PutUint64(reqBuf[13:21], uint64(c.offset))

		// Set Payload: DB Index
		reqPayload := make([]byte, 17)
		reqPayload[0] = byte(c.dbIndex)
		binary.BigEndian.PutUint64(reqPayload[1:9], c.generation)
		binary.BigEndian.PutUint64(reqPayload[9:17], uint64(c.offset))

		reqHeader := make([]byte, 5)
		reqHeader[0] = OpCodeSync
		binary.BigEndian.PutUint32(reqHeader[1:], uint32(len(reqPayload)))

		if err := c.conn.SetDeadline(time.Now().Add(60 * time.Second)); err != nil {
			return err
		}
		if _, err := c.conn.Write(reqHeader); err != nil {
			return err
		}
		if _, err := c.conn.Write(reqPayload); err != nil {
			return err
		}

		if _, err := io.ReadFull(c.conn, headerBuf); err != nil {
			return err
		}

		status := headerBuf[0]
		payloadLen := binary.BigEndian.Uint32(headerBuf[1:])

		if payloadLen > MaxResponseSize {
			return fmt.Errorf("response too large: %d", payloadLen)
		}

		body := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.conn, body); err != nil {
			return err
		}

		hasData, err := c.handleResponse(status, body)
		if err != nil {
			if isBrokenPipe(err) {
				return err
			}
			log.Printf("[CDC DB:%d] Protocol error: %v", c.dbIndex, err)
			time.Sleep(1 * time.Second)
		}

		// Optimization: If we just switched generations or got data, loop immediately
		// otherwise sleep briefly to avoid busy loop
		if !hasData {
			// If no data, sleep a bit to avoid hammering the server
			time.Sleep(100 * time.Millisecond)
		}
	}
}

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
		log.Printf("[CDC DB:%d] Gen Mismatch. Switched to Generation %d", c.dbIndex, serverGen)

	case ResStatusOK:
		if len(body) < 16 {
			return false, fmt.Errorf("empty OK body")
		}
		serverGen := binary.BigEndian.Uint64(body[0:8])
		// serverNextOffset := binary.BigEndian.Uint64(body[8:16]) // Can be used for debugging
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
		}

	default:
		return false, fmt.Errorf("server status: %d", status)
	}
	return hasData, nil
}

func (c *CDCClient) parseBatch(data []byte) (int64, error) {
	var consumed int
	totalLen := len(data)
	currentEntryOffset := c.offset

	for {
		if totalLen-consumed < HeaderSize {
			if totalLen-consumed > 0 {
				msg := fmt.Sprintf("Partial header bytes remaining: %d. Consumed so far: %d", totalLen-consumed, consumed)
				if consumed == 0 {
					return 0, fmt.Errorf("%s (Batch invalid - Header incomplete)", msg)
				}
			}
			break
		}
		header := data[consumed : consumed+HeaderSize]
		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])
		// skip minReadVersion [8:16] and CRC [16:20]

		isDelete := valLen == Tombstone
		var payloadLen int
		if isDelete {
			payloadLen = int(keyLen)
		} else {
			payloadLen = int(keyLen) + int(valLen)
		}

		// Safety check: ensure payloadLen is sane
		if payloadLen < 0 {
			return 0, fmt.Errorf("invalid negative payload length detected: %d", payloadLen)
		}

		entrySize := HeaderSize + payloadLen
		if totalLen-consumed < entrySize {
			msg := fmt.Sprintf("Incomplete entry. Need %d bytes, have %d. KeyLen=%d, ValLen=%d", entrySize, totalLen-consumed, keyLen, valLen)
			// If we haven't consumed anything, this is a fatal error for this batch
			// preventing an infinite tight loop of retries.
			if consumed == 0 {
				log.Printf("[CDC] CRITICAL: Raw Header Dump: %x (KeyLen=%d, ValLen=%d)", header, keyLen, valLen)
				return 0, fmt.Errorf("%s (Stuck at head - Offset %d)", msg, currentEntryOffset)
			}
			log.Printf("[CDC] WARN: %s (Dropped partial tail)", msg)
			break
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

func isBrokenPipe(err error) bool {
	if err == nil {
		return false
	}
	opErr, ok := err.(*net.OpError)
	if ok {
		if syscallErr, ok := opErr.Err.(*os.SyscallError); ok {
			return syscallErr.Err == syscall.EPIPE
		}
	}
	return err == io.ErrClosedPipe || err.Error() == "write: broken pipe"
}

// --- Handlers ---

// PipeHandler dumps to stdout (CDC Mode)
type PipeHandler struct {
	EncodeBase64 bool
	writer       *bufio.Writer
	statePath    string
}

func NewPipeHandler(base64 bool, statePath string) *PipeHandler {
	return &PipeHandler{EncodeBase64: base64, writer: bufio.NewWriter(os.Stdout), statePath: statePath}
}

func (h *PipeHandler) OnSet(key string, val []byte, offset int64) error {
	valStr := string(val)
	if h.EncodeBase64 {
		valStr = base64.StdEncoding.EncodeToString(val)
	}
	_, err := fmt.Fprintf(h.writer, "SET %s %s\n", key, valStr)
	if err == nil {
		h.writer.Flush() // Explicit flush for interactive CLI feedback
	}
	return err
}

func (h *PipeHandler) OnDelete(key string, offset int64) error {
	_, err := fmt.Fprintf(h.writer, "DEL %s\n", key)
	if err == nil {
		h.writer.Flush() // Explicit flush for interactive CLI feedback
	}
	return err
}

func (h *PipeHandler) OnReset(gen uint64) error {
	h.saveState(gen, 0)
	fmt.Fprintf(h.writer, "RESET %d\n", gen)
	return h.Flush()
}

func (h *PipeHandler) OnCheckpoint(gen uint64, off int64) error {
	h.Flush()
	return h.saveState(gen, off)
}

func (h *PipeHandler) Flush() error { return h.writer.Flush() }

func (h *PipeHandler) saveState(gen uint64, off int64) error {
	if h.statePath == "" {
		return nil
	}
	data, _ := json.Marshal(CheckpointState{Generation: gen, Offset: off})
	return os.WriteFile(h.statePath, data, 0o644)
}

// ReplicaHandler applies changes to local store (Follower Mode)
type ReplicaHandler struct {
	store *Store
	batch []bufferedOp
}

func NewReplicaHandler(s *Store) *ReplicaHandler {
	return &ReplicaHandler{store: s, batch: make([]bufferedOp, 0, 1000)}
}

func (h *ReplicaHandler) OnSet(key string, val []byte, offset int64) error {
	// Copy val to avoid buffer reuse issues
	v := make([]byte, len(val))
	copy(v, val)
	h.batch = append(h.batch, bufferedOp{opType: OpJournalSet, key: key, val: v})
	return nil
}

func (h *ReplicaHandler) OnDelete(key string, offset int64) error {
	h.batch = append(h.batch, bufferedOp{opType: OpJournalDelete, key: key})
	return nil
}

func (h *ReplicaHandler) OnReset(gen uint64) error {
	h.batch = h.batch[:0]
	return h.store.SetReplicationState(gen, 0)
}

func (h *ReplicaHandler) OnCheckpoint(gen uint64, off int64) error {
	if len(h.batch) == 0 {
		return h.store.SetReplicationState(gen, off)
	}

	rv, localGen := h.store.AcquireSnapshot()
	defer h.store.ReleaseSnapshot(rv)

	// In follower mode, we assume the leader's stream is the source of truth.
	// We apply the batch. The journal will append this data.
	// Note: Store will calculate new CRCs and Headers (including new TxStart).
	if err := h.store.ApplyBatch(h.batch, rv, localGen); err != nil {
		log.Printf("[Replica] ApplyBatch warning: %v", err)
	}

	h.batch = h.batch[:0]
	return h.store.SetReplicationState(gen, off)
}

func (h *ReplicaHandler) Flush() error { return nil }

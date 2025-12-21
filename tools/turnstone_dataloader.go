package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ============================================================================
// CONFIGURATION
// ============================================================================

var (
	cfgHost       string
	cfgPort       int
	cfgAuth       string
	cfgFile       string
	cfgWorkers    int
	cfgBatchSize  int
	cfgReportFreq time.Duration

	// mTLS Configuration
	cfgCAFile   string
	cfgCertFile string
	cfgKeyFile  string
)

// Protocol Constants
const (
	OpCodeAuth   = 0x23
	OpCodeSet    = 0x03
	OpCodeBegin  = 0x10
	OpCodeCommit = 0x11
	OpCodeQuit   = 0xFF

	StatusOk            = 0x00
	ResStatusTxConflict = 0x05 // Transaction Conflict Code

	// Networking
	DialTimeout  = 5 * time.Second
	ReadTimeout  = 5 * time.Second
	WriteTimeout = 5 * time.Second
)

// Errors
var ErrConflict = fmt.Errorf("transaction conflict")

// Stats
var (
	recordsLoaded uint64
	bytesLoaded   uint64
	conflicts     uint64 // Track retries
)

// ============================================================================
// MAIN
// ============================================================================

func main() {
	// Configure logging to show timestamps
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	parseFlags()

	// Setup Signal Handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Verify Input File
	f, err := os.Open(cfgFile)
	if err != nil {
		log.Fatalf("Fatal: Failed to open input file '%s': %v", cfgFile, err)
	}
	defer f.Close()

	// Verify Cert Files
	if _, err := os.Stat(cfgCAFile); os.IsNotExist(err) {
		log.Fatalf("Fatal: CA file not found: %s", cfgCAFile)
	}
	if _, err := os.Stat(cfgCertFile); os.IsNotExist(err) {
		log.Fatalf("Fatal: Client Cert file not found: %s", cfgCertFile)
	}
	if _, err := os.Stat(cfgKeyFile); os.IsNotExist(err) {
		log.Fatalf("Fatal: Client Key file not found: %s", cfgKeyFile)
	}

	log.Printf("--- Turnstone Data Loader ---")
	log.Printf("Target:   %s:%d (mTLS Enabled)", cfgHost, cfgPort)
	log.Printf("Source:   %s", cfgFile)
	log.Printf("Workers:  %d", cfgWorkers)
	log.Printf("Batching: %d records/tx", cfgBatchSize)
	log.Println("-----------------------------")

	// Initialize Channels and WaitGroups
	// Buffer channel to keep workers busy without blocking the reader
	jobChan := make(chan []string, cfgWorkers*cfgBatchSize*2)
	var wg sync.WaitGroup

	// Start Reporting Routine
	go reporter(ctx)

	startTime := time.Now()

	// Start Consumers (Database Writers)
	for i := 0; i < cfgWorkers; i++ {
		wg.Add(1)
		go worker(ctx, i, jobChan, &wg)
	}

	// Start Producer (CSV Reader)
	readCSV(ctx, f, jobChan)

	// Shutdown sequence
	close(jobChan) // Stop workers
	wg.Wait()      // Wait for workers to finish

	duration := time.Since(startTime)
	totalRecords := atomic.LoadUint64(&recordsLoaded)
	totalConflicts := atomic.LoadUint64(&conflicts)

	log.Println("-----------------------------")
	log.Printf("Load Complete.")
	log.Printf("Duration:      %v", duration.Round(time.Millisecond))
	log.Printf("Total Records: %d", totalRecords)
	log.Printf("Total Retries: %d", totalConflicts)
	if duration.Seconds() > 0 {
		log.Printf("Throughput:    %.0f records/sec", float64(totalRecords)/duration.Seconds())
	}
}

func parseFlags() {
	flag.StringVar(&cfgHost, "host", "127.0.0.1", "Database host")
	flag.IntVar(&cfgPort, "port", 6379, "Database port")
	flag.StringVar(&cfgAuth, "auth", "", "Authentication password (if server requires separate auth)")
	flag.StringVar(&cfgFile, "file", "fake_weather_data.csv", "Path to CSV file to load")
	flag.IntVar(&cfgWorkers, "workers", 10, "Number of concurrent loader threads")
	flag.IntVar(&cfgBatchSize, "batch", 100, "Number of records per transaction")
	flag.DurationVar(&cfgReportFreq, "report", 2*time.Second, "Progress report frequency")

	// TLS Flags
	flag.StringVar(&cfgCAFile, "ca", "certs/ca.crt", "Path to CA certificate")
	flag.StringVar(&cfgCertFile, "cert", "certs/client.crt", "Path to client certificate")
	flag.StringVar(&cfgKeyFile, "key", "certs/client.key", "Path to client key")

	flag.Parse()
}

// readCSV reads lines from the file and pushes them to the job channel
func readCSV(ctx context.Context, r io.Reader, jobs chan<- []string) {
	scanner := csv.NewReader(r)
	// Allow variable number of fields (in case Value contains commas)
	scanner.FieldsPerRecord = -1

	// Optional: Skip header if your CSV has one.
	// Uncomment the next line to skip the first row.
	// _, _ = scanner.Read()

	lineNum := 0
	for {
		if ctx.Err() != nil {
			break
		}
		record, err := scanner.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Fail fast on bad data
			log.Fatalf("Fatal: CSV Parse Error at line %d: %v", lineNum, err)
		}
		lineNum++

		// Validate record (must have at least a key)
		if len(record) < 1 {
			continue
		}

		select {
		case jobs <- record:
		case <-ctx.Done():
			return
		}
	}
}

// reporter prints progress stats to the console
func reporter(ctx context.Context) {
	ticker := time.NewTicker(cfgReportFreq)
	defer ticker.Stop()

	var lastRecords uint64
	var lastBytes uint64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currRecords := atomic.LoadUint64(&recordsLoaded)
			currBytes := atomic.LoadUint64(&bytesLoaded)
			currConflicts := atomic.LoadUint64(&conflicts)

			deltaRecords := currRecords - lastRecords
			deltaBytes := currBytes - lastBytes

			rate := float64(deltaRecords) / cfgReportFreq.Seconds()
			mbps := float64(deltaBytes) / 1024 / 1024 / cfgReportFreq.Seconds()

			log.Printf("Progress: %8d records | %6.0f rec/s (%6.2f MB/s) | Retries: %d",
				currRecords, rate, mbps, currConflicts)

			lastRecords = currRecords
			lastBytes = currBytes
		}
	}
}

// ============================================================================
// WORKER & CLIENT
// ============================================================================

func worker(ctx context.Context, id int, jobs <-chan []string, wg *sync.WaitGroup) {
	defer wg.Done()

	client := NewClient(fmt.Sprintf("%s:%d", cfgHost, cfgPort), cfgAuth)

	// Connect immediately. If this fails, the tool fails.
	if err := client.Connect(); err != nil {
		log.Fatalf("[Worker %d] Fatal connection error: %v", id, err)
	}
	defer client.Close()

	batch := make([][]string, 0, cfgBatchSize)

	// Helper to send the accumulated batch to the server
	flush := func() {
		if len(batch) == 0 {
			return
		}

		// Retry Loop for Conflicts
		for {
			err := client.SendBatch(batch)
			if err == nil {
				atomic.AddUint64(&recordsLoaded, uint64(len(batch)))
				break // Success
			}

			if err == ErrConflict {
				atomic.AddUint64(&conflicts, 1)
				// Backoff slightly to let other transactions finish
				time.Sleep(10 * time.Millisecond)
				continue // Retry: SendBatch calls BEGIN again, getting a new snapshot
			}

			// Fail fast on non-transient errors (network, auth, disk full)
			log.Fatalf("[Worker %d] Fatal write error: %v", id, err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush() // Try to finish pending work
			return
		case record, ok := <-jobs:
			if !ok {
				flush() // Channel closed, flush remainder
				return
			}
			batch = append(batch, record)
			if len(batch) >= cfgBatchSize {
				flush()
			}
		}
	}
}

// ============================================================================
// TCP CLIENT ADAPTER
// ============================================================================

type Client struct {
	addr     string
	password string
	conn     net.Conn
	reader   *bufio.Reader
	buf      []byte // Reusable scratch buffer for headers
}

func NewClient(addr, password string) *Client {
	return &Client{
		addr:     addr,
		password: password,
		buf:      make([]byte, 5),
	}
}

func (c *Client) Connect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	// Load Client Certs
	cert, err := tls.LoadX509KeyPair(cfgCertFile, cfgKeyFile)
	if err != nil {
		return fmt.Errorf("failed to load client keypair: %w", err)
	}

	// Load CA
	caCert, err := os.ReadFile(cfgCAFile)
	if err != nil {
		return fmt.Errorf("failed to read CA file: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	dialer := &net.Dialer{Timeout: DialTimeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", c.addr, tlsConfig)
	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)

	// Authenticate if password is provided (Optional second factor)
	if c.password != "" {
		if err := c.auth(); err != nil {
			conn.Close()
			return err
		}
	}
	return nil
}

func (c *Client) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) auth() error {
	// Auth Packet: [OpCodeAuth][Len][Password]
	passBytes := []byte(c.password)

	if err := c.writeHeader(OpCodeAuth, uint32(len(passBytes))); err != nil {
		return err
	}
	if _, err := c.conn.Write(passBytes); err != nil {
		return err
	}
	return c.expectOk()
}

func (c *Client) SendBatch(records [][]string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	c.conn.SetDeadline(time.Now().Add(WriteTimeout * 5)) // Generous timeout for batch

	// 1. BEGIN TRANSACTION
	if err := c.writeHeader(OpCodeBegin, 0); err != nil {
		return err
	}
	if err := c.expectOk(); err != nil {
		return err
	}

	// 2. SET OPERATIONS
	var batchBytes uint64
	for _, record := range records {
		if len(record) < 1 {
			continue
		}

		key := strings.TrimSpace(record[0])
		if key == "" {
			continue
		}

		// Reconstruct value from remaining columns
		val := ""
		if len(record) > 1 {
			val = strings.Join(record[1:], ",")
		}

		keyBytes := []byte(key)
		valBytes := []byte(val)

		// Payload: [KeyLen 4b][Key][Value]
		payloadLen := 4 + len(keyBytes) + len(valBytes)

		// Write OpCodeSet Header
		if err := c.writeHeader(OpCodeSet, uint32(payloadLen)); err != nil {
			return err
		}

		// Write Payload
		binary.BigEndian.PutUint32(c.buf[:4], uint32(len(keyBytes)))
		if _, err := c.conn.Write(c.buf[:4]); err != nil {
			return err
		}
		if _, err := c.conn.Write(keyBytes); err != nil {
			return err
		}
		if _, err := c.conn.Write(valBytes); err != nil {
			return err
		}

		// Read Response for SET
		if err := c.expectOk(); err != nil {
			return err
		}
		batchBytes += uint64(payloadLen)
	}

	// 3. COMMIT TRANSACTION
	if err := c.writeHeader(OpCodeCommit, 0); err != nil {
		return err
	}
	if err := c.expectOk(); err != nil {
		return err
	}

	atomic.AddUint64(&bytesLoaded, batchBytes)
	return nil
}

// Low-level helpers

func (c *Client) writeHeader(op byte, length uint32) error {
	c.buf[0] = op
	binary.BigEndian.PutUint32(c.buf[1:], length)
	_, err := c.conn.Write(c.buf)
	return err
}

func (c *Client) expectOk() error {
	// Read Response Header: [Status 1b][Len 4b]
	if _, err := io.ReadFull(c.reader, c.buf); err != nil {
		return err
	}

	status := c.buf[0]
	length := binary.BigEndian.Uint32(c.buf[1:])

	if length > 0 {
		// Discard payload if present (we don't expect payloads for SET/COMMIT/BEGIN/AUTH)
		if _, err := c.reader.Discard(int(length)); err != nil {
			return err
		}
	}

	if status != StatusOk {
		if status == ResStatusTxConflict {
			return ErrConflict
		}
		return fmt.Errorf("server error: code 0x%02x", status)
	}
	return nil
}

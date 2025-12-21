package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/client"
)

// Config
var (
	host        = flag.String("host", "localhost:6379", "Target server address")
	caFile      = flag.String("ca", "", "CA Certificate file")
	certFile    = flag.String("cert", "", "Client Certificate file")
	keyFile     = flag.String("key", "", "Client Key file")
	concurrency = flag.Int("c", 50, "Number of concurrent connections")
	duration    = flag.Duration("d", 10*time.Second, "Test duration")
	keySpace    = flag.Int("keys", 10000, "Key space size (e.g., key_0 to key_10000)")
	writeRatio  = flag.Float64("w", 0.5, "Write ratio (0.0 - 1.0), default 50%")
	readRatio   = flag.Float64("r", -1.0, "Read ratio (0.0 - 1.0), overrides -w if set")
	valSize     = flag.Int("size", 128, "Size of value payload in bytes")
)

// WorkerStats prevents atomic contention by aggregating locally
type WorkerStats struct {
	Ops       int64
	Errors    int64
	Conflicts int64
	Latencies []time.Duration
}

// ServerError allows checking specific status codes
type ServerError struct {
	Status byte
	Msg    string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("server error status: 0x%x, msg: %s", e.Status, e.Msg)
}

// Stats
var (
	// Flag to ensure we only print the first error to avoid console flooding
	hasPrintedError int32
)

func main() {
	flag.Parse()

	// Handle Read Ratio override
	if *readRatio >= 0 {
		*writeRatio = 1.0 - *readRatio
	}

	fmt.Printf("Starting stress test against %s\n", *host)
	if *caFile != "" {
		fmt.Println("Security: mTLS Enabled")
	} else {
		fmt.Println("Security: Insecure TCP")
	}
	fmt.Printf("Workers: %d | Duration: %v | Keys: %d\n", *concurrency, *duration, *keySpace)
	fmt.Printf("Payload: %dB | Write Ratio: %.0f%% | Read Ratio: %.0f%%\n", *valSize, *writeRatio*100, (1.0-*writeRatio)*100)

	var wg sync.WaitGroup
	start := time.Now()

	// Channel to collect stats from workers upon completion
	statsCh := make(chan WorkerStats, *concurrency)
	stopCh := make(chan struct{})

	// Timer to stop test
	time.AfterFunc(*duration, func() {
		close(stopCh)
	})

	// Progress monitor
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		startLog := time.Now()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				elapsed := time.Since(startLog).Seconds()
				fmt.Printf("\rRunning... %.0fs / %.0fs", elapsed, duration.Seconds())
			}
		}
	}()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go worker(i, stopCh, statsCh, &wg)
	}

	wg.Wait()
	close(statsCh)
	fmt.Println("\n\n--- Aggregating Results ---")

	var totalOps int64
	var totalErr int64
	var totalConflicts int64
	var allLatencies []time.Duration

	// Pre-allocate to avoid massive resizing if possible (estimate)
	allLatencies = make([]time.Duration, 0, *concurrency*10000)

	for s := range statsCh {
		totalOps += s.Ops
		totalErr += s.Errors
		totalConflicts += s.Conflicts
		allLatencies = append(allLatencies, s.Latencies...)
	}

	totalDuration := time.Since(start)
	rps := float64(totalOps) / totalDuration.Seconds()

	// Calculate Percentiles
	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	var p50, p95, p99 time.Duration
	if len(allLatencies) > 0 {
		p50 = allLatencies[int(float64(len(allLatencies))*0.50)]
		p95 = allLatencies[int(float64(len(allLatencies))*0.95)]
		p99 = allLatencies[int(float64(len(allLatencies))*0.99)]
	}

	fmt.Printf("Total Ops:        %d\n", totalOps)
	fmt.Printf("Total Errors:     %d\n", totalErr)
	fmt.Printf("Total Conflicts:  %d\n", totalConflicts)
	fmt.Printf("Duration:         %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Throughput:       %.2f requests/sec\n", rps)
	fmt.Println("Latency Distribution:")
	fmt.Printf("  p50: %v\n", p50)
	fmt.Printf("  p95: %v\n", p95)
	fmt.Printf("  p99: %v\n", p99)
}

func connect(id int) (net.Conn, error) {
	var conn net.Conn
	var err error

	dialer := net.Dialer{Timeout: 5 * time.Second}

	if *caFile != "" && *certFile != "" && *keyFile != "" {
		caCert, readErr := os.ReadFile(*caFile)
		if readErr != nil {
			return nil, fmt.Errorf("read ca: %v", readErr)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA pem")
		}

		cert, readErr := tls.LoadX509KeyPair(*certFile, *keyFile)
		if readErr != nil {
			return nil, fmt.Errorf("load keys: %v", readErr)
		}

		tlsCfg := &tls.Config{
			RootCAs:      caCertPool,
			Certificates: []tls.Certificate{cert},
		}
		conn, err = tls.DialWithDialer(&dialer, "tcp", *host, tlsCfg)
	} else {
		conn, err = dialer.Dial("tcp", *host)
	}

	if err != nil {
		fmt.Printf("[Worker %d] Connect error: %v\n", id, err)
		return nil, err
	}
	return conn, nil
}

func worker(id int, stopCh <-chan struct{}, statsCh chan<- WorkerStats, wg *sync.WaitGroup) {
	defer wg.Done()

	// Thread-local random source to avoid global mutex contention
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	stats := WorkerStats{
		Latencies: make([]time.Duration, 0, 10000),
	}

	conn, err := connect(id)
	if err != nil {
		stats.Errors++
		statsCh <- stats
		return
	}
	defer conn.Close()

	readBuf := make([]byte, 4096) // Reusable read buffer

	// Pre-generate a static value payload to avoid allocs during benchmark
	staticVal := make([]byte, *valSize)
	rng.Read(staticVal)
	valStr := string(staticVal)

	for {
		select {
		case <-stopCh:
			statsCh <- stats
			return
		default:
			// Configurable Read/Write mix
			isWrite := rng.Float64() < *writeRatio
			key := fmt.Sprintf("key_%d", rng.Intn(*keySpace))
			var err error

			start := time.Now()

			// 1. Begin Transaction
			if err = sendBegin(conn); err == nil {
				// 2. Perform Operation
				if isWrite {
					// Use random key, but static payload for performance
					err = sendSet(conn, key, valStr, readBuf)
				} else {
					err = sendGet(conn, key, readBuf)
				}

				// 3. Commit Transaction
				if err == nil {
					err = sendCommit(conn)
				}
			}

			// Record Latency
			stats.Latencies = append(stats.Latencies, time.Since(start))

			if err != nil {
				// Check specific error types
				if se, ok := err.(*ServerError); ok && se.Status == client.ResStatusTxConflict {
					stats.Conflicts++
					// Do not reconnect on conflict; the protocol allows continuing
					continue
				}

				stats.Errors++
				// Print error once
				if atomic.CompareAndSwapInt32(&hasPrintedError, 0, 1) {
					fmt.Printf("\n[!] First Error Encountered: %v\n", err)
				}

				// Reconnect on actual network/protocol errors
				conn.Close()
				conn, err = connect(id)
				if err != nil {
					statsCh <- stats
					return
				}
			} else {
				stats.Ops++
			}
		}
	}
}

// --- Protocol Helpers (Optimized) ---

func sendBegin(conn net.Conn) error {
	send(conn, client.OpCodeBegin, nil)
	return readResp(conn, nil)
}

func sendCommit(conn net.Conn) error {
	send(conn, client.OpCodeCommit, nil)
	return readResp(conn, nil)
}

func sendSet(conn net.Conn, key, val string, buf []byte) error {
	// Optimization: Calculate size exactly to single alloc
	totalLen := 4 + len(key) + len(val)
	// In a real high-perf client, we'd use a sync.Pool for this buffer too
	payload := make([]byte, totalLen)

	binary.BigEndian.PutUint32(payload[0:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], val)

	send(conn, client.OpCodeSet, payload)
	return readResp(conn, buf)
}

func sendGet(conn net.Conn, key string, buf []byte) error {
	send(conn, client.OpCodeGet, []byte(key))
	return readResp(conn, buf)
}

func send(w io.Writer, op byte, payload []byte) {
	// Write header + payload in one go if possible to reduce syscalls,
	// but standard net.Conn usually buffers anyway.
	// For strictly adhering to protocol:
	header := make([]byte, 5)
	header[0] = op
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))

	// Combining write might be slightly better for packet coalescence
	if len(payload) > 0 {
		// Small optimization: if payload small, alloc one buffer
		if len(payload) < 1024 {
			full := make([]byte, 5+len(payload))
			copy(full, header)
			copy(full[5:], payload)
			w.Write(full)
			return
		}
	}

	w.Write(header)
	if len(payload) > 0 {
		w.Write(payload)
	}
}

func readResp(r io.Reader, buf []byte) error {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	status := header[0]
	length := binary.BigEndian.Uint32(header[1:])

	if status != client.ResStatusOK && status != client.ResStatusNotFound {
		var msg string
		if length > 0 {
			// Cap error message reading to avoid massive allocations on bogus length
			readLen := length
			if readLen > 1024 {
				readLen = 1024
			}
			msgBytes := make([]byte, readLen)
			if _, err := io.ReadFull(r, msgBytes); err == nil {
				msg = string(msgBytes)
			}
			// Discard rest if truncated
			if length > readLen {
				io.CopyN(io.Discard, r, int64(length-readLen))
			}
		}
		// Return typed error for status code inspection
		return &ServerError{Status: status, Msg: strings.TrimSpace(msg)}
	}

	if length > 0 {
		if buf == nil || uint32(len(buf)) < length {
			// If reusable buffer is too small, just discard the data for benchmarking
			// (unless we strictly need to verify content).
			// For benchmarking, we usually just want to consume the bytes.
			io.CopyN(io.Discard, r, int64(length))
			return nil
		}
		_, err := io.ReadFull(r, buf[:length])
		return err
	}
	return nil
}

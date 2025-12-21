package main

import (
	"bufio"
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
	host         = flag.String("host", "localhost:6379", "Target server address")
	caFile       = flag.String("ca", "", "CA Certificate file")
	certFile     = flag.String("cert", "", "Client Certificate file")
	keyFile      = flag.String("key", "", "Client Key file")
	concurrency  = flag.Int("c", 50, "Number of concurrent connections")
	duration     = flag.Duration("d", 10*time.Second, "Test duration")
	keySpace     = flag.Int("keys", 10000, "Key space size (e.g., key_0 to key_10000)")
	writeRatio   = flag.Float64("w", 0.5, "Write ratio (0.0 - 1.0), default 50%")
	readRatio    = flag.Float64("r", -1.0, "Read ratio (0.0 - 1.0), overrides -w if set")
	valSize      = flag.Int("size", 128, "Size of value payload in bytes")
	pipelineSize = flag.Int("p", 50, "Flush interval (batch size for network writes)")
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
	hasPrintedError int32
)

func main() {
	flag.Parse()

	if *readRatio >= 0 {
		*writeRatio = 1.0 - *readRatio
	}

	fmt.Printf("Starting benchmark against %s\n", *host)
	if *caFile != "" {
		fmt.Println("Security: mTLS Enabled")
	} else {
		fmt.Println("Security: Insecure TCP")
	}
	fmt.Printf("Workers: %d | Duration: %v | Keys: %d | Flush Interval: %d\n", *concurrency, *duration, *keySpace, *pipelineSize)
	fmt.Printf("Payload: %dB | Write Ratio: %.0f%% | Read Ratio: %.0f%%\n", *valSize, *writeRatio*100, (1.0-*writeRatio)*100)
	fmt.Printf("Mode: Async Pipelining with Backpressure\n")

	var wg sync.WaitGroup
	start := time.Now()

	statsCh := make(chan WorkerStats, *concurrency)
	stopCh := make(chan struct{})

	time.AfterFunc(*duration, func() {
		close(stopCh)
	})

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

	allLatencies = make([]time.Duration, 0, *concurrency*10000)

	for s := range statsCh {
		totalOps += s.Ops
		totalErr += s.Errors
		totalConflicts += s.Conflicts
		allLatencies = append(allLatencies, s.Latencies...)
	}

	totalDuration := time.Since(start)
	rps := float64(totalOps) / totalDuration.Seconds()

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

func worker(id int, stopCh <-chan struct{}, statsCh chan<- WorkerStats, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	stats := WorkerStats{Latencies: make([]time.Duration, 0, 10000)}

	// Outer loop handles connection attempts/re-connections
	for {
		// Check stop before reconnecting
		select {
		case <-stopCh:
			statsCh <- stats
			return
		default:
		}

		conn, bw, err := connect(id)
		if err != nil {
			stats.Errors++
			time.Sleep(100 * time.Millisecond) // Don't spam reconnects
			continue
		}

		// Run the async session. Returns only on error or stop.
		runAsyncSession(conn, bw, rng, stopCh, &stats)
		conn.Close()
	}
}

func connect(id int) (net.Conn, *bufio.Writer, error) {
	var conn net.Conn
	var err error

	// 1. mTLS Strategy
	if *caFile != "" && *certFile != "" && *keyFile != "" {
		caCert, readErr := os.ReadFile(*caFile)
		if readErr != nil {
			return nil, nil, fmt.Errorf("read ca: %v", readErr)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		cert, readErr := tls.LoadX509KeyPair(*certFile, *keyFile)
		if readErr != nil {
			return nil, nil, fmt.Errorf("load keys: %v", readErr)
		}

		tlsCfg := &tls.Config{
			RootCAs:      caCertPool,
			Certificates: []tls.Certificate{cert},
		}
		conn, err = tls.Dial("tcp", *host, tlsCfg)
	} else {
		// 2. Plain TCP
		conn, err = net.Dial("tcp", *host)
	}

	if err != nil {
		fmt.Printf("[Worker %d] Connect error: %v\n", id, err)
		return nil, nil, err
	}

	// Use a large write buffer to minimize syscalls
	return conn, bufio.NewWriterSize(conn, 32*1024), nil
}

func runAsyncSession(conn net.Conn, bw *bufio.Writer, rng *rand.Rand, stopCh <-chan struct{}, stats *WorkerStats) {
	// IMPORTANT: Tying channel capacity to pipeline size prevents buffer bloat.
	// This creates "backpressure".
	queueDepth := *pipelineSize * 2
	if queueDepth < 50 {
		queueDepth = 50
	}
	inflightCh := make(chan time.Time, queueDepth)

	// Error channel to signal failure from either side
	errCh := make(chan error, 2)

	// Pre-gen payload
	staticVal := make([]byte, *valSize)
	rng.Read(staticVal)
	valStr := string(staticVal)
	readBuf := make([]byte, 4096)

	// --- READER GOROUTINE ---
	go func() {
		defer close(errCh) // Signal writer to stop if reader exits
		for startT := range inflightCh {
			// Expect 3 responses per transaction: Begin, Op, Commit
			var txConflict bool

			// 1. BEGIN Resp
			if err := readResp(conn, nil); err != nil {
				errCh <- err
				return
			}

			// 2. OP Resp
			if err := readResp(conn, readBuf); err != nil {
				// We must consume the protocol even on error
				if _, ok := err.(*ServerError); !ok {
					errCh <- err
					return
				}
			}

			// 3. COMMIT Resp
			if err := readResp(conn, nil); err != nil {
				if se, ok := err.(*ServerError); ok && se.Status == client.ResStatusTxConflict {
					txConflict = true
				} else {
					errCh <- err
					return
				}
			}

			// Stats
			if txConflict {
				stats.Conflicts++
			} else {
				stats.Ops++
			}
			stats.Latencies = append(stats.Latencies, time.Since(startT))
		}
	}()

	// --- WRITER LOOP ---
	flushInterval := *pipelineSize
	if flushInterval < 1 {
		flushInterval = 1
	}
	opsSinceFlush := 0

	for {
		select {
		case <-stopCh:
			return // Exit cleanly
		case err := <-errCh:
			// Reader failed
			if atomic.CompareAndSwapInt32(&hasPrintedError, 0, 1) {
				fmt.Printf("\n[!] Error: %v\n", err)
			}
			stats.Errors++
			return
		default:
			// Send Request
			key := fmt.Sprintf("key_%d", rng.Intn(*keySpace))
			isWrite := rng.Float64() < *writeRatio

			// Track time before writing to buffer
			t := time.Now()

			// Write 3 commands
			writeBegin(bw)
			if isWrite {
				writeSet(bw, key, valStr)
			} else {
				writeGet(bw, key)
			}
			writeCommit(bw)

			// Notify Reader (Backpressure point)
			select {
			case inflightCh <- t:
			case err := <-errCh:
				if atomic.CompareAndSwapInt32(&hasPrintedError, 0, 1) {
					fmt.Printf("\n[!] Error: %v\n", err)
				}
				stats.Errors++
				return
			}

			opsSinceFlush++
			if opsSinceFlush >= flushInterval {
				if err := bw.Flush(); err != nil {
					stats.Errors++
					return
				}
				opsSinceFlush = 0
			}
		}
	}
}

// --- Protocol Helpers (Optimized) ---

func writeBegin(w io.Writer) {
	write(w, client.OpCodeBegin, nil)
}

func writeCommit(w io.Writer) {
	write(w, client.OpCodeCommit, nil)
}

func writeSet(w io.Writer, key, val string) {
	totalLen := 4 + len(key) + len(val)
	writeHeader(w, client.OpCodeSet, totalLen)

	// Optimized: Manual uint32 write to avoid allocation/reflection
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(key)))
	w.Write(buf[:])

	io.WriteString(w, key)
	io.WriteString(w, val)
}

func writeGet(w io.Writer, key string) {
	write(w, client.OpCodeGet, []byte(key))
}

func writeHeader(w io.Writer, op byte, length int) {
	var header [5]byte
	header[0] = op
	binary.BigEndian.PutUint32(header[1:], uint32(length))
	w.Write(header[:])
}

func write(w io.Writer, op byte, payload []byte) {
	writeHeader(w, op, len(payload))
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
			readLen := length
			if readLen > 1024 {
				readLen = 1024
			}
			msgBytes := make([]byte, readLen)
			if _, err := io.ReadFull(r, msgBytes); err == nil {
				msg = string(msgBytes)
			}
			if length > readLen {
				io.CopyN(io.Discard, r, int64(length-readLen))
			}
		}
		return &ServerError{Status: status, Msg: strings.TrimSpace(msg)}
	}

	if length > 0 {
		if buf == nil {
			io.CopyN(io.Discard, r, int64(length))
			return nil
		}
		if uint32(len(buf)) < length {
			io.CopyN(io.Discard, r, int64(length))
			return nil
		}
		_, err := io.ReadFull(r, buf[:length])
		return err
	}
	return nil
}

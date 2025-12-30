package main

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/client"
)

var (
	addr          = flag.String("addr", "localhost:6379", "Server address")
	caFile        = flag.String("ca", "certs/ca.crt", "Path to CA certificate")
	certFile      = flag.String("cert", "certs/client.crt", "Path to Client certificate")
	keyFile       = flag.String("key", "certs/client.key", "Path to Client key")
	concurrency   = flag.Int("c", 50, "Number of concurrent clients")
	totalOps      = flag.Int("n", 10000, "Total number of operations per phase")
	valueSize     = flag.Int("v", 128, "Value size in bytes (for SET operations)")
	keySize       = flag.Int("k", 32, "Minimum key size in bytes (padded if shorter)")
	readRatio     = flag.Float64("ratio", -1.0, "Read ratio (0.0 to 1.0). If set, runs a mixed workload")
	pipelineDepth = flag.Int("depth", 1, "Pipeline depth (transactions per network round-trip)")
)

func main() {
	flag.Parse()

	if *totalOps <= 0 || *concurrency <= 0 || *pipelineDepth <= 0 {
		log.Fatal("Invalid -n, -c, or -depth values. Must be > 0")
	}

	// Prepare payload once
	payload := make([]byte, *valueSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("Failed to generate payload: %v", err)
	}

	fmt.Printf("--- TurnstoneDB Benchmark (Async Pipeline) ---\n")
	fmt.Printf("Server:      %s\n", *addr)
	fmt.Printf("Concurrency: %d clients\n", *concurrency)
	fmt.Printf("Total Ops:   %d\n", *totalOps)
	fmt.Printf("Pipeline:    %d tx/batch\n", *pipelineDepth)
	fmt.Printf("Payload:     %d bytes\n", *valueSize)

	mode := "Sequential (Write -> Read)"
	if *readRatio >= 0.0 && *readRatio <= 1.0 {
		mode = fmt.Sprintf("Mixed (%.0f%% Read / %.0f%% Write)", *readRatio*100, (1.0-*readRatio)*100)
	}
	fmt.Printf("Mode:        %s\n", mode)
	fmt.Println("--------------------------------------------------")

	tlsConfig, err := loadTLSConfig()
	if err != nil {
		log.Fatal(err)
	}

	if *readRatio >= 0.0 && *readRatio <= 1.0 {
		runWorkload("MIXED", tlsConfig, payload, *readRatio)
	} else {
		runWorkload("WRITE", tlsConfig, payload, 0.0)
		runWorkload("READ ", tlsConfig, payload, 1.0)
	}
}

func loadTLSConfig() (*tls.Config, error) {
	caCert, err := os.ReadFile(*caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA: %w", err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load certs: %w", err)
	}

	return &tls.Config{
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, nil
}

func generateKey(clientID, index int) string {
	baseKey := fmt.Sprintf("bench-%d-%d", clientID, index)
	if len(baseKey) < *keySize {
		return baseKey + strings.Repeat("x", *keySize-len(baseKey))
	}
	return baseKey
}

func runWorkload(phase string, tlsConfig *tls.Config, payload []byte, readPct float64) {
	fmt.Printf("Starting %s phase...\n", phase)

	var wg sync.WaitGroup
	var completedOps int64
	var failedOps int64
	var notFoundOps int64
	var totalDuration int64

	opsPerClient := *totalOps / *concurrency
	// Adjust ops to be a multiple of depth for simplicity
	if opsPerClient < *pipelineDepth {
		opsPerClient = *pipelineDepth
	}
	batchesPerClient := opsPerClient / *pipelineDepth

	startTotal := time.Now()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Local random source
			seed, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
			r := mrand.New(mrand.NewSource(seed.Int64()))

			conn, err := tls.Dial("tcp", *addr, tlsConfig)
			if err != nil {
				log.Printf("[Client %d] Dial failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(opsPerClient))
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)
			// Pre-allocate write buffer: (Header(5) + Max(Begin/Commit/Op)) * 3 * Depth
			// Estimate roughly 4KB + payload per depth to be safe
			writeBuf := make([]byte, 0, *pipelineDepth*(20+*valueSize+*keySize)*3)
			headerBuf := make([]byte, 5)

			for b := 0; b < batchesPerClient; b++ {
				writeBuf = writeBuf[:0]
				startBatch := time.Now()

				// 1. Build Pipeline Request
				for d := 0; d < *pipelineDepth; d++ {
					// Determine Op
					isRead := false
					if phase == "READ " {
						isRead = true
					} else if phase == "WRITE" {
						isRead = false
					} else {
						isRead = r.Float64() < readPct
					}

					// Key selection
					keyIndex := b*(*pipelineDepth) + d
					if phase == "MIXED" {
						keyIndex = r.Intn(opsPerClient)
					}
					key := generateKey(clientID, keyIndex)

					// Append BEGIN
					writeBuf = appendHeader(writeBuf, client.OpCodeBegin, 0)

					// Append OP
					if isRead {
						writeBuf = appendHeader(writeBuf, client.OpCodeGet, len(key))
						writeBuf = append(writeBuf, key...)
					} else {
						// Set Format: [4b KeyLen][Key][Val]
						kLen := len(key)
						vLen := len(payload)
						totalLen := 4 + kLen + vLen
						writeBuf = appendHeader(writeBuf, client.OpCodeSet, totalLen)

						// We need to append 4-byte key length
						var lenBytes [4]byte
						binary.BigEndian.PutUint32(lenBytes[:], uint32(kLen))
						writeBuf = append(writeBuf, lenBytes[:]...)
						writeBuf = append(writeBuf, key...)
						writeBuf = append(writeBuf, payload...)
					}

					// Append COMMIT
					writeBuf = appendHeader(writeBuf, client.OpCodeCommit, 0)
				}

				// 2. Flush Write
				if _, err := conn.Write(writeBuf); err != nil {
					atomic.AddInt64(&failedOps, int64(*pipelineDepth))
					return // Connection dead
				}

				// 3. Read Responses (3 per transaction * depth)
				batchFailed := false
				for d := 0; d < *pipelineDepth*3; d++ {
					if _, err := io.ReadFull(reader, headerBuf); err != nil {
						atomic.AddInt64(&failedOps, int64(*pipelineDepth))
						return
					}
					status := headerBuf[0]
					length := binary.BigEndian.Uint32(headerBuf[1:])

					// Skip body
					if length > 0 {
						if _, err := reader.Discard(int(length)); err != nil {
							atomic.AddInt64(&failedOps, int64(*pipelineDepth))
							return
						}
					}

					// Check status
					if status != client.ResStatusOK && status != client.ResStatusNotFound {
						batchFailed = true
					}
					if status == client.ResStatusNotFound {
						atomic.AddInt64(&notFoundOps, 1) // Count per op roughly
					}
				}

				latency := time.Since(startBatch).Nanoseconds()

				// In pipeline mode, we amortize latency across the batch
				// This isn't perfect per-op latency but gives good throughput indication
				if batchFailed {
					atomic.AddInt64(&failedOps, int64(*pipelineDepth))
				} else {
					atomic.AddInt64(&completedOps, int64(*pipelineDepth))
					atomic.AddInt64(&totalDuration, latency)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(startTotal)
	printStats(phase, elapsed, completedOps, failedOps, notFoundOps, totalDuration)
}

func appendHeader(buf []byte, op byte, length int) []byte {
	var header [5]byte
	header[0] = op
	binary.BigEndian.PutUint32(header[1:], uint32(length))
	return append(buf, header[:]...)
}

func printStats(phase string, elapsed time.Duration, success, failed, notFound int64, totalLatencyNs int64) {
	tps := float64(success) / elapsed.Seconds()

	// Since latency was tracked per batch, we need to adjust
	// But simply: totalDuration is sum of batch latencies.
	// Average Op Latency = (Total Batch Latencies) / (Number of Batches) / Depth?
	// Actually: (Sum of Batch Durations) / Total Ops isn't quite right for "Per Op Latency"
	// because ops happened in parallel on the wire.
	// However, usually in pipeline bench, Latency = BatchRTT / Depth.

	avgLatency := float64(0)
	if success > 0 {
		// Calculate average time per operation roughly
		avgLatency = (float64(totalLatencyNs) / float64(success)) / 1e6
	}

	fmt.Printf("Phase: %s\n", phase)
	fmt.Printf("  Duration:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total Ops:   %d\n", success+failed)
	fmt.Printf("  Successful:  %d\n", success)
	fmt.Printf("  Not Found:   %d\n", notFound)
	fmt.Printf("  Failed:      %d\n", failed)
	fmt.Printf("  Throughput:  %.2f TPS\n", tps)
	fmt.Printf("  Avg Latency: %.3f ms (Amortized)\n", avgLatency)
	fmt.Println("--------------------------------------------------")
}

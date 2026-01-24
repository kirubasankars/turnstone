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
	batchSize     = flag.Int("batch", 1, "Batch size (operations per transaction)")
	dbNum         = flag.Int("db", 1, "Database number to use (DB 0 is typically read-only)")
	keyPrefix     = flag.String("prefix", "bench", "Key prefix to avoid collisions between concurrent benchmark runs")
)

func main() {
	flag.Parse()

	if *totalOps <= 0 || *concurrency <= 0 || *pipelineDepth <= 0 || *batchSize <= 0 {
		log.Fatal("Invalid -n, -c, -depth, or -batch values. Must be > 0")
	}

	// Prepare payload once
	payload := make([]byte, *valueSize)
	if _, err := rand.Read(payload); err != nil {
		log.Fatalf("Failed to generate payload: %v", err)
	}

	fmt.Printf("--- TurnstoneDB Benchmark (Async Pipeline) ---\n")
	fmt.Printf("Server:       %s\n", *addr)
	fmt.Printf("Database:     %d\n", *dbNum)
	fmt.Printf("Concurrency:  %d clients\n", *concurrency)
	fmt.Printf("Total Ops:    %d\n", *totalOps)
	fmt.Printf("Pipeline:     %d tx/batch (inflight)\n", *pipelineDepth)
	fmt.Printf("Batch Size:   %d ops/tx\n", *batchSize)
	fmt.Printf("Payload:      %d bytes\n", *valueSize)
	fmt.Printf("Key Prefix:   %s\n", *keyPrefix)

	mode := "Sequential (Write -> Read)"
	if *readRatio >= 0.0 && *readRatio <= 1.0 {
		mode = fmt.Sprintf("Mixed (%.0f%% Read / %.0f%% Write)", *readRatio*100, (1.0-*readRatio)*100)
	}
	fmt.Printf("Mode:         %s\n", mode)
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
	baseKey := fmt.Sprintf("%s-%d-%d", *keyPrefix, clientID, index)
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

	// Adjust calculations for batch size
	opsPerClient := *totalOps / *concurrency
	txsPerClient := opsPerClient / *batchSize
	if txsPerClient == 0 {
		txsPerClient = 1
	}

	// batchesOfPipeline is how many times we fill the pipeline depth
	batchesOfPipeline := txsPerClient / *pipelineDepth
	if batchesOfPipeline == 0 {
		batchesOfPipeline = 1
	}

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

			// --- Select Database ---
			dbName := []byte(fmt.Sprintf("%d", *dbNum))
			selBuf := appendHeader(make([]byte, 0, 5+len(dbName)), client.OpCodeSelect, len(dbName))
			selBuf = append(selBuf, dbName...)
			if _, err := conn.Write(selBuf); err != nil {
				log.Printf("[Client %d] Select write failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(opsPerClient))
				return
			}

			// Read Select Response
			selHead := make([]byte, 5)
			if _, err := io.ReadFull(reader, selHead); err != nil {
				log.Printf("[Client %d] Select read failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(opsPerClient))
				return
			}
			if selHead[0] != client.ResStatusOK {
				log.Printf("[Client %d] Select failed status: 0x%x", clientID, selHead[0])
				atomic.AddInt64(&failedOps, int64(opsPerClient))
				return
			}
			if sLen := binary.BigEndian.Uint32(selHead[1:]); sLen > 0 {
				if _, err := reader.Discard(int(sLen)); err != nil {
					return
				}
			}
			// -------------------------

			// Buffer sizing: Depth * (Begin(20) + Commit(20) + BatchSize * OpSize)
			estOpSize := 20 + *valueSize + *keySize
			writeBuf := make([]byte, 0, *pipelineDepth*(40+(*batchSize*estOpSize)))
			headerBuf := make([]byte, 5)

			txCount := 0
			for b := 0; b < batchesOfPipeline; b++ {
				writeBuf = writeBuf[:0]
				startBatch := time.Now()

				// 1. Build Pipeline Request (Depth = Number of parallel Transactions)
				for d := 0; d < *pipelineDepth; d++ {
					// Append BEGIN
					writeBuf = appendHeader(writeBuf, client.OpCodeBegin, 0)

					// Append Batch of OPs
					for k := 0; k < *batchSize; k++ {
						// Determine Op Type
						isRead := false
						if phase == "READ " {
							isRead = true
						} else if phase == "WRITE" {
							isRead = false
						} else {
							isRead = r.Float64() < readPct
						}

						// Key selection
						keyIndex := (txCount * *batchSize) + k
						if phase == "MIXED" {
							keyIndex = r.Intn(opsPerClient)
						}
						key := generateKey(clientID, keyIndex)

						if isRead {
							writeBuf = appendHeader(writeBuf, client.OpCodeGet, len(key))
							writeBuf = append(writeBuf, key...)
						} else {
							kLen := len(key)
							totalLen := 4 + kLen + len(payload)
							writeBuf = appendHeader(writeBuf, client.OpCodeSet, totalLen)
							var lenBytes [4]byte
							binary.BigEndian.PutUint32(lenBytes[:], uint32(kLen))
							writeBuf = append(writeBuf, lenBytes[:]...)
							writeBuf = append(writeBuf, key...)
							writeBuf = append(writeBuf, payload...)
						}
					}

					// Append COMMIT
					writeBuf = appendHeader(writeBuf, client.OpCodeCommit, 0)
					txCount++
				}

				// 2. Flush Write
				if _, err := conn.Write(writeBuf); err != nil {
					atomic.AddInt64(&failedOps, int64(*pipelineDepth**batchSize))
					return
				}

				// 3. Read Responses
				// Expect: Depth * (1 Begin + BatchSize Ops + 1 Commit)
				expectedResps := *pipelineDepth * (2 + *batchSize)
				batchFailed := false

				for i := 0; i < expectedResps; i++ {
					if _, err := io.ReadFull(reader, headerBuf); err != nil {
						atomic.AddInt64(&failedOps, int64(*pipelineDepth**batchSize))
						return
					}
					status := headerBuf[0]
					length := binary.BigEndian.Uint32(headerBuf[1:])

					if length > 0 {
						if _, err := reader.Discard(int(length)); err != nil {
							atomic.AddInt64(&failedOps, int64(*pipelineDepth**batchSize))
							return
						}
					}

					if status != client.ResStatusOK && status != client.ResStatusNotFound {
						batchFailed = true
					}
					if status == client.ResStatusNotFound {
						atomic.AddInt64(&notFoundOps, 1)
					}
				}

				latency := time.Since(startBatch).Nanoseconds()

				opsInBatch := int64(*pipelineDepth * *batchSize)
				if batchFailed {
					atomic.AddInt64(&failedOps, opsInBatch)
				} else {
					atomic.AddInt64(&completedOps, opsInBatch)
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
	avgLatency := float64(0)

	// Note: Latency calculation here is "Latency per Network Round Trip" (Pipeline Batch)
	// It is roughly (Network RTT / (Depth * BatchSize)) if we amortize,
	// but strictly speaking, it's the time the client waited for the whole batch.
	if success > 0 {
		avgLatency = (float64(totalLatencyNs) / float64(success)) / 1e6
	}

	fmt.Printf("Phase: %s\n", phase)
	fmt.Printf("  Duration:    %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Total Ops:   %d\n", success+failed)
	fmt.Printf("  Successful:  %d\n", success)
	fmt.Printf("  Not Found:   %d\n", notFound)
	fmt.Printf("  Failed:      %d\n", failed)
	fmt.Printf("  Throughput:  %.2f TPS\n", tps)
	fmt.Printf("  Avg Latency: %.3f ms (Amortized per Op)\n", avgLatency)
	fmt.Println("--------------------------------------------------")
}

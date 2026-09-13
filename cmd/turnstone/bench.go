// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package main

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"turnstone/client"
	"turnstone/internal/tlsutil"
	"turnstone/protocol"
)

const benchSoakKeySpace = 10000

func newBenchCmd() *cobra.Command {
	var addr string
	var concurrency int
	var totalOps int
	var valueSize int
	var keySize int
	var readRatio float64
	var pipelineDepth int
	var batchSize int
	var dbNum int
	var keyPrefix string
	var duration time.Duration
	var report time.Duration

	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Run a load and throughput benchmark",
		Long: `Drive GET/SET traffic against a running primary.

Default: a fixed --ops count (write then read, or mixed if --read-ratio is set).
With --duration, soak until the timer expires instead of --ops.
The database must be PRIMARY (or start the server with --dev).`,
		Run: func(cmd *cobra.Command, args []string) {
			if concurrency <= 0 || pipelineDepth <= 0 || batchSize <= 0 {
				log.Fatal("Invalid --concurrency, --depth, or --batch. Must be > 0")
			}
			if duration < 0 || report < 0 {
				log.Fatal("Invalid --duration or --report. Must be >= 0")
			}
			if duration == 0 && totalOps <= 0 {
				log.Fatal("Invalid --ops. Must be > 0")
			}
			if readRatio > 1 {
				log.Fatal("Invalid --read-ratio. Must be between 0.0 and 1.0")
			}

			payload := make([]byte, valueSize)
			if _, err := rand.Read(payload); err != nil {
				log.Fatalf("Failed to generate payload: %v", err)
			}

			soak := duration > 0
			if soak && !cmd.Flags().Changed("report") {
				report = 5 * time.Second
			}
			if soak && readRatio < 0 {
				readRatio = 0.5
			}

			fmt.Printf("--- TurnstoneDB Benchmark (Async Pipeline) ---\n")
			fmt.Printf("Server:       %s\n", addr)
			fmt.Printf("Home:         %s\n", homeDir)
			fmt.Printf("Database:     %d\n", dbNum)
			fmt.Printf("Concurrency:  %d clients\n", concurrency)
			if soak {
				fmt.Printf("Duration:     %s\n", duration)
			} else {
				fmt.Printf("Total Ops:    %d\n", totalOps)
			}
			fmt.Printf("Pipeline:     %d tx/batch (inflight)\n", pipelineDepth)
			fmt.Printf("Batch Size:   %d ops/tx\n", batchSize)
			fmt.Printf("Payload:      %d bytes\n", valueSize)
			fmt.Printf("Key Prefix:   %s\n", keyPrefix)

			mode := "Sequential (Write -> Read)"
			if readRatio >= 0.0 && readRatio <= 1.0 {
				mode = fmt.Sprintf("Mixed (%.0f%% Read / %.0f%% Write)", readRatio*100, (1.0-readRatio)*100)
			}
			fmt.Printf("Mode:         %s\n", mode)
			fmt.Println("--------------------------------------------------")

			tlsConfig, err := tlsutil.LoadFromHome(homeDir, tlsutil.RoleClient)
			if err != nil {
				log.Fatal(err)
			}

			if err := benchPreflight(addr, dbNum); err != nil {
				log.Fatal(err)
			}

			if soak {
				runDurationWorkload(addr, dbNum, concurrency, pipelineDepth, batchSize, valueSize, keySize, keyPrefix, duration, report, tlsConfig, payload, readRatio)
				return
			}

			if readRatio >= 0.0 && readRatio <= 1.0 {
				runWorkload(addr, dbNum, concurrency, totalOps, pipelineDepth, batchSize, valueSize, keySize, keyPrefix, "MIXED", tlsConfig, payload, readRatio)
			} else {
				runWorkload(addr, dbNum, concurrency, totalOps, pipelineDepth, batchSize, valueSize, keySize, keyPrefix, "WRITE", tlsConfig, payload, 0.0)
				runWorkload(addr, dbNum, concurrency, totalOps, pipelineDepth, batchSize, valueSize, keySize, keyPrefix, "READ ", tlsConfig, payload, 1.0)
			}
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "localhost:6379", "Server address")
	cmd.Flags().IntVar(&concurrency, "concurrency", 50, "Number of concurrent clients")
	cmd.Flags().IntVar(&totalOps, "ops", 10000, "Total number of operations per phase")
	cmd.Flags().DurationVar(&duration, "duration", 0, "Run until this elapsed time instead of --ops")
	cmd.Flags().DurationVar(&report, "report", 0, "Live stats interval (0 disables; defaults to 5s with --duration)")
	cmd.Flags().IntVar(&valueSize, "value-size", 128, "Value size in bytes (for SET operations)")
	cmd.Flags().IntVar(&keySize, "key-size", 32, "Minimum key size in bytes (padded if shorter)")
	cmd.Flags().Float64Var(&readRatio, "read-ratio", -1.0, "Read ratio (0.0 to 1.0). If set, runs a mixed workload")
	cmd.Flags().IntVar(&pipelineDepth, "depth", 1, "Pipeline depth (transactions per network round-trip)")
	cmd.Flags().IntVar(&batchSize, "batch", 1, "Batch size (operations per transaction)")
	cmd.Flags().IntVar(&dbNum, "db", 1, "Database number to use (DB 0 is typically read-only)")
	cmd.Flags().StringVar(&keyPrefix, "prefix", "bench", "Key prefix to avoid collisions between concurrent benchmark runs")

	return cmd
}

func benchPreflight(addr string, dbNum int) error {
	caPath, certPath, keyPath := tlsutil.CertPaths(homeDir, tlsutil.RoleClient)

	cl, err := client.NewMTLSClientHelper(addr, caPath, certPath, keyPath, nil)
	if err != nil {
		return fmt.Errorf("preflight connect failed: %w", err)
	}
	defer cl.Close()

	dbName := fmt.Sprintf("%d", dbNum)
	if err := cl.Select(dbName); err != nil {
		return fmt.Errorf("preflight SELECT %s failed: %w", dbName, err)
	}

	raw, err := cl.Stat()
	if err != nil {
		return fmt.Errorf("preflight STAT failed: %w", err)
	}
	var st struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("preflight STAT parse failed: %w (body=%q)", err, raw)
	}
	if st.State != "PRIMARY" {
		return fmt.Errorf(`database %s is %s; writes require PRIMARY

Databases start UNDEFINED after init and reject BEGIN/SET/GET until promoted.

  turnstone cli --admin --home %s
  %s> select %s
  %s> promote

Or start the server with --dev to auto-promote every database`,
			dbName, st.State, homeDir, dbName, dbName, dbName)
	}
	return nil
}

func generateKey(keyPrefix string, keySize int, clientID, index int) string {
	baseKey := fmt.Sprintf("%s-%d-%d", keyPrefix, clientID, index)
	if len(baseKey) < keySize {
		return baseKey + strings.Repeat("x", keySize-len(baseKey))
	}
	return baseKey
}

func runWorkload(
	addr string,
	dbNum int,
	concurrency int,
	totalOps int,
	pipelineDepth int,
	batchSize int,
	valueSize int,
	keySize int,
	keyPrefix string,
	phase string,
	tlsConfig *tls.Config,
	payload []byte,
	readPct float64,
) {
	fmt.Printf("Starting %s phase...\n", phase)

	var wg sync.WaitGroup
	var completedOps int64
	var failedOps int64
	var notFoundOps int64
	var totalDuration int64

	baseOps := totalOps / concurrency
	remainder := totalOps % concurrency

	startTotal := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

		opsForThisClient := baseOps
		if i < remainder {
			opsForThisClient++
		}

		go func(clientID int, numOps int) {
			defer wg.Done()

			if numOps == 0 {
				return
			}

			txsPerClient := numOps / batchSize
			if txsPerClient == 0 && numOps > 0 {
				txsPerClient = 1
			}

			batchesOfPipeline := txsPerClient / pipelineDepth
			if batchesOfPipeline == 0 && txsPerClient > 0 {
				batchesOfPipeline = 1
			}

			seed, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
			r := mrand.New(mrand.NewSource(seed.Int64()))

			conn, err := tls.Dial("tcp", addr, tlsConfig)
			if err != nil {
				log.Printf("[Client %d] Dial failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(numOps))
				return
			}
			defer conn.Close()

			reader := bufio.NewReader(conn)

			dbName := []byte(fmt.Sprintf("%d", dbNum))
			selBuf := protocol.AppendHeader(make([]byte, 0, 5+len(dbName)), protocol.OpCodeSelect, len(dbName))
			selBuf = append(selBuf, dbName...)
			if _, err := conn.Write(selBuf); err != nil {
				log.Printf("[Client %d] Select write failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(numOps))
				return
			}

			selHead := make([]byte, 5)
			if _, err := io.ReadFull(reader, selHead); err != nil {
				log.Printf("[Client %d] Select read failed: %v", clientID, err)
				atomic.AddInt64(&failedOps, int64(numOps))
				return
			}
			if selHead[0] != protocol.ResStatusOK {
				log.Printf("[Client %d] Select failed status: 0x%x", clientID, selHead[0])
				atomic.AddInt64(&failedOps, int64(numOps))
				return
			}
			if sLen := binary.BigEndian.Uint32(selHead[1:]); sLen > 0 {
				if _, err := reader.Discard(int(sLen)); err != nil {
					return
				}
			}

			estOpSize := 20 + valueSize + keySize
			writeBuf := make([]byte, 0, pipelineDepth*(40+(batchSize*estOpSize)))
			headerBuf := make([]byte, 5)
			var logFail sync.Once

			txCount := 0

			for b := 0; b < batchesOfPipeline; b++ {
				writeBuf = writeBuf[:0]
				startBatch := time.Now()

				for d := 0; d < pipelineDepth; d++ {
					opIsRead := make([]bool, batchSize)
					allRead := true
					for k := 0; k < batchSize; k++ {
						isRead := false
						if phase == "READ " {
							isRead = true
						} else if phase == "WRITE" {
							isRead = false
						} else {
							isRead = r.Float64() < readPct
						}
						opIsRead[k] = isRead
						if !isRead {
							allRead = false
						}
					}

					if allRead {
						writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeBegin, 1)
						writeBuf = append(writeBuf, protocol.BeginReadOnly)
					} else {
						writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeBegin, 0)
					}

					for k := 0; k < batchSize; k++ {
						isRead := opIsRead[k]

						keyIndex := (txCount * batchSize) + k
						if phase == "MIXED" {
							keyIndex = r.Intn(numOps)
						}
						key := generateKey(keyPrefix, keySize, clientID, keyIndex)

						if isRead {
							writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeGet, len(key))
							writeBuf = append(writeBuf, key...)
						} else {
							kLen := len(key)
							totalLen := 4 + kLen + len(payload)
							writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeSet, totalLen)
							var lenBytes [4]byte
							binary.BigEndian.PutUint32(lenBytes[:], uint32(kLen))
							writeBuf = append(writeBuf, lenBytes[:]...)
							writeBuf = append(writeBuf, key...)
							writeBuf = append(writeBuf, payload...)
						}
					}

					writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeCommit, 0)
					txCount++
				}

				if _, err := conn.Write(writeBuf); err != nil {
					atomic.AddInt64(&failedOps, int64(pipelineDepth*batchSize))
					return
				}

				expectedResps := pipelineDepth * (2 + batchSize)
				batchFailed := false

				for i := 0; i < expectedResps; i++ {
					if _, err := io.ReadFull(reader, headerBuf); err != nil {
						atomic.AddInt64(&failedOps, int64(pipelineDepth*batchSize))
						return
					}
					status := headerBuf[0]
					length := binary.BigEndian.Uint32(headerBuf[1:])
					failedStatus := status != protocol.ResStatusOK && status != protocol.ResStatusNotFound

					var errBody string
					if length > 0 {
						if failedStatus {
							buf := make([]byte, length)
							if _, err := io.ReadFull(reader, buf); err != nil {
								atomic.AddInt64(&failedOps, int64(pipelineDepth*batchSize))
								return
							}
							errBody = string(buf)
						} else if _, err := reader.Discard(int(length)); err != nil {
							atomic.AddInt64(&failedOps, int64(pipelineDepth*batchSize))
							return
						}
					}

					if failedStatus {
						batchFailed = true
						logFail.Do(func() {
							log.Printf("%s first failure: status=0x%02x (%s) body=%q", phase, status, protocol.StatusName(status), errBody)
						})
					}
					if status == protocol.ResStatusNotFound {
						atomic.AddInt64(&notFoundOps, 1)
					}
				}

				latency := time.Since(startBatch).Nanoseconds()

				opsInBatch := int64(pipelineDepth * batchSize)
				if batchFailed {
					atomic.AddInt64(&failedOps, opsInBatch)
				} else {
					atomic.AddInt64(&completedOps, opsInBatch)
					atomic.AddInt64(&totalDuration, latency)
				}
			}
		}(i, opsForThisClient)
	}

	wg.Wait()
	elapsed := time.Since(startTotal)
	printBenchStats(phase, elapsed, completedOps, failedOps, notFoundOps, totalDuration)
}

func runDurationWorkload(
	addr string,
	dbNum int,
	concurrency int,
	pipelineDepth int,
	batchSize int,
	valueSize int,
	keySize int,
	keyPrefix string,
	duration time.Duration,
	report time.Duration,
	tlsConfig *tls.Config,
	payload []byte,
	readPct float64,
) {
	phase := "MIXED"
	if readPct == 0 {
		phase = "WRITE"
	} else if readPct == 1 {
		phase = "READ "
	}
	fmt.Printf("Starting %s phase...\n", phase)

	var wg sync.WaitGroup
	var completedOps int64
	var failedOps int64
	var notFoundOps int64
	var totalDuration int64

	deadline := time.Now().Add(duration)
	startTotal := time.Now()
	stopReport := startBenchReporter(report, startTotal, &completedOps, &failedOps)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			runDurationClient(addr, dbNum, clientID, pipelineDepth, batchSize, valueSize, keySize, keyPrefix, deadline, tlsConfig, payload, readPct, phase, &completedOps, &failedOps, &notFoundOps, &totalDuration)
		}(i)
	}

	wg.Wait()
	stopReport()
	elapsed := time.Since(startTotal)
	printBenchStats(phase, elapsed, completedOps, failedOps, notFoundOps, totalDuration)
}

func startBenchReporter(interval time.Duration, start time.Time, completed, failed *int64) func() {
	if interval <= 0 {
		return func() {}
	}
	stopCh := make(chan struct{})
	done := make(chan struct{})
	ticker := time.NewTicker(interval)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(start)
				success := atomic.LoadInt64(completed)
				fail := atomic.LoadInt64(failed)
				tps := float64(0)
				if elapsed.Seconds() > 0 {
					tps = float64(success) / elapsed.Seconds()
				}
				fmt.Printf("  [%s] success=%d failed=%d tps=%.0f\n",
					elapsed.Round(time.Second), success, fail, tps)
			case <-stopCh:
				return
			}
		}
	}()
	return func() {
		close(stopCh)
		<-done
	}
}

func runDurationClient(
	addr string,
	dbNum int,
	clientID int,
	pipelineDepth int,
	batchSize int,
	valueSize int,
	keySize int,
	keyPrefix string,
	deadline time.Time,
	tlsConfig *tls.Config,
	payload []byte,
	readPct float64,
	phase string,
	completedOps, failedOps, notFoundOps, totalDuration *int64,
) {
	seed, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	r := mrand.New(mrand.NewSource(seed.Int64()))

	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		log.Printf("[Client %d] Dial failed: %v", clientID, err)
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	dbName := []byte(fmt.Sprintf("%d", dbNum))
	selBuf := protocol.AppendHeader(make([]byte, 0, 5+len(dbName)), protocol.OpCodeSelect, len(dbName))
	selBuf = append(selBuf, dbName...)
	if _, err := conn.Write(selBuf); err != nil {
		log.Printf("[Client %d] Select write failed: %v", clientID, err)
		return
	}

	selHead := make([]byte, 5)
	if _, err := io.ReadFull(reader, selHead); err != nil {
		log.Printf("[Client %d] Select read failed: %v", clientID, err)
		return
	}
	if selHead[0] != protocol.ResStatusOK {
		log.Printf("[Client %d] Select failed status: 0x%x", clientID, selHead[0])
		return
	}
	if sLen := binary.BigEndian.Uint32(selHead[1:]); sLen > 0 {
		if _, err := reader.Discard(int(sLen)); err != nil {
			return
		}
	}

	estOpSize := 20 + valueSize + keySize
	writeBuf := make([]byte, 0, pipelineDepth*(40+(batchSize*estOpSize)))
	headerBuf := make([]byte, 5)
	var logFail sync.Once
	writeSeq := 0

	for time.Now().Before(deadline) {
		writeBuf = writeBuf[:0]
		startBatch := time.Now()

		for d := 0; d < pipelineDepth; d++ {
			opIsRead := make([]bool, batchSize)
			keyIndex := make([]int, batchSize)
			allRead := true
			for k := 0; k < batchSize; k++ {
				isRead := r.Float64() < readPct
				if isRead && writeSeq == 0 {
					isRead = false
				}
				opIsRead[k] = isRead
				if isRead {
					span := writeSeq
					if span > benchSoakKeySpace {
						span = benchSoakKeySpace
					}
					keyIndex[k] = r.Intn(span)
				} else {
					keyIndex[k] = writeSeq % benchSoakKeySpace
					writeSeq++
					allRead = false
				}
			}

			if allRead {
				writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeBegin, 1)
				writeBuf = append(writeBuf, protocol.BeginReadOnly)
			} else {
				writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeBegin, 0)
			}

			for k := 0; k < batchSize; k++ {
				key := generateKey(keyPrefix, keySize, clientID, keyIndex[k])
				if opIsRead[k] {
					writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeGet, len(key))
					writeBuf = append(writeBuf, key...)
				} else {
					kLen := len(key)
					totalLen := 4 + kLen + len(payload)
					writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeSet, totalLen)
					var lenBytes [4]byte
					binary.BigEndian.PutUint32(lenBytes[:], uint32(kLen))
					writeBuf = append(writeBuf, lenBytes[:]...)
					writeBuf = append(writeBuf, key...)
					writeBuf = append(writeBuf, payload...)
				}
			}

			writeBuf = protocol.AppendHeader(writeBuf, protocol.OpCodeCommit, 0)
		}

		if _, err := conn.Write(writeBuf); err != nil {
			atomic.AddInt64(failedOps, int64(pipelineDepth*batchSize))
			return
		}

		expectedResps := pipelineDepth * (2 + batchSize)
		batchFailed := false

		for i := 0; i < expectedResps; i++ {
			if _, err := io.ReadFull(reader, headerBuf); err != nil {
				atomic.AddInt64(failedOps, int64(pipelineDepth*batchSize))
				return
			}
			status := headerBuf[0]
			length := binary.BigEndian.Uint32(headerBuf[1:])
			failedStatus := status != protocol.ResStatusOK && status != protocol.ResStatusNotFound

			var errBody string
			if length > 0 {
				if failedStatus {
					buf := make([]byte, length)
					if _, err := io.ReadFull(reader, buf); err != nil {
						atomic.AddInt64(failedOps, int64(pipelineDepth*batchSize))
						return
					}
					errBody = string(buf)
				} else if _, err := reader.Discard(int(length)); err != nil {
					atomic.AddInt64(failedOps, int64(pipelineDepth*batchSize))
					return
				}
			}

			if failedStatus {
				batchFailed = true
				logFail.Do(func() {
					log.Printf("%s first failure: status=0x%02x (%s) body=%q", phase, status, protocol.StatusName(status), errBody)
				})
			}
			if status == protocol.ResStatusNotFound {
				atomic.AddInt64(notFoundOps, 1)
			}
		}

		latency := time.Since(startBatch).Nanoseconds()
		opsInBatch := int64(pipelineDepth * batchSize)
		if batchFailed {
			atomic.AddInt64(failedOps, opsInBatch)
		} else {
			atomic.AddInt64(completedOps, opsInBatch)
			atomic.AddInt64(totalDuration, latency)
		}
	}
}

func printBenchStats(phase string, elapsed time.Duration, success, failed, notFound int64, totalLatencyNs int64) {
	tps := float64(success) / elapsed.Seconds()
	avgLatency := float64(0)

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

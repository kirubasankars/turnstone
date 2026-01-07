package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/client"
)

// DevicePayload defines the structure for our fake JSON data
type DevicePayload struct {
	DeviceID      string  `json:"device_id"`
	DeviceType    string  `json:"device_type"`
	FriendlyName  string  `json:"friendly_name"`
	LastSeen      int64   `json:"last_seen"`
	AppsInstalled int     `json:"number_of_apps_installed"`
	GPS           GPSInfo `json:"gps_coords"`
	Padding       string  `json:"padding,omitempty"` // Used to bloat size
}

type GPSInfo struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

func main() {
	home := flag.String("home", ".", "Home directory for certs")
	workers := flag.Int("workers", 10, "Number of concurrent workers")
	duration := flag.Duration("duration", 10*time.Second, "Test duration")
	addr := flag.String("target", "localhost:6379", "Target address")
	batchSize := flag.Int("batch", 1, "Number of operations per transaction (Batch Size)")
	keyCount := flag.Int("keys", 1_000_000, "Total number of unique device keys (Cardinality)")
	paddingSize := flag.Int("padding", 0, "Additional bytes of padding per value to stress memory/throughput")
	flag.Parse()

	// Setup Cert Paths
	caFile := filepath.Join(*home, "certs/ca.crt")
	certFile := filepath.Join(*home, "certs/client.crt")
	keyFile := filepath.Join(*home, "certs/client.key")

	// Pre-calculate padding string once
	padStr := ""
	if *paddingSize > 0 {
		padStr = strings.Repeat("x", *paddingSize)
	}

	fmt.Printf("Starting Load Generator\n")
	fmt.Printf("Target: %s (Partition 1)\n", *addr)
	fmt.Printf("Duration: %v\n", *duration)
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Batch Size: %d\n", *batchSize)
	fmt.Printf("Device Pool: %d keys\n", *keyCount)
	if *paddingSize > 0 {
		fmt.Printf("Payload Padding: %d bytes\n", *paddingSize)
	}
	fmt.Println("")

	var ops int64
	var errCount int64
	var conflictCount int64

	// Error Tracking
	errorMap := make(map[string]int)
	var errMu sync.Mutex

	recordError := func(err error) {
		atomic.AddInt64(&errCount, 1)
		msg := err.Error()
		errMu.Lock()
		errorMap[msg]++
		errMu.Unlock()
	}

	// Device types list
	deviceTypes := []string{"android", "windows", "mac", "apple", "apple tv"}

	var wg sync.WaitGroup
	start := time.Now()
	done := make(chan struct{})

	// Timer
	go func() {
		time.Sleep(*duration)
		close(done)
	}()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create Client
			cli, err := client.NewMTLSClientHelper(*addr, caFile, certFile, keyFile, nil)
			if err != nil {
				recordError(fmt.Errorf("connect failed: %v", err))
				return
			}
			defer cli.Close()

			// Initialize random source per worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			// Select Partition
			if err := cli.Select("1"); err != nil {
				recordError(fmt.Errorf("select failed: %v", err))
				return
			}

			// Pre-allocate batch buffer
			type batchOp struct {
				key string
				val []byte
			}
			currentBatch := make([]batchOp, 0, *batchSize)

			for {
				select {
				case <-done:
					return
				default:
				}

				// 1. Generate Batch Data
				currentBatch = currentBatch[:0]
				for b := 0; b < *batchSize; b++ {
					// Pick random device from the configurable pool
					devID := rng.Intn(*keyCount)
					key := fmt.Sprintf("device-%d", devID)

					// Deterministically pick type based on ID
					typeIdx := devID % len(deviceTypes)
					dType := deviceTypes[typeIdx]

					// Create fake device data
					data := DevicePayload{
						DeviceID:      key,
						DeviceType:    dType,
						FriendlyName:  fmt.Sprintf("%s-unit-%d", dType, devID),
						LastSeen:      time.Now().Unix(),
						AppsInstalled: 42 + (devID % 20),
						GPS: GPSInfo{
							Lat: 37.7749 + (rng.Float64() * 0.01),
							Lon: -122.4194 + (rng.Float64() * 0.01),
						},
						Padding: padStr,
					}

					val, err := json.Marshal(data)
					if err != nil {
						recordError(fmt.Errorf("json marshal failed: %v", err))
						continue
					}
					currentBatch = append(currentBatch, batchOp{key: key, val: val})
				}

				if len(currentBatch) == 0 {
					continue
				}

				// 2. Execute Transaction with Retry
				maxRetries := 5
				for attempt := 0; attempt < maxRetries; attempt++ {
					if err := cli.Begin(); err != nil {
						recordError(err)
						break // Fatal error for this op (likely connection issue)
					}

					// Queue all sets in batch
					txErr := func() error {
						for _, op := range currentBatch {
							if err := cli.Set(op.key, op.val); err != nil {
								return err
							}
						}
						return nil
					}()

					if txErr != nil {
						cli.Abort()
						recordError(txErr)
						break
					}

					// Commit
					err := cli.Commit()
					if err == nil {
						// Success
						atomic.AddInt64(&ops, int64(len(currentBatch)))
						break
					}

					// Check for Conflict
					if errors.Is(err, client.ErrTxConflict) {
						atomic.AddInt64(&conflictCount, 1)
						// Exponential backoff + Jitter
						backoff := time.Duration(1<<attempt) * time.Millisecond
						jitter := time.Duration(rng.Intn(10)) * time.Millisecond
						time.Sleep(backoff + jitter)
						continue // Retry loop
					}

					// Other errors
					recordError(err)
					break
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Println("\nStopping workers...")
	fmt.Println("--- Results ---")
	fmt.Printf("Total Operations: %d\n", ops)
	fmt.Printf("Total Errors:     %d\n", errCount)
	fmt.Printf("Total Conflicts:  %d (Retried)\n", conflictCount)
	fmt.Printf("Throughput:       %.2f ops/sec\n", float64(ops)/elapsed.Seconds())

	if len(errorMap) > 0 {
		fmt.Println("\n--- Error Breakdown ---")
		type errStat struct {
			msg   string
			count int
		}
		var stats []errStat
		for msg, count := range errorMap {
			stats = append(stats, errStat{msg, count})
		}
		sort.Slice(stats, func(i, j int) bool {
			return stats[i].count > stats[j].count
		})

		for _, stat := range stats {
			fmt.Printf("[%4d] %s\n", stat.count, stat.msg)
		}
	}
}

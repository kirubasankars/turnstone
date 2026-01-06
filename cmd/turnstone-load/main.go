package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
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
	flag.Parse()

	// Setup Cert Paths
	caFile := filepath.Join(*home, "certs/ca.crt")
	certFile := filepath.Join(*home, "certs/client.crt")
	keyFile := filepath.Join(*home, "certs/client.key")

	fmt.Printf("Starting Load Generator\n")
	fmt.Printf("Target: %s (Partition 1)\n", *addr)
	fmt.Printf("Duration: %v\n", *duration)
	fmt.Printf("Workers: %d\n", *workers)
	fmt.Printf("Device Pool: 1,000,000\n\n")

	var ops int64
	var errCount int64

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

			for {
				select {
				case <-done:
					return
				default:
				}

				// Pick random device from 1M pool
				devID := rng.Intn(1_000_000)
				key := fmt.Sprintf("device-%d", devID)

				// Deterministically pick type based on ID so a specific device doesn't change types randomly
				typeIdx := devID % len(deviceTypes)
				dType := deviceTypes[typeIdx]

				// Create fake device data
				data := DevicePayload{
					DeviceID:      key,
					DeviceType:    dType,
					FriendlyName:  fmt.Sprintf("%s-unit-%d", dType, devID),
					LastSeen:      time.Now().Unix(),
					AppsInstalled: 42 + (devID % 20), // Deterministic variation based on ID
					GPS: GPSInfo{
						Lat: 37.7749 + (rng.Float64() * 0.01), // Small random jitter
						Lon: -122.4194 + (rng.Float64() * 0.01),
					},
				}

				val, err := json.Marshal(data)
				if err != nil {
					recordError(fmt.Errorf("json marshal failed: %v", err))
					continue
				}

				// Operation: Set
				if err := cli.Begin(); err != nil {
					recordError(err)
					continue
				}
				if err := cli.Set(key, val); err != nil {
					// If Set fails, we must Abort (or close), but usually we just count it
					cli.Abort()
					recordError(err)
					continue
				}
				if err := cli.Commit(); err != nil {
					recordError(err)
					continue
				}

				atomic.AddInt64(&ops, 1)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Println("\nStopping workers...")
	fmt.Println("--- Results ---")
	fmt.Printf("Total Operations: %d\n", ops)
	fmt.Printf("Total Errors:     %d\n", errCount)
	fmt.Printf("Throughput:       %.2f ops/sec\n", float64(ops)/elapsed.Seconds())

	if len(errorMap) > 0 {
		fmt.Println("\n--- Error Breakdown ---")
		// Sort errors by count for readability
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

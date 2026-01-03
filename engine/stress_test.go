package engine

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turnstone/protocol"
)

// --- 5. Stress & Benchmarks ---

func TestConcurrentWrites(t *testing.T) {
	db, _ := setupDB(t)
	defer db.Close()

	var wg sync.WaitGroup
	workers := 10
	itemsPerWorker := 100

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				key := fmt.Sprintf("w%d-k%d", id, i)
				val := []byte(fmt.Sprintf("val-%d-%d", id, i))
				tx := db.BeginTx()
				tx.Put(key, val)
				if err := tx.Commit(); err != nil {
					t.Errorf("Concurrent write failed: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()

	count, _ := db.KeyCount()
	expected := int64(workers * itemsPerWorker)
	if count != expected {
		t.Errorf("Concurrent write count mismatch. Expected %d, got %d", expected, count)
	}
}

func TestConcurrentIncrement(t *testing.T) {
	db, _ := setupDB(t)
	defer db.Close()

	key := "counter"
	// Initialize counter to 0
	txInit := db.BeginTx()
	txInit.Put(key, []byte("0"))
	if err := txInit.Commit(); err != nil {
		t.Fatal(err)
	}

	workers := 3
	increments := 100
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			// Use local RNG for backoff to avoid global mutex contention
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			for j := 0; j < increments; j++ {
				// Retry loop for Optimistic Locking (CAS)
				for {
					tx := db.BeginTx()

					// tx.Get() caches the version, enabling "No Blind Write" protection.
					// Note: tx.Get is defined in test_utils.go for package engine testing
					valBytes, err := tx.Get(key)
					if err != nil {
						tx.Abort()
						t.Errorf("Unexpected Get error: %v", err)
						return
					}

					val, _ := strconv.Atoi(string(valBytes))
					newVal := val + 1

					tx.Put(key, []byte(strconv.Itoa(newVal)))

					if err := tx.Commit(); err == nil {
						break // Success
					} else if err == ErrVersionConflict {
						// Randomized jitter to break livelock
						// Sleep between 1-10ms
						time.Sleep(time.Duration(rng.Intn(10)+1) * time.Millisecond)
						continue // Retry
					} else {
						t.Errorf("Commit failed: %v", err)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final result
	finalBytes, err := db.Get(key, db.CurrentTxID())
	if err != nil {
		t.Fatalf("Failed to read final value: %v", err)
	}
	finalVal, _ := strconv.Atoi(string(finalBytes))
	expected := workers * increments

	if finalVal != expected {
		t.Errorf("Race condition detected. Expected %d, got %d", expected, finalVal)
	}
}

func TestStress5Min(t *testing.T) {
	return
	// This test runs for 5 minutes to detect memory leaks, race conditions,
	// or degradation over time.
	// Run with: go test -v -run=TestStress5Min -timeout=6m

	if testing.Short() {
		t.Skip("Skipping 5-minute stress test in short mode")
	}

	db, _ := setupDB(t)
	defer db.Close()

	duration := 5 * time.Minute
	workers := 8
	keySpace := 100000 // Circular key space to force overwrites/GC

	fmt.Printf("Starting 5-minute stress test with %d workers on %d keys...\n", workers, keySpace)

	start := time.Now()
	var ops uint64
	var errors uint64

	doneCh := make(chan struct{})
	timer := time.AfterFunc(duration, func() {
		close(doneCh)
	})
	defer timer.Stop()

	var wg sync.WaitGroup
	wg.Add(workers)

	// Status reporter
	stopStats := make(chan struct{})
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopStats:
				return
			case <-ticker.C:
				currentOps := atomic.LoadUint64(&ops)
				currentErrs := atomic.LoadUint64(&errors)
				elapsed := time.Since(start).Seconds()
				fmt.Printf("[Stress] T+%.0fs | Ops: %d | rate: %.0f/s | Errors: %d\n",
					elapsed, currentOps, float64(currentOps)/elapsed, currentErrs)
			}
		}
	}()

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			// Local random to avoid lock contention on global rand
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

			val := make([]byte, 128) // 128B payload

			for {
				select {
				case <-doneCh:
					return
				default:
					k := rng.Intn(keySpace)
					key := fmt.Sprintf("k%d", k)

					// 50% Writes, 50% Reads
					if rng.Float32() < 0.5 {
						// Write
						rng.Read(val) // Randomize content

						// Write with CAS loop to handle strict SI
						for {
							tx := db.BeginTx()

							// 1. Read to get version (and cache it)
							_, err := tx.Get(key)
							if err != nil && err != protocol.ErrKeyNotFound {
								tx.Abort()
								atomic.AddUint64(&errors, 1)
								break // abort this op
							}

							// 2. Put (uses cached version)
							tx.Put(key, val)

							// 3. Commit
							if err := tx.Commit(); err == nil {
								break // Success
							} else if err == ErrVersionConflict {
								// Simple jitter backoff
								time.Sleep(time.Duration(rng.Intn(5)+1) * time.Millisecond)
								continue // Retry
							} else {
								atomic.AddUint64(&errors, 1)
								break // Real error
							}
						}

					} else {
						// Read
						_, err := db.Get(key, db.CurrentTxID())
						if err != nil && err != protocol.ErrKeyNotFound {
							atomic.AddUint64(&errors, 1)
							t.Errorf("Read failed: %v", err)
						}
					}
					atomic.AddUint64(&ops, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	close(stopStats)
	elapsed := time.Since(start)

	totalOps := atomic.LoadUint64(&ops)
	totalErrors := atomic.LoadUint64(&errors)

	fmt.Printf("--- Stress Test Results ---\n")
	fmt.Printf("Duration: %v\n", elapsed)
	fmt.Printf("Total Ops: %d\n", totalOps)
	fmt.Printf("Ops/Sec: %.2f\n", float64(totalOps)/elapsed.Seconds())
	fmt.Printf("Errors: %d\n", totalErrors)
}

func BenchmarkPut(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	val := []byte("benchmark_value_payload_1234567890")
	b.ResetTimer()

	// Use modulo to recycle keys. This limits the total "live" dataset size
	// to ~100k keys, ensuring that we create "garbage" (old versions)
	// that the GC can reclaim. Without this, the disk fills up infinitely.
	for i := 0; i < b.N; i++ {
		tx := db.BeginTx()
		tx.Put(fmt.Sprintf("k%d", i%100000), val)
		tx.Commit()
	}
}

func BenchmarkGet(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Pre-fill
	val := []byte("benchmark_value")
	key := "bench_key"
	tx := db.BeginTx()
	tx.Put(key, val)
	tx.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(key, 100) // Read at version 100
	}
}

func BenchmarkDelete(b *testing.B) {
	db, _ := setupDB(b)
	defer db.Close()

	// Put is fast, Delete is just a Put with IsDelete=true
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := db.BeginTx()
		tx.Delete(fmt.Sprintf("k%d", i))
		tx.Commit()
	}
}

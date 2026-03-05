package stonedb

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestCorrectness_ModelBased (Differential Testing)
// We execute random operations against StoneDB and a simple Go Map (the "Reference Model").
// At the end, the DB must match the Map exactly.
func TestCorrectness_ModelBased(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		// Force frequent internal churn to stress flushing/compaction
		MaxWALSize:           1024 * 10,
		CompactionMinGarbage: 1024,
		CompactionInterval:   10 * time.Millisecond,
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The Reference Model
	model := make(map[string]string)

	// Ops config
	numOps := 5000
	numKeys := 100

	// Deterministic Randomness
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("key-%d", rng.Intn(numKeys))

		// 80% Writes, 20% Deletes
		if rng.Float32() < 0.8 {
			// PUT
			val := fmt.Sprintf("val-%d-%d", i, rng.Int())

			// Update DB
			tx := db.NewTransaction(true)
			if err := tx.Put([]byte(key), []byte(val)); err != nil {
				t.Fatalf("Op %d: Put failed: %v", i, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Op %d: Commit failed: %v", i, err)
			}

			// Update Model
			model[key] = val
		} else {
			// DELETE
			// Update DB
			tx := db.NewTransaction(true)
			if err := tx.Delete([]byte(key)); err != nil {
				t.Fatalf("Op %d: Delete failed: %v", i, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Op %d: Commit failed: %v", i, err)
			}

			// Update Model
			delete(model, key)
		}
	}

	// VERIFICATION
	// Iterate through every key in the model and ensure DB matches.
	for key, expectedVal := range model {
		tx := db.NewTransaction(false)
		val, err := tx.Get([]byte(key))
		tx.Discard()

		if err != nil {
			t.Errorf("Verification Failed: Key %s exists in Model but not DB. Err: %v", key, err)
			continue
		}
		if string(val) != expectedVal {
			t.Errorf("Verification Failed: Key %s mismatch. Model=%s, DB=%s", key, expectedVal, val)
		}
	}

	// Verify count
	count, _ := db.KeyCount()
	if count != int64(len(model)) {
		t.Errorf("Key Count mismatch. Model=%d, DB=%d", len(model), count)
	}
}

// TestCorrectness_BankTransfers (Invariant Testing)
// Simulates a banking system.
// Total money in the system must remain constant (Invariant).
// This tests Atomicity (money isn't lost/created) and Isolation (locks work).
func TestCorrectness_BankTransfers(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{MaxWALSize: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		numAccounts        = 10
		initialBalance     = 1000
		numWorkers         = 5
		transfersPerWorker = 100
	)

	// 1. Initialize Accounts
	{
		tx := db.NewTransaction(true)
		for i := 0; i < numAccounts; i++ {
			key := accountKey(i)
			tx.Put(key, intToBytes(initialBalance))
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate expected Total
	expectedTotal := numAccounts * initialBalance

	// 2. Run Concurrent Transfers
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))

			for i := 0; i < transfersPerWorker; i++ {
				from := rng.Intn(numAccounts)
				to := rng.Intn(numAccounts)
				if from == to {
					continue
				}
				amount := 10 // Fixed transfer amount

				// Execute Transfer Transaction
				// Retry loop for conflict resolution
				for {
					tx := db.NewTransaction(true)

					// Read From
					bFromBytes, _ := tx.Get(accountKey(from))
					bFrom := bytesToInt(bFromBytes)

					// Read To
					bToBytes, _ := tx.Get(accountKey(to))
					bTo := bytesToInt(bToBytes)

					// Check Funds
					if bFrom < amount {
						tx.Discard()
						break // Not enough funds, skip this transfer
					}

					// Update
					tx.Put(accountKey(from), intToBytes(bFrom-amount))
					tx.Put(accountKey(to), intToBytes(bTo+amount))

					// Commit
					if err := tx.Commit(); err == nil {
						break // Success
					} else if err != ErrWriteConflict {
						// Real error?
						fmt.Printf("Worker %d error: %v\n", workerID, err)
						break
					}
					// If ErrWriteConflict, loop and retry
				}
			}
		}(w)
	}

	wg.Wait()

	// 3. Verify Invariant: Sum of all accounts == expectedTotal
	total := 0
	tx := db.NewTransaction(false)
	for i := 0; i < numAccounts; i++ {
		val, err := tx.Get(accountKey(i))
		if err != nil {
			t.Fatalf("Account %d missing: %v", i, err)
		}
		total += bytesToInt(val)
	}
	tx.Discard()

	if total != expectedTotal {
		t.Fatalf("Invariant Violated! Money lost or created. Expected %d, Got %d", expectedTotal, total)
	}
}

// --- Helpers ---

func accountKey(id int) []byte {
	return []byte(fmt.Sprintf("acc-%04d", id))
}

func intToBytes(n int) []byte {
	return []byte(strconv.Itoa(n))
}

func bytesToInt(b []byte) int {
	n, _ := strconv.Atoi(string(b))
	return n
}

// TestCorrectness_BankTransfers_HeavyStress performs the bank transfer test
// with aggressive WAL rotation and compaction settings to verify data integrity
// under system stress.
func TestCorrectness_BankTransfers_HeavyStress(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		// Very aggressive settings to force frequent rotation and compaction
		// while transactions are in flight.
		MaxWALSize:           4096,                 // 4KB WAL (Force frequent rotation)
		CompactionMinGarbage: 1024,                 // 1KB Garbage (Force frequent compaction candidates)
		CompactionInterval:   5 * time.Millisecond, // Check for compaction very frequently
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const (
		numAccounts        = 20
		initialBalance     = 10000
		numWorkers         = 10
		transfersPerWorker = 500
	)

	// 1. Initialize Accounts
	{
		tx := db.NewTransaction(true)
		for i := 0; i < numAccounts; i++ {
			key := accountKey(i)
			tx.Put(key, intToBytes(initialBalance))
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Calculate expected Total
	expectedTotal := numAccounts * initialBalance

	// 2. Run Concurrent Transfers
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			// Use different seed for each worker
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for i := 0; i < transfersPerWorker; i++ {
				from := rng.Intn(numAccounts)
				to := rng.Intn(numAccounts)
				if from == to {
					continue
				}
				amount := rng.Intn(50) + 1 // Random amount 1..50

				// Execute Transfer Transaction with Retry
				for {
					tx := db.NewTransaction(true)

					// Read From
					bFromBytes, _ := tx.Get(accountKey(from))
					bFrom := bytesToInt(bFromBytes)

					// Read To
					bToBytes, _ := tx.Get(accountKey(to))
					bTo := bytesToInt(bToBytes)

					// Check Funds
					if bFrom < amount {
						tx.Discard()
						break // Not enough funds, skip this transfer attempt
					}

					// Update
					tx.Put(accountKey(from), intToBytes(bFrom-amount))
					tx.Put(accountKey(to), intToBytes(bTo+amount))

					// Commit
					if err := tx.Commit(); err == nil {
						break // Success
					} else if err != ErrWriteConflict {
						// Real error?
						t.Errorf("Worker %d error: %v", workerID, err)
						break
					}
					// If ErrWriteConflict, loop and retry
				}
			}
		}(w)
	}

	wg.Wait()

	// 3. Verify Invariant: Sum of all accounts == expectedTotal
	total := 0
	tx := db.NewTransaction(false)
	for i := 0; i < numAccounts; i++ {
		val, err := tx.Get(accountKey(i))
		if err != nil {
			t.Fatalf("Account %d missing: %v", i, err)
		}
		total += bytesToInt(val)
	}
	tx.Discard()

	if total != expectedTotal {
		t.Fatalf("Invariant Violated under stress! Money lost or created. Expected %d, Got %d", expectedTotal, total)
	}
}

// TestCorrectness_ConcurrentIncrement verifies that concurrent Read-Modify-Write operations
// on a single key result in the correct final state, provided conflicts are retried.
func TestCorrectness_ConcurrentIncrement(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("counter")

	// 1. Initialize
	{
		tx := db.NewTransaction(true)
		tx.Put(key, []byte("0"))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	const (
		goroutines = 2
		increments = 50
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < increments; j++ {
				// Retry loop for OCC
				for {
					tx := db.NewTransaction(true)
					valBytes, err := tx.Get(key)
					if err != nil {
						tx.Discard()
						t.Errorf("Get failed: %v", err)
						return
					}

					val, _ := strconv.Atoi(string(valBytes))
					val++

					tx.Put(key, []byte(strconv.Itoa(val)))

					err = tx.Commit()
					if err == nil {
						break // Success, move to next increment
					}
					if err != ErrWriteConflict {
						t.Errorf("Unexpected commit error: %v", err)
						return
					}
					// If conflict, retry
				}
			}
		}()
	}

	wg.Wait()

	// Verify
	tx := db.NewTransaction(false)
	valBytes, err := tx.Get(key)
	if err != nil {
		t.Fatalf("Final get failed: %v", err)
	}
	tx.Discard()

	val, _ := strconv.Atoi(string(valBytes))
	expected := goroutines * increments
	if val != expected {
		t.Errorf("Counter mismatch. Expected %d, got %d", expected, val)
	}
}

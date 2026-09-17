// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
)

// BenchmarkDB_Insert measures performance of inserting NEW unique keys.
func BenchmarkDB_Insert(b *testing.B) {
	dir := b.TempDir()
	opts := Options{}
	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("benchmark_value_data_1234567890")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Unique key for every iteration
		key := []byte(fmt.Sprintf("insert-key-%d", i))
		tx := db.NewTransaction(true)
		if err := tx.Put(key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDB_Update measures performance of updating EXISTING keys.
// This stresses the MVCC mechanism and Garbage Collection more than pure inserts.
func BenchmarkDB_Update(b *testing.B) {
	dir := b.TempDir()
	opts := Options{}
	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("benchmark_value_data_1234567890")
	numKeys := 10000

	// 1. Pre-populate the DB
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("update-key-%d", i))
		tx := db.NewTransaction(true)
		tx.Put(key, val)
		tx.Commit()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Randomly select a key from the existing set to update
		k := rand.Intn(numKeys)
		key := []byte(fmt.Sprintf("update-key-%d", k))

		tx := db.NewTransaction(true)
		if err := tx.Put(key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDB_Read measures random read performance on existing keys.
func BenchmarkDB_Read(b *testing.B) {
	dir := b.TempDir()
	opts := Options{}
	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 10000
	val := []byte("benchmark_value_data_1234567890")

	// 1. Pre-populate (Batching for speed)
	batchSize := 100
	for i := 0; i < numKeys; i += batchSize {
		tx := db.NewTransaction(true)
		for j := 0; j < batchSize; j++ {
			if i+j >= numKeys {
				break
			}
			key := []byte(fmt.Sprintf("read-key-%d", i+j))
			tx.Put(key, val)
		}
		tx.Commit()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := rand.Intn(numKeys)
		key := []byte(fmt.Sprintf("read-key-%d", k))

		tx := db.NewTransaction(false)
		if _, err := tx.Get(key); err != nil {
			b.Fatal(err)
		}
		tx.Discard()
	}
}

// BenchmarkDB_Mixed measures a 50/50 mix of Reads and Updates.
func BenchmarkDB_Mixed(b *testing.B) {
	dir := b.TempDir()
	opts := Options{}
	db, err := Open(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 10000
	val := []byte("benchmark_value_data_1234567890")

	// Pre-populate
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		tx := db.NewTransaction(true)
		tx.Put(key, val)
		tx.Commit()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := rand.Intn(numKeys)
		key := []byte(fmt.Sprintf("key-%d", k))

		if i%2 == 0 {
			// Read
			tx := db.NewTransaction(false)
			if _, err := tx.Get(key); err != nil {
				b.Fatal(err)
			}
			tx.Discard()
		} else {
			// Update
			tx := db.NewTransaction(true)
			if err := tx.Put(key, val); err != nil {
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkDB_InsertParallel(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("benchmark_value_data_1234567890")
	var seq uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			key := []byte(fmt.Sprintf("p-ins-%d", n))
			tx := db.NewTransaction(true)
			if err := tx.Put(key, val); err != nil {
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDB_ReadParallel(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 10000
	val := []byte("benchmark_value_data_1234567890")
	tx := db.NewTransaction(true)
	for i := 0; i < numKeys; i++ {
		if err := tx.Put([]byte(fmt.Sprintf("pread-%d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("pread-%d", i%numKeys))
			rtx := db.NewTransaction(false)
			if _, err := rtx.Get(key); err != nil {
				b.Fatal(err)
			}
			rtx.Discard()
			i++
		}
	})
}

func BenchmarkDB_UpdateParallel(b *testing.B) {
	dir := b.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numKeys := 10000
	val := []byte("benchmark_value_data_1234567890")
	seed := db.NewTransaction(true)
	for i := 0; i < numKeys; i++ {
		if err := seed.Put([]byte(fmt.Sprintf("pupd-%d", i)), val); err != nil {
			b.Fatal(err)
		}
	}
	if err := seed.Commit(); err != nil {
		b.Fatal(err)
	}

	var seq uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddUint64(&seq, 1)
			key := []byte(fmt.Sprintf("pupd-%d", n%uint64(numKeys)))
			tx := db.NewTransaction(true)
			if err := tx.Put(key, val); err != nil {
				// First-writer-wins: retry once on conflict.
				tx.Discard()
				tx = db.NewTransaction(true)
				if err := tx.Put(key, val); err != nil {
					tx.Discard()
					continue
				}
			}
			if err := tx.Commit(); err != nil {
				tx.Discard()
			}
		}
	})
}

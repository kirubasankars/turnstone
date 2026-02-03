// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package stonedb

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkDB_Insert measures performance of inserting NEW unique keys.
func BenchmarkDB_Insert(b *testing.B) {
	dir := b.TempDir()
	opts := Options{MaxWALSize: 64 * 1024 * 1024}
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
	opts := Options{MaxWALSize: 64 * 1024 * 1024}
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
	opts := Options{MaxWALSize: 64 * 1024 * 1024}
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
	opts := Options{MaxWALSize: 64 * 1024 * 1024}
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

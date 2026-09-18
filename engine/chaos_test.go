// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func liveMap(t *testing.T, db *DB) map[string]string {
	t.Helper()
	out := make(map[string]string)
	tx := db.NewTransaction(false)
	defer tx.Discard()
	db.index.ForEachKey(func(key []byte, _ []indexVersion) {
		val, err := tx.Get(key)
		if err == ErrKeyNotFound {
			return
		}
		if err != nil {
			t.Fatalf("liveMap get %q: %v", key, err)
		}
		out[string(key)] = string(val)
	})
	return out
}

func requireMapsEqual(t *testing.T, want, got map[string]string, ctx string) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: key count want %d got %d", ctx, len(want), len(got))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: key %s want %q got %q", ctx, k, v, got[k])
		}
	}
}

func drainDurable(t *testing.T, src, dst *DB) {
	t.Helper()
	off := dst.LastLogOffset()
	for {
		data, next, err := src.ReadLogRange(off, 1<<20)
		if err != nil {
			t.Fatalf("ReadLogRange(%d): %v", off, err)
		}
		if len(data) == 0 {
			return
		}
		if _, err := dst.ApplyLogRange(data); err != nil {
			t.Fatalf("ApplyLogRange at %d: %v", off, err)
		}
		if next <= off {
			t.Fatalf("drain did not advance: off=%d next=%d", off, next)
		}
		off = next
	}
}

func durableWAL(t *testing.T, db *DB) []byte {
	t.Helper()
	data, _, err := db.ReadLogRange(0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestChaos_ConcurrentWriteReadDisjoint(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	const (
		writers = 8
		keysPer = 16
		rounds  = 12
		readers = 4
	)
	var writersDone sync.WaitGroup
	var stop int32
	var readerWG sync.WaitGroup

	writersDone.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer writersDone.Done()
			for r := 0; r < rounds; r++ {
				tx := db.NewTransaction(true)
				for k := 0; k < keysPer; k++ {
					key := []byte(fmt.Sprintf("w%d-k%d", id, k))
					if err := tx.Put(key, []byte(fmt.Sprintf("r%d", r))); err != nil {
						tx.Discard()
						t.Errorf("put: %v", err)
						return
					}
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}

	readerWG.Add(readers)
	for r := 0; r < readers; r++ {
		go func(id int) {
			defer readerWG.Done()
			rng := rand.New(rand.NewSource(int64(id + 11)))
			for atomic.LoadInt32(&stop) == 0 {
				wid := rng.Intn(writers)
				kid := rng.Intn(keysPer)
				key := []byte(fmt.Sprintf("w%d-k%d", wid, kid))
				tx := db.NewTransaction(false)
				val, err := tx.Get(key)
				tx.Discard()
				if err == ErrKeyNotFound {
					continue
				}
				if err != nil {
					t.Errorf("read: %v", err)
					return
				}
				var n int
				if _, scanErr := fmt.Sscanf(string(val), "r%d", &n); scanErr != nil || n < 0 || n >= rounds {
					t.Errorf("reader saw impossible value %q", val)
					return
				}
			}
		}(r)
	}

	writersDone.Wait()
	atomic.StoreInt32(&stop, 1)
	readerWG.Wait()

	want := make(map[string]string)
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPer; k++ {
			want[fmt.Sprintf("w%d-k%d", w, k)] = fmt.Sprintf("r%d", rounds-1)
		}
	}
	got := liveMap(t, db)
	requireMapsEqual(t, want, got, "before reopen")

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	requireMapsEqual(t, want, liveMap(t, db2), "after reopen")
}

// TestChaos_DurableSnapshotsAreRecovered is the crash-safety contract for
// concurrent writers: every ReadLogRange snapshot is a prefix of the WAL
// recovered after Close+Open.
func TestChaos_DurableSnapshotsAreRecovered(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	const writers = 6
	const n = 40
	var snaps [][]byte
	var snapMu sync.Mutex
	var stop int32
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			data := durableWAL(t, db)
			snapMu.Lock()
			snaps = append(snaps, append([]byte(nil), data...))
			snapMu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	var wwg sync.WaitGroup
	wwg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wwg.Done()
			for i := 0; i < n; i++ {
				tx := db.NewTransaction(true)
				key := []byte(fmt.Sprintf("c%d-%d", id, i))
				if err := tx.Put(key, []byte("v")); err != nil {
					tx.Discard()
					t.Errorf("put: %v", err)
					return
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}
	wwg.Wait()
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	final := durableWAL(t, db)
	snapMu.Lock()
	for i, s := range snaps {
		if !bytes.HasPrefix(final, s) {
			t.Fatalf("in-memory durable snapshot %d (%d bytes) is not a prefix of the final durable WAL (%d bytes)", i, len(s), len(final))
		}
	}
	snapMu.Unlock()

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	recovered := durableWAL(t, db2)
	if !bytes.Equal(recovered, final) {
		t.Fatalf("recovered WAL %d bytes, want final durable %d bytes", len(recovered), len(final))
	}
	snapMu.Lock()
	defer snapMu.Unlock()
	for i, s := range snaps {
		if !bytes.HasPrefix(recovered, s) {
			t.Fatalf("snapshot %d missing from recovered WAL", i)
		}
	}
}

func TestChaos_ReplicationCatchupUnderLoad(t *testing.T) {
	primary, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	follower, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	const writers = 5
	const n = 30
	var stop int32
	var catchWG sync.WaitGroup
	catchWG.Add(1)
	go func() {
		defer catchWG.Done()
		for {
			drainDurable(t, primary, follower)
			if atomic.LoadInt32(&stop) == 1 && follower.LastLogOffset() >= primary.DurableOffset() {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	var wwg sync.WaitGroup
	wwg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wwg.Done()
			for i := 0; i < n; i++ {
				tx := primary.NewTransaction(true)
				if err := tx.Put([]byte(fmt.Sprintf("r%d-%d", id, i)), []byte("x")); err != nil {
					tx.Discard()
					t.Errorf("put: %v", err)
					return
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}
	wwg.Wait()
	atomic.StoreInt32(&stop, 1)
	catchWG.Wait()
	drainDurable(t, primary, follower)

	if follower.DurableOffset() != primary.DurableOffset() {
		t.Fatalf("follower durable %d primary durable %d", follower.DurableOffset(), primary.DurableOffset())
	}
	requireMapsEqual(t, liveMap(t, primary), liveMap(t, follower), "replica catch-up")
}

func TestChaos_PhysicalBackupUnderWrites(t *testing.T) {
	src, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	const writers = 4
	const n = 25
	var wwg sync.WaitGroup
	wwg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wwg.Done()
			for i := 0; i < n; i++ {
				tx := src.NewTransaction(true)
				if err := tx.Put([]byte(fmt.Sprintf("b%d-%d", id, i)), []byte("v")); err != nil {
					tx.Discard()
					t.Errorf("put: %v", err)
					return
				}
				if err := tx.Commit(); err != nil {
					t.Errorf("commit: %v", err)
					return
				}
			}
		}(w)
	}

	// Mid-load durable snapshot (the backup cut).
	time.Sleep(5 * time.Millisecond)
	cut := durableWAL(t, src)
	wwg.Wait()

	restored, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if len(cut) > 0 {
		if _, err := restored.ApplyLogRange(cut); err != nil {
			t.Fatal(err)
		}
	}

	// Every restored key must still exist on the source (writes only insert).
	rst := liveMap(t, restored)
	srcMap := liveMap(t, src)
	for k, v := range rst {
		if srcMap[k] != v {
			t.Fatalf("backup key %s: restored %q source %q", k, v, srcMap[k])
		}
	}

	// Applying the same cut to a second engine must match the first restore.
	dup, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer dup.Close()
	if len(cut) > 0 {
		if _, err := dup.ApplyLogRange(cut); err != nil {
			t.Fatal(err)
		}
	}
	requireMapsEqual(t, rst, liveMap(t, dup), "backup apply is deterministic")
}

func TestChaos_CloseDuringWritesThenRecover(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				if atomic.LoadInt32(&db.closed) == 1 {
					return
				}
				tx := db.NewTransaction(true)
				err := tx.Put([]byte(fmt.Sprintf("x%d-%d", id, i)), []byte("v"))
				if err != nil {
					tx.Discard()
					if atomic.LoadInt32(&db.closed) == 1 {
						return
					}
					continue
				}
				if err := tx.Commit(); err != nil {
					return
				}
				i++
			}
		}(w)
	}

	time.Sleep(20 * time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writers did not exit after Close")
	}

	db2, err := Open(dir, Options{TruncateCorruptTail: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	got := liveMap(t, db2)
	tx := db2.NewTransaction(false)
	defer tx.Discard()
	for k, v := range got {
		val, err := tx.Get([]byte(k))
		if err != nil || string(val) != v {
			t.Fatalf("reopen get %s: %v %q", k, err, val)
		}
	}
}

func TestIndex_CloseConcurrentDropXid(t *testing.T) {
	idx := NewIndex()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("k%d", id))
			for n := 0; n < 200; n++ {
				_ = idx.Put(key, indexVersion{xmin: uint64(id + 1)})
				_ = idx.DropXid(uint64(id + 1))
			}
		}(i)
	}
	time.Sleep(2 * time.Millisecond)
	if err := idx.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestChaos_MonkeyModel(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(20260918))
	model := make(map[string]string)
	const keys = 24
	const ops = 400

	reopen := func() {
		t.Helper()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		requireMapsEqual(t, model, liveMap(t, db), "monkey reopen")
	}

	for i := 0; i < ops; i++ {
		key := fmt.Sprintf("k%d", rng.Intn(keys))
		switch rng.Intn(10) {
		case 0, 1, 2, 3:
			val := fmt.Sprintf("v%d", i)
			tx := db.NewTransaction(true)
			if err := tx.Put([]byte(key), []byte(val)); err != nil {
				tx.Discard()
				t.Fatalf("op %d put: %v", i, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("op %d commit: %v", i, err)
			}
			model[key] = val
		case 4:
			tx := db.NewTransaction(true)
			if err := tx.Delete([]byte(key)); err != nil {
				tx.Discard()
				t.Fatalf("op %d del: %v", i, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("op %d del commit: %v", i, err)
			}
			delete(model, key)
		case 5:
			tx := db.NewTransaction(false)
			val, err := tx.Get([]byte(key))
			tx.Discard()
			want, ok := model[key]
			if !ok {
				if err != ErrKeyNotFound {
					t.Fatalf("op %d get missing: %v", i, err)
				}
				break
			}
			if err != nil || string(val) != want {
				t.Fatalf("op %d get %s: got %q %v want %q", i, key, val, err, want)
			}
		case 6:
			tx := db.NewTransaction(true)
			_ = tx.Put([]byte(key), []byte("aborted"))
			tx.Discard()
		case 7:
			tx1 := db.NewTransaction(true)
			tx2 := db.NewTransaction(true)
			if err := tx1.Put([]byte(key), []byte("first")); err != nil {
				tx1.Discard()
				tx2.Discard()
				break
			}
			if err := tx2.Put([]byte(key), []byte("second")); err != ErrWriteConflict {
				tx2.Discard()
				tx1.Discard()
				t.Fatalf("op %d expected conflict, got %v", i, err)
			}
			if err := tx1.Commit(); err != nil {
				t.Fatalf("op %d winner commit: %v", i, err)
			}
			tx2.Discard()
			model[key] = "first"
		case 8:
			if i > 0 && i%40 == 0 {
				reopen()
			}
		default:
			tx := db.NewTransaction(true)
			if err := tx.Put([]byte(key), []byte(fmt.Sprintf("m%d", i))); err != nil {
				tx.Discard()
				continue
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("op %d commit: %v", i, err)
			}
			model[key] = fmt.Sprintf("m%d", i)
		}
	}

	requireMapsEqual(t, model, liveMap(t, db), "monkey final")
	reopen()
	db.Close()
}

func TestChaos_SnapshotFrozenUnderWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx := db.NewTransaction(true)
	if err := tx.Put([]byte("k"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	snap := db.NewTransaction(false)
	defer snap.Discard()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			tx := db.NewTransaction(true)
			if err := tx.Put([]byte("k"), []byte(fmt.Sprintf("v%d", n+1))); err != nil {
				tx.Discard()
				return
			}
			_ = tx.Commit()
		}(i)
	}
	wg.Wait()

	val, err := snap.Get([]byte("k"))
	if err != nil || string(val) != "v0" {
		t.Fatalf("snapshot drifted: %v %q", err, val)
	}
}

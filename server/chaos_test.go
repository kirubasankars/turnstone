// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turnstone/internal/backup"
	"turnstone/protocol"
)

func setPayload(k, v string) []byte {
	key := []byte(k)
	val := []byte(v)
	pl := make([]byte, 4+len(key)+len(val))
	binary.BigEndian.PutUint32(pl[0:4], uint32(len(key)))
	copy(pl[4:], key)
	copy(pl[4+len(key):], val)
	return pl
}

func tryCommitWrite(c *testClient, k, v string) byte {
	c.Send(protocol.OpCodeBegin, nil)
	st, _ := c.Read()
	if st != protocol.ResStatusOK {
		return st
	}
	c.Send(protocol.OpCodeSet, setPayload(k, v))
	st, _ = c.Read()
	if st != protocol.ResStatusOK {
		c.Send(protocol.OpCodeAbort, nil)
		c.Read()
		return st
	}
	c.Send(protocol.OpCodeCommit, nil)
	st, _ = c.Read()
	return st
}

func TestChaos_ServerConcurrentWriteRead(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	_, addr, cancel := startServerNode(t, baseDir, "chaos_wr", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	const writers = 4
	const n = 20
	var wwg sync.WaitGroup
	wwg.Add(writers)
	var writeErr int32
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wwg.Done()
			c := connectClient(t, addr, clientTLS)
			defer c.Close()
			selectDatabase(t, c, "1")
			for i := 0; i < n; i++ {
				if st := tryCommitWrite(c, fmt.Sprintf("w%d-%d", id, i), "v"); st != protocol.ResStatusOK {
					atomic.StoreInt32(&writeErr, 1)
					t.Errorf("write status 0x%x", st)
					return
				}
			}
		}(w)
	}

	var stop int32
	var rwg sync.WaitGroup
	rwg.Add(2)
	for r := 0; r < 2; r++ {
		go func(id int) {
			defer rwg.Done()
			c := connectClient(t, addr, clientTLS)
			defer c.Close()
			selectDatabase(t, c, "1")
			rng := rand.New(rand.NewSource(int64(id + 3)))
			for atomic.LoadInt32(&stop) == 0 {
				wid := rng.Intn(writers)
				i := rng.Intn(n)
				val := readKey(t, c, fmt.Sprintf("w%d-%d", wid, i))
				if val != nil && string(val) != "v" {
					t.Errorf("reader %d unexpected value %q", id, val)
					return
				}
			}
		}(r)
	}

	wwg.Wait()
	atomic.StoreInt32(&stop, 1)
	rwg.Wait()
	if atomic.LoadInt32(&writeErr) != 0 {
		t.Fatal("writer failed")
	}

	c := connectClient(t, addr, clientTLS)
	defer c.Close()
	selectDatabase(t, c, "1")
	for w := 0; w < writers; w++ {
		for i := 0; i < n; i++ {
			if string(readKey(t, c, fmt.Sprintf("w%d-%d", w, i))) != "v" {
				t.Fatalf("missing w%d-%d", w, i)
			}
		}
	}
}

func TestChaos_ServerBackupUnderWrites(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")
	_, addr, cancel := startServerNode(t, baseDir, "chaos_bk", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	c := connectClient(t, addr, clientTLS)
	defer c.Close()
	selectDatabase(t, c, "1")
	for i := 0; i < 8; i++ {
		if st := tryCommitWrite(c, fmt.Sprintf("seed-%d", i), "s"); st != protocol.ResStatusOK {
			t.Fatalf("seed write: 0x%x", st)
		}
	}

	var stop int32
	var wg sync.WaitGroup
	wg.Add(3)
	for w := 0; w < 3; w++ {
		go func(id int) {
			defer wg.Done()
			cl := connectClient(t, addr, clientTLS)
			defer cl.Close()
			selectDatabase(t, cl, "1")
			i := 0
			for atomic.LoadInt32(&stop) == 0 {
				_ = tryCommitWrite(cl, fmt.Sprintf("bw%d-%d", id, i), "x")
				i++
			}
		}(w)
	}

	time.Sleep(40 * time.Millisecond)
	// Overlap backup with the tail of the write storm, then let the stream
	// go idle. WaitIdle must exceed the 50ms replica send tick.
	errCh := make(chan error, 1)
	fullDir := filepath.Join(baseDir, "chaos_backup")
	go func() {
		_, err := backup.RunBackup(context.Background(), backup.BackupOptions{
			Host:     addr,
			DBName:   "1",
			OutDir:   fullDir,
			Type:     backup.TypeFull,
			Compress: false,
			WaitIdle: 300 * time.Millisecond,
			TLS:      adminTLS,
		})
		errCh <- err
	}()
	time.Sleep(30 * time.Millisecond)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	if err := <-errCh; err != nil {
		t.Fatalf("backup under load: %v", err)
	}
	meta, err := backup.LoadMeta(filepath.Join(fullDir, backup.DefaultMetaFile))
	if err != nil {
		t.Fatal(err)
	}
	if meta.EndLSN == 0 {
		t.Fatal("backup end_lsn is 0 after seeded writes")
	}

	restoredHome := filepath.Join(baseDir, "chaos_restored")
	if _, err := backup.RunRestore(context.Background(), backup.RestoreOptions{
		BackupDirs: []string{fullDir},
		OutHome:    restoredHome,
		Verify:     true,
	}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	_, restAddr, cancelRest := startServerNode(t, baseDir, "chaos_restored", clientTLS)
	defer cancelRest()
	promoteNode(t, baseDir, restAddr, "1")
	rc := connectClient(t, restAddr, clientTLS)
	defer rc.Close()
	selectDatabase(t, rc, "1")

	// Restored keys must still be present on the live primary (insert-only load).
	pc := connectClient(t, addr, clientTLS)
	defer pc.Close()
	selectDatabase(t, pc, "1")
	found := 0
	for w := 0; w < 3 && found < 3; w++ {
		for i := 0; i < 8; i++ {
			k := fmt.Sprintf("bw%d-%d", w, i)
			rv := readKey(t, rc, k)
			if rv == nil {
				continue
			}
			pv := readKey(t, pc, k)
			if string(pv) != string(rv) {
				t.Fatalf("restored %s=%q primary %q", k, rv, pv)
			}
			found++
		}
	}
	if found == 0 {
		t.Fatal("restore contained no expected keys from the write load")
	}
}

func TestChaos_ServerReplicaUnderWrites(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	adminTLS := getRoleTLS(t, baseDir, "admin")
	_, primAddr, cancelP := startServerNode(t, baseDir, "chaos_prim", clientTLS)
	defer cancelP()
	promoteNode(t, baseDir, primAddr, "1")
	_, replAddr, cancelR := startServerNode(t, baseDir, "chaos_repl", clientTLS)
	defer cancelR()

	admin := connectClient(t, replAddr, adminTLS)
	defer admin.Close()
	selectDatabase(t, admin, "1")
	configureReplication(t, admin, primAddr, "1")

	const writers = 3
	const n = 15
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			c := connectClient(t, primAddr, clientTLS)
			defer c.Close()
			selectDatabase(t, c, "1")
			for i := 0; i < n; i++ {
				if st := tryCommitWrite(c, fmt.Sprintf("rw%d-%d", id, i), "y"); st != protocol.ResStatusOK {
					t.Errorf("write 0x%x", st)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	rc := connectClient(t, replAddr, clientTLS)
	defer rc.Close()
	selectDatabase(t, rc, "1")
	waitForConditionOrTimeout(t, 8*time.Second, func() bool {
		for w := 0; w < writers; w++ {
			for i := 0; i < n; i++ {
				if string(readKey(t, rc, fmt.Sprintf("rw%d-%d", w, i))) != "y" {
					return false
				}
			}
		}
		return true
	}, "replica missing concurrent writes")
}

func TestChaos_ServerMonkey(t *testing.T) {
	baseDir, clientTLS := setupSharedCertEnv(t)
	_, addr, cancel := startServerNode(t, baseDir, "chaos_monkey", clientTLS)
	defer cancel()
	promoteNode(t, baseDir, addr, "1")

	c := connectClient(t, addr, clientTLS)
	defer c.Close()
	selectDatabase(t, c, "1")

	rng := rand.New(rand.NewSource(99))
	model := make(map[string]string)
	inTx := false
	pending := make(map[string]string)
	pendingDel := make(map[string]bool)

	clearPending := func() {
		pending = make(map[string]string)
		pendingDel = make(map[string]bool)
	}

	finishAbort := func() {
		if !inTx {
			return
		}
		c.Send(protocol.OpCodeAbort, nil)
		c.Read()
		inTx = false
		clearPending()
	}

	for i := 0; i < 200; i++ {
		switch rng.Intn(9) {
		case 0:
			c.Send(protocol.OpCodePing, nil)
			st, _ := c.Read()
			if st != protocol.ResStatusOK {
				t.Fatalf("ping: 0x%x", st)
			}
		case 1, 2:
			if !inTx {
				c.Send(protocol.OpCodeBegin, nil)
				if st, _ := c.Read(); st == protocol.ResStatusOK {
					inTx = true
				}
			}
			if !inTx {
				break
			}
			k := fmt.Sprintf("m%d", rng.Intn(12))
			v := fmt.Sprintf("v%d", i)
			c.Send(protocol.OpCodeSet, setPayload(k, v))
			st, _ := c.Read()
			if st == protocol.ResStatusOK {
				pending[k] = v
				delete(pendingDel, k)
			} else if st == protocol.ResStatusTxConflict {
				inTx = false
				clearPending()
			}
		case 3:
			if !inTx {
				c.Send(protocol.OpCodeBegin, nil)
				if st, _ := c.Read(); st == protocol.ResStatusOK {
					inTx = true
				}
			}
			if !inTx {
				break
			}
			k := fmt.Sprintf("m%d", rng.Intn(12))
			c.Send(protocol.OpCodeDel, []byte(k))
			st, _ := c.Read()
			if st == protocol.ResStatusOK {
				delete(pending, k)
				pendingDel[k] = true
			} else if st == protocol.ResStatusTxConflict {
				inTx = false
				clearPending()
			}
		case 4:
			if !inTx {
				c.Send(protocol.OpCodeBegin, nil)
				c.Read()
				inTx = true
			}
			c.Send(protocol.OpCodeGet, []byte(fmt.Sprintf("m%d", rng.Intn(12))))
			c.Read()
		case 5:
			if !inTx {
				break
			}
			c.Send(protocol.OpCodeCommit, nil)
			st, _ := c.Read()
			if st == protocol.ResStatusOK {
				for k := range pendingDel {
					delete(model, k)
				}
				for k, v := range pending {
					model[k] = v
				}
			}
			inTx = false
			clearPending()
		case 6:
			finishAbort()
		case 7:
			c.Send(protocol.OpCodeSet, setPayload("no-tx", "x"))
			st, _ := c.Read()
			if st != protocol.ResStatusTxRequired && st != protocol.ResStatusOK && st != protocol.ResStatusTxConflict {
				// OK or conflict only if a tx leaked; tx-required is the common case.
				if st == protocol.ResStatusErr {
					t.Fatalf("set without tx: unexpected ERR")
				}
			}
			if st == protocol.ResStatusOK {
				// We were still in a tx; abort to resync the monkey state.
				finishAbort()
			}
		default:
			finishAbort()
			c.Send(protocol.OpCodeSelect, []byte("1"))
			if st, _ := c.Read(); st != protocol.ResStatusOK {
				t.Fatalf("select: 0x%x", st)
			}
		}
	}
	finishAbort()

	for k, want := range model {
		got := readKey(t, c, k)
		if string(got) != want {
			t.Fatalf("monkey model %s: want %q got %q", k, want, got)
		}
	}
}

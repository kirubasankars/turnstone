// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"sync"
	"testing"
	"time"
)

// TestWal_AppendDoesNotBlockOnFsync is the concurrent-write contract:
// SET (unsynced append) must be able to take the insert lock while COMMIT's
// fdatasync is still in flight. Holding log.mu across flush serialized every
// writer behind disk latency.
func TestWal_AppendDoesNotBlockOnFsync(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testingBeforeSync = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	t.Cleanup(func() {
		testingBeforeSync = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	payload := encodeRecord(Record{Type: RecordSet, XID: 1, Key: []byte("k"), Value: []byte("v")})
	errCh := make(chan error, 1)
	go func() {
		_, err := log.AppendEncoded(payload, true)
		errCh <- err
	}()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fsync to start")
	}

	done := make(chan error, 1)
	go func() {
		_, err := log.AppendEncoded(payload, false)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("append during fsync: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unsynced append blocked behind in-flight fsync")
	}

	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("synced append: %v", err)
	}
}

// TestWal_ReadLogRangeHidesInFlightSync is the crash-safety contract for
// overlapping insert and fdatasync: a COMMIT (or any synced append) must not
// appear in ReadLogRange — or advance DurableOffset — until fsync returns.
// Concurrent unsynced SETs must not publish that in-flight COMMIT either.
func TestWal_ReadLogRangeHidesInFlightSync(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testingBeforeSync = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	t.Cleanup(func() {
		testingBeforeSync = nil
		select {
		case <-release:
		default:
			close(release)
		}
	})

	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	synced := encodeRecord(Record{Type: RecordCommit, XID: 1})
	errCh := make(chan error, 1)
	go func() {
		_, err := log.AppendEncoded(synced, true)
		errCh <- err
	}()

	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fsync to start")
	}

	if got := log.DurableOffset(); got != 0 {
		t.Fatalf("DurableOffset during fsync = %d, want 0", got)
	}
	data, next, err := log.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 || next != 0 {
		t.Fatalf("ReadLogRange leaked in-flight sync: data=%d next=%d", len(data), next)
	}

	unsynced := encodeRecord(Record{Type: RecordSet, XID: 2, Key: []byte("k"), Value: []byte("v")})
	if _, err := log.AppendEncoded(unsynced, false); err != nil {
		t.Fatal(err)
	}
	if log.WriteOffset() <= log.DurableOffset() {
		t.Fatal("expected write head to race ahead of durable during flush")
	}
	data, next, err = log.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 || next != 0 {
		t.Fatalf("unsynced SET published in-flight COMMIT: data=%d next=%d", len(data), next)
	}

	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("synced append: %v", err)
	}
	if log.DurableOffset() == 0 {
		t.Fatal("DurableOffset still 0 after fsync")
	}
	data, _, err = log.ReadLogRange(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("ReadLogRange empty after fsync published the COMMIT")
	}
}

func TestWal_ActiveSegmentTracksAllocated(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if !log.segments[log.activeIndex].allocated {
		t.Fatal("preallocated segment should be marked allocated")
	}
}

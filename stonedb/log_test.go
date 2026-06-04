// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"sync"
	"testing"
	"time"
)

func TestDataLog_ReadDuringFsync(t *testing.T) {
	dir := t.TempDir()
	log, err := OpenDataLog(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()

	const frames = 64
	offsets := make([]int64, frames)
	vals := make([][]byte, frames)

	for i := 0; i < frames; i++ {
		val := []byte{byte(i), byte(i >> 8), 'x'}
		vals[i] = val
		build := func(opID uint64) []byte {
			return encodeWALRecord(WALRecord{
				Type:  WALRecordSet,
				XID:   1,
				OpID:  opID,
				Key:   []byte("k"),
				Value: val,
			})
		}
		opIDs, offs, err := log.AppendRecordsWithOpIDs(func() uint64 { return uint64(i + 1) }, []func(uint64) []byte{build}, false)
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offs[0]
		_ = opIDs
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		nextOp := frames + 1
		deadline := time.Now().Add(200 * time.Millisecond)
		for time.Now().Before(deadline) {
			build := func(opID uint64) []byte {
				return encodeWALRecord(WALRecord{Type: WALRecordCommit, XID: 1, OpID: opID})
			}
			_, _, err := log.AppendRecordsWithOpIDs(func() uint64 {
				id := uint64(nextOp)
				nextOp++
				return id
			}, []func(uint64) []byte{build}, true)
			if err != nil {
				t.Error(err)
				return
			}
		}
		close(done)
	}()

readLoop:
	for {
		select {
		case <-done:
			break readLoop
		default:
			for i := 0; i < frames; i++ {
				got, err := log.ReadValueAt(offsets[i], uint32(len(vals[i])))
				if err != nil {
					t.Fatalf("read during fsync failed: %v", err)
				}
				if len(got) != len(vals[i]) || got[0] != vals[i][0] {
					t.Fatalf("unexpected value at frame %d", i)
				}
			}
		}
	}

	wg.Wait()
}

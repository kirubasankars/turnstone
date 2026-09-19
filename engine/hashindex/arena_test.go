// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

func TestShardBufferRoundTrip(t *testing.T) {
	buf, err := newShardBuffer(8192)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.close()

	for i := range buf.data {
		buf.data[i] = byte(i)
	}
	if err := growShardBuffer(buf, 1<<20); err != nil {
		t.Fatal(err)
	}
	if len(buf.data) < 1<<20 {
		t.Fatalf("expected grown buffer >= 1MiB, got %d", len(buf.data))
	}
	for i := 0; i < 8192; i++ {
		if buf.data[i] != byte(i) {
			t.Fatalf("grow lost byte at %d", i)
		}
	}
}

func TestShardBufferCloseIdempotent(t *testing.T) {
	buf, err := newShardBuffer(4096)
	if err != nil {
		t.Fatal(err)
	}
	buf.close()
	buf.close()
	if buf.data != nil {
		t.Fatal("expected data nil after close")
	}
}

func TestShardBufferUsesMmapOnUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap arenas use heap fallback on Windows")
	}
	buf, err := newShardBuffer(4096)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.close()
	if buf.mmapBacking == nil {
		t.Fatal("expected mmap backing on Unix")
	}
}

func TestGrowReleasesPreviousMmapBacking(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap arenas use heap fallback on Windows")
	}
	buf, err := newShardBuffer(4096)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.close()

	first := buf.mmapBacking
	if err := growShardBuffer(buf, 1<<20); err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 {
		t.Fatal("missing first mapping")
	}
	if &first[0] == &buf.mmapBacking[0] {
		t.Fatal("grow should replace mmap backing")
	}
}

func TestCompactReleasesOldShardMapping(t *testing.T) {
	idx := New()
	defer idx.Close()

	key := []byte("compact-release")
	for i := 1; i <= 20; i++ {
		idx.Put(key, Version{Offset: int64(i * 10), Xmin: uint64(i)})
	}

	shardIdx := int(hashKey(key) & 255)
	seg := idx.shards[shardIdx]
	seg.mu.RLock()
	oldBacking := seg.arena.mmapBacking
	seg.mu.RUnlock()

	filter := func(_ []byte, chain []Version) []Version {
		if len(chain) == 0 {
			return nil
		}
		return chain[:1]
	}
	if _, err := idx.CompactShard(shardIdx, filter); err != nil {
		t.Fatal(err)
	}

	seg.mu.RLock()
	newBacking := seg.arena.mmapBacking
	seg.mu.RUnlock()

	if runtime.GOOS != "windows" {
		if oldBacking != nil && newBacking != nil && &oldBacking[0] == &newBacking[0] {
			t.Fatal("compact should install a fresh mmap backing")
		}
	}
}

func TestIndexCloseReleasesShardMappings(t *testing.T) {
	idx := New()
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("close-key-%d", i))
		idx.Put(key, Version{Offset: int64(i), Xmin: uint64(i + 1)})
	}
	idx.Close()

	for i, seg := range idx.shards {
		if seg == nil {
			continue
		}
		if seg.buf != nil || seg.arena != nil {
			t.Fatalf("shard %d buffers not cleared on index close", i)
		}
	}
}

func readVmRSSKb() (int, error) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, fmt.Errorf("malformed VmRSS line: %q", line)
		}
		return strconv.Atoi(fields[1])
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("VmRSS not found")
}

func TestMmapArenaRSSDropsAfterClose(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("RSS verification uses /proc/self/status")
	}

	rssBefore, err := readVmRSSKb()
	if err != nil {
		t.Fatal(err)
	}

	idx := New()
	const targetKeys = 4000
	for i := 0; i < targetKeys; i++ {
		key := []byte(fmt.Sprintf("rss-key-%04d", i))
		idx.Put(key, Version{Offset: int64(i), ValueLen: 128, Xmin: uint64(i + 1)})
		for v := 2; v <= 6; v++ {
			idx.Put(key, Version{Offset: int64(i*10 + v), ValueLen: 128, Xmin: uint64(i*10 + v)})
		}
	}

	rssPeak, err := readVmRSSKb()
	if err != nil {
		t.Fatal(err)
	}
	inflation := rssPeak - rssBefore
	if inflation < 1024 {
		t.Skipf("arena inflation too small for RSS test (%d KiB)", inflation)
	}

	idx.Close()
	idx = nil
	runtime.GC()
	debug.FreeOSMemory()

	rssAfter, err := readVmRSSKb()
	if err != nil {
		t.Fatal(err)
	}

	allowed := rssBefore + inflation/4 + 4096
	if rssAfter > allowed {
		t.Fatalf("RSS did not drop after close: before=%dKiB peak=%dKiB after=%dKiB allowed<=%dKiB",
			rssBefore, rssPeak, rssAfter, allowed)
	}
}

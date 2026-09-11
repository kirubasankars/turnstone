// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package hashindex

import "fmt"

// ShardStats reports arena usage for one hash shard.
type ShardStats struct {
	ShardIndex uint32
	SlotCount  uint32
	KeyCount   uint32
	ArenaUsed  uint64
	LiveBytes  uint64
}

// IndexStats aggregates per-shard arena usage.
type IndexStats struct {
	Shards [numShards]ShardStats
}

// Stats returns current arena usage for every shard.
func (idx *Index) Stats() IndexStats {
	var out IndexStats
	for i := 0; i < numShards; i++ {
		seg := idx.shards[i]
		if seg == nil {
			continue
		}
		out.Shards[i] = seg.stats(uint32(i))
	}
	return out
}

func (s *shard) stats(shardIndex uint32) ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.data == nil {
		return ShardStats{ShardIndex: shardIndex}
	}
	st := ShardStats{
		ShardIndex: shardIndex,
		SlotCount:  s.slotCount(),
		KeyCount:   s.keyCount(),
		ArenaUsed:  s.arenaUsed(),
	}
	st.LiveBytes = s.liveBytesLocked(nil)
	return st
}

// VersionFilter returns the version chain to retain for key (newest first).
type VersionFilter func(key []byte, chain []Version) []Version

func (s *shard) liveBytesLocked(filter VersionFilter) uint64 {
	data := s.data
	if data == nil {
		return 0
	}
	table := int(s.tableOff())
	slots := s.slotCount()
	var live uint64
	for slot := uint32(0); slot < slots; slot++ {
		recOff := readU64(data, table+int(slot)*8)
		if recOff == 0 {
			continue
		}
		key := s.readKey(recOff)
		chain := s.readChainLocked(recOff)
		versions := chain
		if filter != nil {
			versions = filter(key, chain)
		}
		if len(versions) == 0 {
			continue
		}
		live += uint64(12 + len(key))
		live += uint64(len(versions)) * versionNodeSz
	}
	return live
}

func (s *shard) readChainLocked(recOff uint64) []Version {
	data := s.data
	var chain []Version
	for node := s.versionHead(recOff); node != 0; node = readU64(data, int(node)+versionSize) {
		if int(node)+versionNodeSz > len(data) {
			break
		}
		chain = append(chain, readVersion(data, int(node)))
	}
	return chain
}

type compactResult struct {
	ArenaBefore uint64
	ArenaAfter  uint64
	KeysBefore  uint32
	KeysAfter   uint32
}

// CompactShard copies live entries into a fresh arena for shard i.
func (idx *Index) CompactShard(shardIndex int, filter VersionFilter) (ShardStats, error) {
	if shardIndex < 0 || shardIndex >= numShards {
		return ShardStats{}, fmt.Errorf("hashindex: invalid shard index %d", shardIndex)
	}
	seg := idx.shards[shardIndex]
	if seg == nil {
		return ShardStats{}, fmt.Errorf("hashindex: shard %d is nil", shardIndex)
	}
	res, err := seg.compact(filter)
	if err != nil {
		return ShardStats{}, err
	}
	return ShardStats{
		ShardIndex: uint32(shardIndex),
		SlotCount:  seg.slotCount(),
		KeyCount:   seg.keyCount(),
		ArenaUsed:  res.ArenaAfter,
		LiveBytes:  res.ArenaAfter,
	}, nil
}

// CompactAll rewrites every shard that has keys, using filter to trim version chains.
func (idx *Index) CompactAll(filter VersionFilter) (IndexStats, error) {
	var out IndexStats
	for i := 0; i < numShards; i++ {
		seg := idx.shards[i]
		if seg == nil {
			continue
		}
		if _, err := seg.compact(filter); err != nil {
			return out, err
		}
		out.Shards[i] = seg.stats(uint32(i))
	}
	return out, nil
}

func (s *shard) compact(filter VersionFilter) (compactResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		return compactResult{}, fmt.Errorf("hashindex: shard is closed")
	}

	res := compactResult{
		ArenaBefore: s.arenaUsed(),
		KeysBefore:  s.keyCount(),
	}

	type keyEntry struct {
		key      []byte
		versions []Version
	}

	data := s.data
	slots := s.slotCount()
	table := int(s.tableOff())
	var entries []keyEntry

	for slot := uint32(0); slot < slots; slot++ {
		recOff := readU64(data, table+int(slot)*8)
		if recOff == 0 {
			continue
		}
		key := s.readKey(recOff)
		chain := s.readChainLocked(recOff)
		versions := chain
		if filter != nil {
			versions = filter(key, chain)
		}
		if len(versions) == 0 {
			continue
		}
		entries = append(entries, keyEntry{key: key, versions: versions})
	}

	slotCount := slots
	if slotCount == 0 {
		slotCount = initialSlots
	}
	tableBytes := uint64(slotCount) * 8
	var liveBytes uint64
	for _, e := range entries {
		liveBytes += uint64(12+len(e.key)) + uint64(len(e.versions))*versionNodeSz
	}
	newSize := int64(headerSize) + int64(tableBytes) + int64(liveBytes) + headerSize
	if newSize < int64(headerSize)+int64(tableBytes)+headerSize {
		newSize = int64(headerSize) + int64(tableBytes) + headerSize
	}

	newData := make([]byte, newSize)
	writeU64(newData, hdrMagicOff, magic)
	writeU32(newData, hdrVersionOff, formatVersion)
	writeU32(newData, hdrSlotCountOff, slotCount)
	writeU32(newData, hdrKeyCountOff, 0)
	tableOff := uint64(headerSize)
	arenaOff := tableOff + tableBytes
	writeU64(newData, hdrTableOffOff, tableOff)
	writeU64(newData, hdrArenaOffOff, arenaOff)
	writeU64(newData, hdrArenaUsedOff, 0)

	newShard := &shard{data: newData}
	newShard.setArenaUsed(0)

	for _, e := range entries {
		recSize := 12 + len(e.key)
		recOff, err := newShard.alloc(recSize)
		if err != nil {
			return res, err
		}
		writeU32(newShard.data, int(recOff), uint32(len(e.key)))
		writeU64(newShard.data, int(recOff)+4, 0)
		copy(newShard.data[int(recOff)+12:], e.key)

		var head uint64
		for i := len(e.versions) - 1; i >= 0; i-- {
			nodeOff, err := newShard.alloc(versionNodeSz)
			if err != nil {
				return res, err
			}
			writeVersion(newShard.data, int(nodeOff), e.versions[i])
			writeU64(newShard.data, int(nodeOff)+versionSize, head)
			head = nodeOff
		}
		newShard.setVersionHead(recOff, head)

		if err := newShard.insertKeySlot(e.key, recOff); err != nil {
			return res, err
		}
	}

	s.data = newShard.data
	res.ArenaAfter = s.arenaUsed()
	res.KeysAfter = s.keyCount()
	return res, nil
}

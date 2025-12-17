package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

const IndexFlushThreshold = 100_000

func (s *Store) Compact() error {
	if !s.compactionRunning.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}

	s.logger.Info("Compaction background job started")

	go func() {
		defer s.compactionRunning.Store(false)
		if err := s.doCompact(); err != nil {
			s.logger.Error("Compaction job failed", "err", err)
		}
	}()

	return nil
}

func (s *Store) doCompact() error {
	s.logger.Info("Starting compaction (Two-Phase)...")
	start := time.Now()

	newGen := s.generation + 1
	tmpJournalPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db.tmp", newGen))
	tmpBoltPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx.tmp", newGen))

	// Cleanup previous failed attempts
	_ = os.Remove(tmpJournalPath)
	_ = os.Remove(tmpBoltPath)

	oldJournalPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", s.generation))
	oldJournalReader, err := OpenJournal(oldJournalPath)
	if err != nil {
		return fmt.Errorf("failed to open reader: %w", err)
	}
	defer oldJournalReader.Close()

	tmpJournal, err := OpenJournal(tmpJournalPath)
	if err != nil {
		return fmt.Errorf("failed to open tmp journal: %w", err)
	}
	defer tmpJournal.Close()

	// Write New Generation Header
	var headerBuf [FileHeaderSize]byte
	binary.BigEndian.PutUint64(headerBuf[:], newGen)
	if _, err := tmpJournal.WriteAt(headerBuf[:], 0); err != nil {
		return fmt.Errorf("failed to write tmp journal header: %w", err)
	}

	tmpBolt, err := bbolt.Open(tmpBoltPath, 0o600, &bbolt.Options{NoFreelistSync: true})
	if err != nil {
		return fmt.Errorf("failed to open tmp bolt: %w", err)
	}
	defer tmpBolt.Close()

	tmpIndex := NewIndex(tmpBolt)

	readOffset := int64(FileHeaderSize)
	writeOffset := int64(FileHeaderSize)
	var keptCount, droppedCount int

	payloadPtr := getBuffer(4096)
	defer putBuffer(payloadPtr)

	threshold := atomic.LoadInt64(&s.compactionThreshold)
	iteration := 0

	// Phase 1: Catch-up (Concurrent)
	for {
		iteration++
		targetOffset := atomic.LoadInt64(&s.offset)
		lag := targetOffset - readOffset

		s.logger.Info("Compaction Loop", "iter", iteration, "processed", readOffset, "target", targetOffset, "lag", lag)

		if lag < threshold {
			break
		}

		var newWriteOffset int64
		var k, d int
		newWriteOffset, k, d, payloadPtr, err = s.copyRange(
			oldJournalReader, tmpJournal, tmpIndex,
			readOffset, targetOffset, writeOffset, payloadPtr,
			false,
		)
		if err != nil {
			return err
		}

		readOffset = targetOffset
		writeOffset = newWriteOffset
		keptCount += k
		droppedCount += d

		if tmpIndex.Len() > IndexFlushThreshold {
			if tmpIndex.Rotate() {
				if err := tmpIndex.FlushToBolt(0); err != nil {
					return fmt.Errorf("failed to flush tmp index: %w", err)
				}
				tmpIndex.FinishFlush()
			}
		}
	}

	// Wait for pending flushes on the MAIN store to reduce lock contention risk
	s.flushWg.Wait()

	// Phase 2: Stop-The-World
	s.mu.Lock()
	s.compacting = true

	// Clean up old state that will be invalid in the new generation
	s.activeSnapshots = make(map[int64]int)
	s.conflictIndex = make(map[string][]int64)

	finalOffset := s.offset
	s.logger.Info("Compaction Finalizing (Stop-The-World)", "remaining_bytes", finalOffset-readOffset)

	var newWriteOffset int64
	var k, d int
	newWriteOffset, k, d, payloadPtr, err = s.copyRange(
		oldJournalReader, tmpJournal, tmpIndex,
		readOffset, finalOffset, writeOffset, payloadPtr,
		true,
	)
	if err != nil {
		s.compacting = false
		s.mu.Unlock()
		return err
	}
	writeOffset = newWriteOffset
	keptCount += k
	droppedCount += d

	if err := tmpJournal.Sync(); err != nil {
		s.compacting = false
		s.mu.Unlock()
		return err
	}

	// PERFORMANCE FIX: Capture the 'hot' active entries from the tmpIndex before we close it.
	// We will inject these into the new s.index after reopen to prevent a cold cache penalty.
	warmCache := make(map[string][]IndexEntry, len(tmpIndex.active))
	for key, entries := range tmpIndex.active {
		// Deep copy the slice to be safe
		newEntries := make([]IndexEntry, len(entries))
		copy(newEntries, entries)
		warmCache[key] = newEntries
	}

	// Flush whatever is left in tmpIndex to disk for durability
	if tmpIndex.Len() > 0 {
		tmpIndex.Rotate()
		if err := tmpIndex.FlushToBolt(0); err != nil {
			s.compacting = false
			s.mu.Unlock()
			return fmt.Errorf("failed to final flush tmp index: %w", err)
		}
	}
	tmpIndex.FinishFlush()

	oldJournalReader.Close()
	tmpJournal.Close()
	tmpBolt.Close()

	// Close current active files
	if err := s.journal.Close(); err != nil {
		s.logger.Error("failed to close active journal", "err", err)
	}
	if err := s.bolt.Close(); err != nil {
		s.logger.Error("failed to close active bolt", "err", err)
	}

	newJournalPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", newGen))
	newBoltPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx", newGen))

	if err := os.Rename(tmpJournalPath, newJournalPath); err != nil {
		s.compacting = false
		s.mu.Unlock()
		s.reopen() // Try to recover
		return fmt.Errorf("failed to swap journal: %w", err)
	}
	if err := os.Rename(tmpBoltPath, newBoltPath); err != nil {
		s.compacting = false
		s.mu.Unlock()
		s.reopen() // Try to recover
		return fmt.Errorf("failed to swap bolt: %w", err)
	}

	// Reopen the store with the new files
	s.generation = newGen
	if err := s.reopen(); err != nil {
		s.compacting = false
		s.mu.Unlock()
		return fmt.Errorf("critical error reopening: %w", err)
	}

	// Inject the warm cache into the new index
	s.index.active = warmCache

	s.offset = writeOffset
	s.compacting = false
	s.mu.Unlock()

	// remove old files
	oldDbPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", newGen-1))
	oldIdxPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx", newGen-1))
	if err := os.Remove(oldDbPath); err != nil {
		s.logger.Warn("failed to remove old db file", "err", err)
	}
	if err := os.Remove(oldIdxPath); err != nil {
		s.logger.Warn("failed to remove old idx file", "err", err)
	}

	s.logger.Info("Compaction complete",
		"duration", time.Since(start),
		"old_size", finalOffset,
		"new_size", writeOffset,
		"kept", keptCount,
		"dropped", droppedCount,
		"new_gen", s.generation,
	)

	return nil
}

func (s *Store) copyRange(
	src *JournalFile, dst *JournalFile, tmpIndex *Index,
	startOff, endOff, writeOff int64,
	payloadPtr *[]byte,
	alreadyLocked bool,
) (int64, int, int, *[]byte, error) {
	if _, err := src.Seek(startOff, 0); err != nil {
		return writeOff, 0, 0, payloadPtr, err
	}

	r := io.LimitReader(src.f, endOff-startOff)
	reader := bufio.NewReader(r)

	headerBuf := make([]byte, HeaderSize)
	currentReadOff := startOff
	kept := 0
	dropped := 0

	for currentReadOff < endOff {
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err == io.EOF {
				break
			}
			return writeOff, kept, dropped, payloadPtr, fmt.Errorf("read error at %d: %w", currentReadOff, err)
		}

		keyLen := binary.BigEndian.Uint32(headerBuf[0:4])
		valLen := binary.BigEndian.Uint32(headerBuf[4:8])
		storedCrc := binary.BigEndian.Uint32(headerBuf[8:12])

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		if int64(cap(*payloadPtr)) < payloadLen {
			putBuffer(payloadPtr)
			payloadPtr = getBuffer(int(payloadLen))
		}
		payload := (*payloadPtr)[:payloadLen]

		if _, err := io.ReadFull(reader, payload); err != nil {
			return writeOff, kept, dropped, payloadPtr, fmt.Errorf("payload read error at %d: %w", currentReadOff, err)
		}

		if !s.skipCrc {
			if crc32.Checksum(payload, CrcTable) != storedCrc {
				return writeOff, kept, dropped, payloadPtr, fmt.Errorf("crc mismatch at %d", currentReadOff)
			}
		}

		key := string(payload[:keyLen])
		totalEntrySize := int64(HeaderSize) + payloadLen

		var isLive bool
		checkLiveness := func() {
			latest, exists := s.index.GetLatest(key)
			// Check if the latest version in the CURRENT index matches the offset we are reading.
			// If yes, this is the live version.
			if exists && latest.Offset() == currentReadOff {
				isLive = true
			}
		}

		if alreadyLocked {
			checkLiveness()
		} else {
			s.mu.RLock()
			checkLiveness()
			s.mu.RUnlock()
		}

		if isLive {
			if _, err := dst.WriteAt(headerBuf, writeOff); err != nil {
				return writeOff, kept, dropped, payloadPtr, err
			}
			if _, err := dst.WriteAt(payload, writeOff+HeaderSize); err != nil {
				return writeOff, kept, dropped, payloadPtr, err
			}

			isDel := valLen == Tombstone
			// Note: We use math.MaxInt64 as minReadVersion because we are building a fresh index
			// and these entries are effectively "base" entries for the new generation.
			tmpIndex.Set(key, writeOff, isDel, math.MaxInt64)

			writeOff += totalEntrySize
			kept++
		} else {
			dropped++
		}

		currentReadOff += totalEntrySize
	}

	return writeOff, kept, dropped, payloadPtr, nil
}

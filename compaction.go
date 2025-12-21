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

func (s *Store) doCompact() (retErr error) {
	s.logger.Info("Starting compaction (Two-Phase)...")
	start := time.Now()

	newGen := s.generation + 1
	tmpJournalPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db.tmp", newGen))
	tmpBoltPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx.tmp", newGen))

	// Ensure cleanup of temp files on failure
	defer func() {
		if retErr != nil {
			// Best effort cleanup
			if err := os.Remove(tmpJournalPath); err != nil && !os.IsNotExist(err) {
				s.logger.Warn("failed to cleanup tmp journal", "path", tmpJournalPath, "err", err)
			}
			if err := os.Remove(tmpBoltPath); err != nil && !os.IsNotExist(err) {
				s.logger.Warn("failed to cleanup tmp bolt", "path", tmpBoltPath, "err", err)
			}
		}
	}()

	// Cleanup previous failed attempts explicitly before starting
	if err := os.Remove(tmpJournalPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clear old tmp journal: %w", err)
	}
	if err := os.Remove(tmpBoltPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clear old tmp bolt: %w", err)
	}

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

	tmpBolt, err := bbolt.Open(tmpBoltPath, 0o600, &bbolt.Options{NoFreelistSync: true})
	if err != nil {
		return fmt.Errorf("failed to open tmp bolt: %w", err)
	}
	defer tmpBolt.Close()

	tmpIndex := NewIndex(tmpBolt)

	readOffset := int64(0)
	writeOffset := int64(0)
	var keptCount, droppedCount int

	threshold := atomic.LoadInt64(&s.compactionThreshold)
	iteration := 0

	// Phase 1: Catch-up (Concurrent)
	for {
		iteration++
		targetOffset := atomic.LoadInt64(&s.offset)
		lag := targetOffset - readOffset

		s.logger.Info("Compaction Loop", "iter", iteration, "processed", readOffset, "target", targetOffset, "lag", lag)

		// Exit condition: lag is small enough OR we've looped too many times (avoid infinite loop)
		if lag < threshold || iteration > MaxCompactionIterations {
			break
		}

		var newWriteOffset int64
		var k, d int
		newWriteOffset, k, d, err = s.copyRange(
			oldJournalReader, tmpJournal, tmpIndex,
			readOffset, targetOffset, writeOffset,
			false,
		)
		if err != nil {
			return err
		}

		readOffset = targetOffset
		writeOffset = newWriteOffset
		keptCount += k
		droppedCount += d

		// Intermediate flushing to keep memory usage low.
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
	s.conflictQueue = make([]Mutation, 0, 1024)

	finalOffset := s.offset
	s.logger.Info("Compaction Finalizing (Stop-The-World)", "remaining_bytes", finalOffset-readOffset)

	var newWriteOffset int64
	var k, d int
	newWriteOffset, k, d, err = s.copyRange(
		oldJournalReader, tmpJournal, tmpIndex,
		readOffset, finalOffset, writeOffset,
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

	// Flush whatever is left in tmpIndex to disk for durability
	tmpIndex.Rotate()
	if err := tmpIndex.FlushToBolt(0); err != nil {
		s.compacting = false
		s.mu.Unlock()
		return fmt.Errorf("failed to final flush tmp index: %w", err)
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
		// ROLLBACK
		if rollbackErr := os.Rename(newJournalPath, tmpJournalPath); rollbackErr != nil {
			s.logger.Error("CRITICAL: Failed to rollback journal rename", "err", rollbackErr)
		}

		s.compacting = false
		s.mu.Unlock()
		s.reopen()
		return fmt.Errorf("failed to swap bolt: %w", err)
	}

	// Reopen the store with the new files
	s.generation = newGen
	if err := s.reopen(); err != nil {
		s.compacting = false
		s.mu.Unlock()
		return fmt.Errorf("critical error reopening: %w", err)
	}

	s.offset = writeOffset
	s.minReadVersion = writeOffset
	s.lastPrunedVersion = writeOffset

	s.compacting = false
	s.mu.Unlock()

	// Graceful Deletion Logic
	oldGen := newGen - 1
	oldDbPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", oldGen))
	oldIdxPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx", oldGen))

	s.logger.Info("Compaction complete. Scheduling cleanup.",
		"old_gen", oldGen,
		"grace_period", s.compactionGracePeriod,
	)

	time.AfterFunc(s.compactionGracePeriod, func() {
		s.logger.Info("Cleaning up old generation files", "gen", oldGen)
		os.Remove(oldDbPath)
		os.Remove(oldIdxPath)
	})

	s.logger.Info("Compaction stats",
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
	alreadyLocked bool,
) (int64, int, int, error) {
	if _, err := src.Seek(startOff, 0); err != nil {
		return writeOff, 0, 0, err
	}

	r := io.LimitReader(src.f, endOff-startOff)
	reader := bufio.NewReader(r)

	currentReadOff := startOff
	kept := 0
	dropped := 0

	// Batching configuration
	const batchSize = 1000
	type batchItem struct {
		headerBuf [HeaderSize]byte
		key       string
		payload   []byte
		entryOff  int64
		entrySize int64
		valLen    uint32
	}
	batch := make([]batchItem, 0, batchSize)

	currentFlushOffset := writeOff
	var scratch [8]byte

	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		survivors := make([]bool, len(batch))

		checkLiveness := func() {
			for i, item := range batch {
				latest, exists := s.index.GetLatest(item.key)
				if exists && latest.Offset() == item.entryOff {
					survivors[i] = true
				}
			}
		}

		if alreadyLocked {
			checkLiveness()
		} else {
			s.mu.RLock()
			checkLiveness()
			s.mu.RUnlock()
		}

		for i, item := range batch {
			if !survivors[i] {
				dropped++
				continue
			}

			// Reset MinReadVersion to 0 in new file
			binary.BigEndian.PutUint64(item.headerBuf[8:16], 0)

			// Write Header
			if _, err := dst.WriteAt(item.headerBuf[:], currentFlushOffset); err != nil {
				return err
			}
			currentFlushOffset += int64(HeaderSize)

			// Write Payload
			if _, err := dst.WriteAt(item.payload, currentFlushOffset); err != nil {
				return err
			}
			currentFlushOffset += int64(len(item.payload))

			isDel := item.valLen == Tombstone

			tmpIndex.Set(item.key, writeOff, item.entrySize, isDel, math.MaxInt64)

			writeOff += item.entrySize
			kept++
		}

		batch = batch[:0]
		return nil
	}

	for currentReadOff < endOff {
		var item batchItem
		item.entryOff = currentReadOff

		if _, err := io.ReadFull(reader, item.headerBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return writeOff, kept, dropped, fmt.Errorf("read error at %d: %w", currentReadOff, err)
		}

		keyLen := binary.BigEndian.Uint32(item.headerBuf[0:4])
		valLen := binary.BigEndian.Uint32(item.headerBuf[4:8])
		storedCrc := binary.BigEndian.Uint32(item.headerBuf[16:20])

		item.valLen = valLen

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		item.payload = make([]byte, payloadLen)

		if _, err := io.ReadFull(reader, item.payload); err != nil {
			return writeOff, kept, dropped, fmt.Errorf("payload read error at %d: %w", currentReadOff, err)
		}

		if !s.skipCrc {
			digest := crc32.New(CrcTable)
			binary.BigEndian.PutUint32(scratch[0:4], keyLen)
			binary.BigEndian.PutUint32(scratch[4:8], valLen)
			digest.Write(scratch[:])
			digest.Write(item.payload)

			if digest.Sum32() != storedCrc {
				return writeOff, kept, dropped, fmt.Errorf("crc mismatch at %d", currentReadOff)
			}
		}

		item.key = string(item.payload[:keyLen])
		item.entrySize = int64(HeaderSize) + payloadLen

		batch = append(batch, item)
		currentReadOff += item.entrySize

		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				return writeOff, kept, dropped, err
			}
		}
	}

	if err := processBatch(); err != nil {
		return writeOff, kept, dropped, err
	}

	return writeOff, kept, dropped, nil
}

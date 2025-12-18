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
		// ROLLBACK: Try to rename the journal back to tmp if bolt fails.
		// This prevents leaving the system in a split-brain state (new DB, old Index).
		if rollbackErr := os.Rename(newJournalPath, tmpJournalPath); rollbackErr != nil {
			s.logger.Error("CRITICAL: Failed to rollback journal rename after bolt swap failure", "err", rollbackErr)
		}

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
	if err := os.Remove(oldDbPath); err != nil && !os.IsNotExist(err) {
		s.logger.Warn("failed to remove old db file", "err", err)
	}
	if err := os.Remove(oldIdxPath); err != nil && !os.IsNotExist(err) {
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

	currentReadOff := startOff
	kept := 0
	dropped := 0

	// Batching configuration
	const batchSize = 1000
	type batchItem struct {
		headerBuf [HeaderSize]byte
		key       string
		payload   []byte // Local copy of the payload
		entryOff  int64  // Original offset in the old file
		entrySize int64
		valLen    uint32
	}
	batch := make([]batchItem, 0, batchSize)

	// processBatch handles the locking (decision) and IO (writing) phases separately
	processBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// 1. DECISION PHASE: Check liveness (Hold Lock)
		// We use a bool slice to mark survivors so we don't do IO under lock
		survivors := make([]bool, len(batch))

		checkLiveness := func() {
			for i, item := range batch {
				latest, exists := s.index.GetLatest(item.key)
				// It is live if the version in the MAIN index matches exactly the offset we just read
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

		// 2. IO PHASE: Write to new file (No Lock)
		for i, item := range batch {
			if !survivors[i] {
				dropped++
				continue
			}

			// Write Header
			if _, err := dst.WriteAt(item.headerBuf[:], writeOff); err != nil {
				return err
			}
			// Write Payload
			if _, err := dst.WriteAt(item.payload, writeOff+HeaderSize); err != nil {
				return err
			}

			isDel := item.valLen == Tombstone
			// Set entry in the temporary index for the new generation
			tmpIndex.Set(item.key, writeOff, isDel, math.MaxInt64)

			writeOff += item.entrySize
			kept++
		}

		// Reset batch for next iteration
		batch = batch[:0]
		return nil
	}

	for currentReadOff < endOff {
		// Prepare a new item
		var item batchItem
		item.entryOff = currentReadOff

		// Read Header
		if _, err := io.ReadFull(reader, item.headerBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return writeOff, kept, dropped, payloadPtr, fmt.Errorf("read error at %d: %w", currentReadOff, err)
		}

		keyLen := binary.BigEndian.Uint32(item.headerBuf[0:4])
		valLen := binary.BigEndian.Uint32(item.headerBuf[4:8])
		storedCrc := binary.BigEndian.Uint32(item.headerBuf[8:12])
		item.valLen = valLen

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		// Use the pooled buffer for reading from disk
		if int64(cap(*payloadPtr)) < payloadLen {
			putBuffer(payloadPtr)
			payloadPtr = getBuffer(int(payloadLen))
		}
		rawPayload := (*payloadPtr)[:payloadLen]

		if _, err := io.ReadFull(reader, rawPayload); err != nil {
			return writeOff, kept, dropped, payloadPtr, fmt.Errorf("payload read error at %d: %w", currentReadOff, err)
		}

		if !s.skipCrc {
			if crc32.Checksum(rawPayload, CrcTable) != storedCrc {
				return writeOff, kept, dropped, payloadPtr, fmt.Errorf("crc mismatch at %d", currentReadOff)
			}
		}

		// Copy data to batch item (we need to copy because payloadPtr is reused)
		item.key = string(rawPayload[:keyLen])
		item.payload = make([]byte, payloadLen)
		copy(item.payload, rawPayload)
		item.entrySize = int64(HeaderSize) + payloadLen

		batch = append(batch, item)
		currentReadOff += item.entrySize

		// If batch is full, process it
		if len(batch) >= batchSize {
			if err := processBatch(); err != nil {
				return writeOff, kept, dropped, payloadPtr, err
			}
		}
	}

	// Process any remaining items
	if err := processBatch(); err != nil {
		return writeOff, kept, dropped, payloadPtr, err
	}

	return writeOff, kept, dropped, payloadPtr, nil
}

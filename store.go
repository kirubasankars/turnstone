package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

type Store struct {
	mu            sync.RWMutex
	journal       *JournalFile
	index         *Index
	bolt          *bbolt.DB
	startTime     time.Time
	logger        *slog.Logger
	allowTruncate bool
	skipCrc       bool

	// Fsync Configuration
	useFsync      bool          // If true, sync on every commit
	fsyncInterval time.Duration // If useFsync is false, sync periodically here

	offset     int64
	generation uint64

	// locking states
	compacting        bool           // Guard for Critical Section (Phase 2)
	compactionRunning atomic.Bool    // Guard for Global Compaction Process (Phase 1 + 2)
	flushWg           sync.WaitGroup // WaitGroup to track active BoltDB flushes

	dataDir string

	activeSnapshots map[int64]int

	opsCh chan *request
	done  chan struct{}
	wg    sync.WaitGroup

	recentMutations []Mutation
	mutationIndex   map[string]int64

	pendingWriteBytes   int64
	compactionThreshold int64
}

func NewStore(dataDir string, logger *slog.Logger, allowTruncate bool, skipCrc bool, useFsync bool, fsyncInterval time.Duration) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to init data dir: %w", err)
	}

	journalPath := filepath.Join(dataDir, DefaultDBName)
	boltPath := filepath.Join(dataDir, BoltDBName)
	logger.Info("Opening store", "journal", journalPath, "bolt", boltPath, "fsync", useFsync, "fsync_interval", fsyncInterval)

	journal, err := OpenJournal(journalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open journal: %w", err)
	}

	boltOpts := &bbolt.Options{Timeout: 1 * time.Second, NoSync: !useFsync}
	boltDB, err := bbolt.Open(boltPath, 0600, boltOpts)
	if err != nil {
		journal.Close()
		return nil, fmt.Errorf("failed to open bolt: %w", err)
	}

	idx := NewIndex(boltDB)

	s := &Store{
		journal:             journal,
		bolt:                boltDB,
		index:               idx,
		recentMutations:     make([]Mutation, 0, 10000),
		mutationIndex:       make(map[string]int64),
		activeSnapshots:     make(map[int64]int),
		startTime:           time.Now(),
		logger:              logger,
		allowTruncate:       allowTruncate,
		skipCrc:             skipCrc,
		useFsync:            useFsync,
		fsyncInterval:       fsyncInterval,
		opsCh:               make(chan *request, 5000),
		done:                make(chan struct{}),
		dataDir:             dataDir,
		compactionThreshold: DefaultCompactionThreshold,
	}

	if err := s.recover(); err != nil {
		boltDB.Close()
		journal.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	s.wg.Add(1)
	go s.runLoop()

	return s, nil
}

func (s *Store) recover() error {
	// 1. Check file state
	info, err := s.journal.f.Stat()
	if err != nil {
		return err
	}

	// 2. Initialize new file or Read existing header
	if info.Size() == 0 {
		s.logger.Info("Initializing new journal with Generation 1")
		// New File: Write Generation 1 Header (8 bytes)
		var buf [FileHeaderSize]byte
		binary.BigEndian.PutUint64(buf[:], 1)

		if _, err := s.journal.WriteAt(buf[:], 0); err != nil {
			return fmt.Errorf("failed to write file header: %w", err)
		}
		if err := s.journal.Sync(); err != nil {
			return err
		}
		s.generation = 1
		s.offset = int64(FileHeaderSize)
	} else {
		// Existing File: Check size and Read Generation
		if info.Size() < int64(FileHeaderSize) {
			return fmt.Errorf("file too small to contain header (size: %d)", info.Size())
		}

		headerBuf := make([]byte, FileHeaderSize)
		if _, err := s.journal.ReadAt(headerBuf, 0); err != nil {
			return fmt.Errorf("failed to read file header: %w", err)
		}
		s.generation = binary.BigEndian.Uint64(headerBuf)
		s.logger.Info("Recovered generation", "gen", s.generation)
	}

	// 3. Try to find the checkpoint in BoltDB
	var startOffset int64 = 0
	var lastOffsetFound bool

	err = s.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketMeta))
		if b != nil {
			v := b.Get([]byte(KeyLastOffset))
			if v != nil {
				startOffset = int64(binary.BigEndian.Uint64(v))
				lastOffsetFound = true
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read index meta: %w", err)
	}

	// Ensure we don't start reading before the header
	if startOffset < int64(FileHeaderSize) {
		startOffset = int64(FileHeaderSize)
	}

	if !lastOffsetFound {
		s.logger.Warn("Index checkpoint not found. Rebuilding index from scratch...")
		startOffset = int64(FileHeaderSize)
	} else {
		s.logger.Info("Recovering from checkpoint", "offset", startOffset)
	}

	if _, err := s.journal.Seek(startOffset, 0); err != nil {
		return fmt.Errorf("journal seek failed: %w", err)
	}

	bufReader := bufio.NewReader(s.journal.f)
	header := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 4096)

	var validOffset int64 = startOffset
	count := 0
	startTime := time.Now()

	for {
		if _, err := io.ReadFull(bufReader, header); err != nil {
			if err == io.EOF {
				break
			}
			if !s.allowTruncate {
				return fmt.Errorf("corruption at offset %d: %w", validOffset, err)
			}
			s.logger.Warn("Partial header detected. Truncating journal.", "offset", validOffset)
			return s.truncate(validOffset)
		}

		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])
		storedCrc := binary.BigEndian.Uint32(header[8:12])

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		if payloadLen > int64(MaxKeySize+MaxValueSize) {
			return fmt.Errorf("corruption: massive payload length (%d) at offset %d", payloadLen, validOffset)
		}

		if int64(cap(payloadBuf)) < payloadLen {
			payloadBuf = make([]byte, payloadLen)
		}
		payload := payloadBuf[:payloadLen]

		if _, err := io.ReadFull(bufReader, payload); err != nil {
			if !s.allowTruncate {
				return fmt.Errorf("partial payload at %d: %w", validOffset, err)
			}
			s.logger.Warn("Partial payload detected. Truncating.", "offset", validOffset)
			return s.truncate(validOffset)
		}

		if !s.skipCrc {
			if crc32.Checksum(payload, CrcTable) != storedCrc {
				if validOffset < int64(FileHeaderSize) {
					return fmt.Errorf("critical corruption: invalid CRC in header region")
				}
				if !s.allowTruncate {
					return fmt.Errorf("crc mismatch at %d", validOffset)
				}
				s.logger.Warn("CRC mismatch. Truncating.", "offset", validOffset)
				return s.truncate(validOffset)
			}
		}

		key := string(payload[:keyLen])
		isDel := valLen == Tombstone

		s.index.Set(key, validOffset, isDel, 0)

		validOffset += int64(HeaderSize) + payloadLen
		count++

		if count%50_000 == 0 {
			s.logger.Info("Reindexing...", "processed", count, "current_offset", validOffset)
		}
	}

	s.offset = validOffset

	if !lastOffsetFound && count > 0 {
		s.logger.Info("Rebuild complete. Triggering initial index save...")
		s.triggerBoltFlush()
	}

	s.logger.Info("Recovery complete",
		"entries", count,
		"gen", s.generation,
		"final_offset", s.offset,
		"duration", time.Since(startTime))
	return nil
}

func (s *Store) truncate(offset int64) error {
	if err := s.journal.Truncate(offset); err != nil {
		return err
	}
	if _, err := s.journal.Seek(offset, 0); err != nil {
		return err
	}
	return nil
}

func (s *Store) Close() error {
	close(s.done)
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bolt != nil {
		s.bolt.Close()
	}
	if s.journal != nil {
		return s.journal.Close()
	}
	return nil
}

func (s *Store) flushJournal(pending []*request, batchBuf *bytes.Buffer) {
	if len(pending) == 0 {
		return
	}
	if err := s.flushBatch(pending, batchBuf); err != nil {
		s.logger.Error("Journal Flush failed", "err", err)
	}
	for _, req := range pending {
		if !req.cancelled.Load() {
			atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
		}
	}
	batchBuf.Reset()
}

func (s *Store) runLoop() {
	defer s.wg.Done()

	var batchBuf bytes.Buffer
	pending := make([]*request, 0, MaxBatchSize)

	batchTicker := time.NewTicker(BatchDelay)
	defer batchTicker.Stop()

	flushTicker := time.NewTicker(FlushInterval)
	defer flushTicker.Stop()

	var fsyncChan <-chan time.Time
	if !s.useFsync && s.fsyncInterval > 0 {
		fsyncTicker := time.NewTicker(s.fsyncInterval)
		defer fsyncTicker.Stop()
		fsyncChan = fsyncTicker.C
	}

	flush := func() {
		if len(pending) > 0 {
			s.flushJournal(pending, &batchBuf)
			pending = pending[:0]
		}
	}

	for {
		select {
		case <-s.done:
			flush()
			s.drainChannels()
			return

		case <-flushTicker.C:
			s.triggerBoltFlush()

		case <-fsyncChan:
			if err := s.journal.Sync(); err != nil {
				s.logger.Error("Background fsync failed", "err", err)
			}

		case req := <-s.opsCh:
			if req.cancelled.Load() {
				continue
			}

			s.mu.RLock()
			currentGen := s.generation
			s.mu.RUnlock()

			if req.generation != currentGen {
				req.resp <- ErrConflict
				atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
				continue
			}

			activePending := make([]*request, 0, len(pending))
			for _, p := range pending {
				if !p.cancelled.Load() {
					activePending = append(activePending, p)
				}
			}

			if s.hasConflict(req, activePending) {
				req.resp <- ErrConflict
				atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
				continue
			}
			if !s.validateBatch(req) {
				atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
				continue
			}

			s.serializeBatch(req, &batchBuf)
			pending = append(pending, req)
			if len(pending) >= MaxBatchSize {
				flush()
			}

		case <-batchTicker.C:
			flush()
		}
	}
}

func (s *Store) triggerBoltFlush() {
	s.mu.Lock()
	if s.compacting {
		s.mu.Unlock()
		return
	}
	hasWork := s.index.Rotate()
	minVer := s.getMinReadVersion()

	if hasWork {
		s.flushWg.Add(1)
	}
	s.mu.Unlock()

	if !hasWork {
		return
	}

	go func(ver int64) {
		defer s.flushWg.Done()
		if err := s.index.FlushToBolt(ver); err != nil {
			s.logger.Error("BoltDB flush failed", "err", err)
		}
		s.mu.Lock()
		s.index.FinishFlush()
		s.mu.Unlock()
	}(minVer)
}

func (s *Store) getMinReadVersion() int64 {
	minVer := s.offset
	if len(s.activeSnapshots) > 0 {
		for ver := range s.activeSnapshots {
			if ver < minVer {
				minVer = ver
			}
		}
	}
	return minVer
}

func (s *Store) AcquireSnapshot() (int64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ver := s.offset
	gen := s.generation
	s.activeSnapshots[ver]++
	return ver, gen
}

func (s *Store) ReleaseSnapshot(ver int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count, ok := s.activeSnapshots[ver]; ok {
		s.activeSnapshots[ver] = count - 1
		if s.activeSnapshots[ver] <= 0 {
			delete(s.activeSnapshots, ver)
		}
	}
}

func (s *Store) hasConflict(req *request, pending []*request) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hasWrites := false
	for _, op := range req.ops {
		if op.opType != OpJournalGet {
			hasWrites = true
			break
		}
	}

	// Read-Write Conflict (Anti-Dependency) Detection for SSI
	if !hasWrites {
		return false
	}

	// 1. Check against pending (uncommitted) batches in memory
	for _, pReq := range pending {
		// Check Write-Write conflicts
		for _, op := range req.ops {
			if op.opType != OpJournalGet {
				if _, exists := pReq.accessMap[op.key]; exists {
					return true
				}
			}
		}
		// Check Read-Write conflicts
		for _, readKey := range req.reads {
			if _, exists := pReq.accessMap[readKey]; exists {
				return true
			}
		}
	}

	// 2. Check against recently committed mutations (since snapshot)
	// Check Write-Write
	for _, op := range req.ops {
		if op.opType != OpJournalGet {
			if lastCommitOffset, exists := s.mutationIndex[op.key]; exists {
				if lastCommitOffset >= req.readVersion {
					return true
				}
			}
		}
	}

	// Check Read-Write
	for _, readKey := range req.reads {
		if lastCommitOffset, exists := s.mutationIndex[readKey]; exists {
			if lastCommitOffset >= req.readVersion {
				return true
			}
		}
	}
	return false
}

func (s *Store) validateBatch(req *request) bool {
	return true
}

func (s *Store) serializeBatch(req *request, buf *bytes.Buffer) {
	for _, op := range req.ops {
		if op.opType == OpJournalGet {
			req.opLens = append(req.opLens, 0)
			continue
		}
		startLen := buf.Len()
		keyLen := len(op.key)
		valLen := uint32(len(op.val))
		if op.opType == OpJournalDelete {
			valLen = Tombstone
		}
		headerPos := buf.Len()
		buf.Write(make([]byte, HeaderSize))
		buf.WriteString(op.key)
		if op.opType == OpJournalSet {
			buf.Write(op.val)
		}
		payload := buf.Bytes()[headerPos+HeaderSize : buf.Len()]

		// Use Castagnoli Table
		crc := crc32.Checksum(payload, CrcTable)

		var header [HeaderSize]byte
		binary.BigEndian.PutUint32(header[0:4], uint32(keyLen))
		binary.BigEndian.PutUint32(header[4:8], valLen)
		binary.BigEndian.PutUint32(header[8:12], crc)
		copy(buf.Bytes()[headerPos:], header[:])
		req.opLens = append(req.opLens, buf.Len()-startLen)
	}
}

func (s *Store) flushBatch(pending []*request, buf *bytes.Buffer) error {
	anyCancelled := false
	for _, req := range pending {
		if req.cancelled.Load() {
			anyCancelled = true
			break
		}
	}

	if anyCancelled {
		for _, req := range pending {
			var err error
			if req.cancelled.Load() {
				err = errors.New("cancelled")
			} else {
				err = ErrBusy
			}
			select {
			case req.resp <- err:
			default:
			}
		}
		return nil
	}

	if _, err := s.journal.WriteAt(buf.Bytes(), s.offset); err != nil {
		for _, req := range pending {
			req.resp <- err
		}
		return err
	}

	if s.useFsync {
		if err := s.journal.Sync(); err != nil {
			for _, req := range pending {
				req.resp <- err
			}
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	currentOffset := s.offset
	minVer := s.getMinReadVersion()

	for _, req := range pending {
		for i, op := range req.ops {
			if op.opType == OpJournalGet {
				continue
			}
			opLen := req.opLens[i]
			reqOffset := currentOffset
			currentOffset += int64(opLen)

			if op.opType == OpJournalDelete {
				s.index.Set(op.key, reqOffset, true, minVer)
			} else {
				s.index.Set(op.key, reqOffset, false, minVer)
			}

			mutation := Mutation{Key: op.key, Offset: reqOffset}
			s.recentMutations = append(s.recentMutations, mutation)
			s.mutationIndex[op.key] = reqOffset
		}
		select {
		case req.resp <- nil:
		default:
		}
	}
	s.offset = currentOffset
	s.pruneMutations(minVer)
	return nil
}

func (s *Store) pruneMutations(minVer int64) {
	pruneIdx := 0
	for pruneIdx < len(s.recentMutations) {
		m := s.recentMutations[pruneIdx]
		if m.Offset < minVer {
			if storedOffset, ok := s.mutationIndex[m.Key]; ok && storedOffset == m.Offset {
				delete(s.mutationIndex, m.Key)
			}
			pruneIdx++
		} else {
			break
		}
	}

	if pruneIdx > 0 {
		for i := range pruneIdx {
			s.recentMutations[i] = Mutation{}
		}
		s.recentMutations = s.recentMutations[pruneIdx:]
		if cap(s.recentMutations) > 4096 && len(s.recentMutations) < cap(s.recentMutations)/4 {
			newSlice := make([]Mutation, len(s.recentMutations))
			copy(newSlice, s.recentMutations)
			s.recentMutations = newSlice
		}
	}
}

func (s *Store) drainChannels() {
	for {
		select {
		case req := <-s.opsCh:
			req.resp <- ErrClosed
			atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
		default:
			return
		}
	}
}

func (s *Store) Get(key string, readVersion int64, generation uint64) ([]byte, error) {
	// FIX: Hold the read lock for the ENTIRE duration of the function.
	// This prevents the compaction process (which requires s.mu.Lock) from swapping
	// the file handle via s.reopen() while we are reading from it.
	s.mu.RLock()
	defer s.mu.RUnlock()

	currentGen := s.generation
	entry, ok := s.index.Get(key, readVersion)

	if currentGen != generation {
		return nil, ErrConflict
	}

	if !ok {
		return nil, ErrKeyNotFound
	}

	// FIX: Ghost Key Check. If recovery truncated the file, index might point to future/invalid offset.
	// Since we are reading, we need to know the actual limit.
	// Using s.offset is safe because we only append. If entry.Offset >= s.offset, it's invalid.
	if entry.Offset() >= atomic.LoadInt64(&s.offset) {
		return nil, ErrKeyNotFound
	}

	var header [HeaderSize]byte
	if _, err := s.journal.ReadAt(header[:], entry.Offset()); err != nil {
		return nil, fmt.Errorf("read header error: %w", err)
	}
	keyLen := binary.BigEndian.Uint32(header[0:4])
	valLen := binary.BigEndian.Uint32(header[4:8])
	if valLen == Tombstone {
		return nil, ErrKeyNotFound
	}
	payloadLen := int(keyLen) + int(valLen)
	bufPtr := getBuffer(payloadLen)
	defer putBuffer(bufPtr)

	if _, err := s.journal.ReadAt(*bufPtr, entry.Offset()+HeaderSize); err != nil {
		return nil, fmt.Errorf("read payload error: %w", err)
	}
	if !s.skipCrc {
		// Use Castagnoli Table
		if crc32.Checksum(*bufPtr, CrcTable) != binary.BigEndian.Uint32(header[8:12]) {
			return nil, ErrCrcMismatch
		}
	}
	val := (*bufPtr)[keyLen:]
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

func (s *Store) ApplyBatch(ops []bufferedOp, reads []string, readVersion int64, generation uint64) error {
	accessMap := make(map[string]bool, len(ops))
	var batchSize int64

	hasWrites := false
	for _, op := range ops {
		if op.opType != OpJournalGet {
			hasWrites = true
			break
		}
	}
	if !hasWrites {
		return nil
	}

	for _, op := range ops {
		isWrite := op.opType != OpJournalGet
		if isWrite {
			accessMap[op.key] = true
			batchSize += int64(len(op.key) + len(op.val) + HeaderSize)
		} else {
			if _, exists := accessMap[op.key]; !exists {
				accessMap[op.key] = false
			}
		}
	}

	if atomic.LoadInt64(&s.pendingWriteBytes)+batchSize > MaxPendingWriteBytes {
		return ErrBusy
	}

	atomic.AddInt64(&s.pendingWriteBytes, batchSize)

	req := &request{
		ops:         ops,
		reads:       reads,
		resp:        make(chan error, 1),
		readVersion: readVersion,
		generation:  generation,
		accessMap:   accessMap,
		batchSize:   batchSize,
	}

	// FIX: Use a reusable timer to prevent memory leaks caused by time.After
	timer := time.NewTimer(DefaultWriteTimeout)
	defer timer.Stop()

	select {
	case s.opsCh <- req:
		// Ensure timer is stopped or drained before reusing for response wait
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(DefaultWriteTimeout)

		select {
		case err := <-req.resp:
			return err
		case <-timer.C:
			req.cancelled.Store(true)
			atomic.AddInt64(&s.pendingWriteBytes, -batchSize)
			return ErrTransactionTimeout
		}
	case <-timer.C:
		atomic.AddInt64(&s.pendingWriteBytes, -batchSize)
		return ErrBusy
	case <-s.done:
		atomic.AddInt64(&s.pendingWriteBytes, -batchSize)
		return ErrClosed
	}
}

func (s *Store) Stats() (int, string, int64, int, int64, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len(), time.Since(s.startTime).Round(time.Second).String(), atomic.LoadInt64(&s.pendingWriteBytes), len(s.activeSnapshots), s.offset, s.generation
}

func (s *Store) reopen() error {
	journalPath := filepath.Join(s.dataDir, DefaultDBName)
	boltPath := filepath.Join(s.dataDir, BoltDBName)

	j, err := OpenJournal(journalPath)
	if err != nil {
		return err
	}
	s.journal = j

	boltOpts := &bbolt.Options{Timeout: 1 * time.Second, NoSync: !s.useFsync}
	b, err := bbolt.Open(boltPath, 0600, boltOpts)
	if err != nil {
		return err
	}
	s.bolt = b
	s.index = NewIndex(b)

	return nil
}


package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

type Store struct {
	mu        sync.RWMutex
	journal   *JournalFile
	index     *Index
	bolt      *bbolt.DB
	startTime time.Time
	logger    *slog.Logger

	// Configuration
	allowTruncate bool
	skipCrc       bool
	useFsync      bool
	fsyncInterval time.Duration

	// State
	offset            int64
	minReadVersion    int64
	lastPrunedVersion int64
	generation        uint64
	compacting        bool
	compactionRunning atomic.Bool

	// Channels & Synchronization
	opsChannel chan *request
	done       chan struct{}
	wg         sync.WaitGroup
	flushWg    sync.WaitGroup

	// Conflict Detection
	activeSnapshots map[int64]int
	conflictIndex   map[string][]int64
	conflictQueue   []Mutation

	// Metrics & Paths
	dataDir               string
	compactionGracePeriod time.Duration
	pendingWriteBytes     int64
	compactionThreshold   int64
}

func findLatestGeneration(dataDir string) (uint64, error) {
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return 0, err
	}

	var maxGen uint64 = 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".db") && !strings.Contains(fileName, ".tmp") {
			genStr := strings.TrimSuffix(fileName, ".db")
			gen, err := strconv.ParseUint(genStr, 10, 64)
			// Skip files that don't match the format or fail parsing
			if err == nil && gen > maxGen {
				maxGen = gen
			}
		}
	}
	if maxGen == 0 {
		return 1, nil // Start with generation 1 if no db file is found
	}
	return maxGen, nil
}

func NewStore(dataDir string, logger *slog.Logger, allowTruncate bool, skipCrc bool, useFsync bool, fsyncInterval time.Duration) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to init data dir: %w", err)
	}

	gen, err := findLatestGeneration(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to find latest generation: %w", err)
	}

	journalPath := filepath.Join(dataDir, fmt.Sprintf("%d.db", gen))
	boltPath := filepath.Join(dataDir, fmt.Sprintf("%d.idx", gen))
	logger.Info("Opening store", "journal", journalPath, "bolt", boltPath, "fsync", useFsync, "generation", gen)

	journal, err := OpenJournal(journalPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open journal: %w", err)
	}

	boltOpts := &bbolt.Options{Timeout: 1 * time.Second, NoSync: !useFsync}
	boltDB, err := bbolt.Open(boltPath, 0o600, boltOpts)
	if err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("failed to open bolt: %w", err)
	}

	s := &Store{
		journal:               journal,
		bolt:                  boltDB,
		index:                 NewIndex(boltDB),
		conflictIndex:         make(map[string][]int64),
		conflictQueue:         make([]Mutation, 0, 1024),
		activeSnapshots:       make(map[int64]int),
		startTime:             time.Now(),
		logger:                logger,
		allowTruncate:         allowTruncate,
		skipCrc:               skipCrc,
		useFsync:              useFsync,
		fsyncInterval:         fsyncInterval,
		opsChannel:            make(chan *request, 5000),
		done:                  make(chan struct{}),
		dataDir:               dataDir,
		compactionThreshold:   DefaultCompactionThreshold,
		compactionGracePeriod: CompactionGracePeriod,
		generation:            gen,
	}

	if err := s.recover(); err != nil {
		_ = boltDB.Close()
		_ = journal.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	s.minReadVersion = s.offset
	s.lastPrunedVersion = s.offset

	s.wg.Add(1)
	go s.runLoop()

	return s, nil
}

func (s *Store) recover() error {
	info, err := s.journal.f.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		s.logger.Info("Initializing new journal", "generation", s.generation)
		s.offset = 0
	} else {
		s.logger.Info("Recovered generation", "gen", s.generation)
	}

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
		return err
	}

	if startOffset < 0 {
		startOffset = 0
	}

	if !lastOffsetFound {
		s.logger.Warn("Rebuilding index from scratch...")
		startOffset = 0
	}

	if _, err := s.journal.Seek(startOffset, 0); err != nil {
		return err
	}

	header := make([]byte, HeaderSize)
	var payload []byte
	validOffset := startOffset
	count := 0

	s.logger.Info("Starting recovery scan", "start_offset", startOffset)

	for {
		var scratch [8]byte

		if _, err := io.ReadFull(s.journal.f, header); err != nil {
			if err == io.EOF {
				break
			}
			if !s.allowTruncate {
				return err
			}
			s.logger.Warn("Truncating corrupt header", "offset", validOffset)
			return s.truncate(validOffset)
		}

		keyLen := binary.BigEndian.Uint32(header[0:4])
		valLen := binary.BigEndian.Uint32(header[4:8])
		storedCrc := binary.BigEndian.Uint32(header[16:20])

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		if payloadLen > int64(MaxKeySize+MaxValueSize) {
			return fmt.Errorf("corruption: massive payload length at %d", validOffset)
		}

		payload = make([]byte, payloadLen)
		if _, err := io.ReadFull(s.journal.f, payload); err != nil {
			if !s.allowTruncate {
				return err
			}
			s.logger.Warn("Truncating corrupt payload", "offset", validOffset)
			return s.truncate(validOffset)
		}

		if !s.skipCrc {
			digest := crc32.New(CrcTable)
			binary.BigEndian.PutUint32(scratch[0:4], keyLen)
			binary.BigEndian.PutUint32(scratch[4:8], valLen)
			digest.Write(scratch[:])
			digest.Write(payload)

			if digest.Sum32() != storedCrc {
				if !s.allowTruncate {
					return fmt.Errorf("crc mismatch at %d", validOffset)
				}
				s.logger.Warn("CRC mismatch. Truncating.", "offset", validOffset)
				return s.truncate(validOffset)
			}
		}

		key := string(payload[:keyLen])
		isDel := valLen == Tombstone
		entrySize := int64(HeaderSize) + payloadLen

		s.index.Set(key, validOffset, entrySize, isDel, 0)

		validOffset += entrySize
		count++

		if count%10000 == 0 {
			s.logger.Info("Recovery progress", "entries", count, "offset", validOffset)
		}
	}

	s.logger.Info("Recovery complete", "entries", count, "final_offset", validOffset)
	s.offset = validOffset

	// Synchronous Flush Logic for Correctness
	if !lastOffsetFound && count > 0 {
		s.logger.Info("Flushing recovered index to BoltDB (Synchronous)...")
		if s.index.Rotate() {
			if err := s.index.FlushToBolt(0); err != nil {
				return err
			}
			s.index.FinishFlush()
		}
	}

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
		_ = s.bolt.Close()
	}
	if s.journal != nil {
		return s.journal.Close()
	}
	return nil
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
			s.flushBatch(pending, &batchBuf)
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

		case req := <-s.opsChannel:
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

			s.serializeBatch(req, &batchBuf)
			pending = append(pending, req)

			if len(pending) >= MaxBatchSize || batchBuf.Len() >= MaxBatchBytes {
				flush()
			}

		case <-batchTicker.C:
			flush()
		}
	}
}

// flushBatch handles the complex logic of writing to the journal (IO)
// and updating the in-memory index (Logic) securely.
func (s *Store) flushBatch(pending []*request, buf *bytes.Buffer) {
	// 1. Filter cancelled requests
	activeReqs := pending[:0]
	for _, req := range pending {
		if req.cancelled.Load() {
			// Notify cancelled
			select {
			case req.resp <- errors.New("cancelled"):
			default:
			}
			continue
		}
		activeReqs = append(activeReqs, req)
	}

	if len(activeReqs) == 0 {
		buf.Reset()
		return
	}

	// 2. IO PHASE: Write to Journal
	// Use RLock prevents Compaction (STW) but allows concurrent reads
	s.mu.RLock()
	if len(activeReqs) > 0 && s.generation != activeReqs[0].generation {
		s.mu.RUnlock()
		s.failBatch(activeReqs, ErrConflict)
		buf.Reset()
		return
	}

	writeOffset := s.offset
	if _, err := s.journal.WriteAt(buf.Bytes(), writeOffset); err != nil {
		s.mu.RUnlock()
		s.failBatch(activeReqs, err)
		buf.Reset()
		return
	}

	if s.useFsync {
		if err := s.journal.Sync(); err != nil {
			s.mu.RUnlock()
			s.failBatch(activeReqs, err)
			buf.Reset()
			return
		}
	}
	s.mu.RUnlock()

	// 3. INDEX UPDATE PHASE: Update Memory
	// Must hold exclusive Lock to update the Index and Offset safely
	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-check generation in case compaction happened between Unlock and Lock
	if len(activeReqs) > 0 && s.generation != activeReqs[0].generation {
		s.failBatch(activeReqs, ErrConflict)
		buf.Reset()
		return
	}

	currentOffset := s.offset
	minVer := s.getMinReadVersion()

	for _, req := range activeReqs {
		for i := range req.ops {
			op := &req.ops[i]
			if op.opType == OpJournalGet {
				s.conflictIndex[op.key] = append(s.conflictIndex[op.key], s.offset+1) // +1 simulates future read
				continue
			}
			opLen := req.opLens[i]
			reqOffset := currentOffset
			currentOffset += int64(opLen)

			isDel := op.opType == OpJournalDelete

			// Update Index
			s.index.Set(op.key, reqOffset, int64(opLen), isDel, minVer)

			// Update Conflict Detection
			s.conflictIndex[op.key] = append(s.conflictIndex[op.key], reqOffset)
			s.conflictQueue = append(s.conflictQueue, Mutation{Key: op.key, Offset: reqOffset})
		}

		// Success Response
		select {
		case req.resp <- nil:
		default:
		}
		atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
	}

	s.offset = currentOffset

	if minVer > s.lastPrunedVersion {
		s.pruneMutations(minVer)
		s.lastPrunedVersion = minVer
	}

	buf.Reset()
}

func (s *Store) failBatch(reqs []*request, err error) {
	for _, req := range reqs {
		select {
		case req.resp <- err:
		default:
		}
		atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
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
	return s.minReadVersion
}

func (s *Store) AcquireSnapshot() (int64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ver := s.offset
	gen := s.generation
	s.activeSnapshots[ver]++
	if ver < s.minReadVersion {
		s.minReadVersion = ver
	}
	return ver, gen
}

func (s *Store) ReleaseSnapshot(readVersion int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count, ok := s.activeSnapshots[readVersion]; ok {
		s.activeSnapshots[readVersion] = count - 1
		if s.activeSnapshots[readVersion] <= 0 {
			delete(s.activeSnapshots, readVersion)
			if readVersion == s.minReadVersion {
				s.minReadVersion = s.offset
				for v := range s.activeSnapshots {
					if v < s.minReadVersion {
						s.minReadVersion = v
					}
				}
			}
		}
	}
}

func (s *Store) hasConflict(req *request, pending []*request) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, pReq := range pending {
		for _, op := range pReq.ops {
			if _, exists := req.accessMap[op.key]; exists {
				return true
			}
		}
	}

	for key := range req.accessMap {
		if recentMutations, exists := s.conflictIndex[key]; exists {
			for _, offset := range recentMutations {
				if offset >= req.readVersion {
					return true
				}
			}
		}
	}
	return false
}

func (s *Store) serializeBatch(req *request, buf *bytes.Buffer) {
	for i := range req.ops {
		op := &req.ops[i]
		if op.opType == OpJournalGet {
			req.opLens = append(req.opLens, 0)
			continue
		}
		startLen := buf.Len()

		// Write pre-calculated header
		// NOTE: We do not overwrite ReadVersion here to allow batching multiple transactions
		// with potentially different read versions (though typically generation check prevents mixed generations).
		buf.Write(op.header[:])
		buf.WriteString(op.key)
		if op.opType == OpJournalSet {
			buf.Write(op.val)
		}

		length := buf.Len() - startLen
		req.opLens = append(req.opLens, length)
	}
}

func (s *Store) ApplyBatch(ops []bufferedOp, readVersion int64, generation uint64) error {
	var batchSize int64
	accessMap := make(map[string]struct{}, len(ops))
	var scratch [8]byte

	for i := range ops {
		op := &ops[i]
		accessMap[op.key] = struct{}{}
		if op.opType != OpJournalGet {
			keyLen := len(op.key)
			valLen := uint32(len(op.val))
			if op.opType == OpJournalDelete {
				valLen = Tombstone
			}
			batchSize += int64(keyLen + int(len(op.val)) + HeaderSize)

			// Calculate CRC
			digest := crc32.New(CrcTable)
			binary.BigEndian.PutUint32(scratch[0:4], uint32(keyLen))
			binary.BigEndian.PutUint32(scratch[4:8], valLen)
			digest.Write(scratch[:])

			_, _ = io.WriteString(digest, op.key)
			if op.opType == OpJournalSet {
				digest.Write(op.val)
			}
			crc := digest.Sum32()

			// Pack Header
			binary.BigEndian.PutUint32(op.header[0:4], uint32(keyLen))
			binary.BigEndian.PutUint32(op.header[4:8], valLen)
			binary.BigEndian.PutUint64(op.header[8:16], uint64(readVersion))
			binary.BigEndian.PutUint32(op.header[16:20], crc)
		}
	}

	if atomic.LoadInt64(&s.pendingWriteBytes)+batchSize > MaxPendingWriteBytes {
		return ErrBusy
	}
	atomic.AddInt64(&s.pendingWriteBytes, batchSize)

	req := &request{
		ops:         ops,
		resp:        make(chan error, 1),
		readVersion: readVersion,
		generation:  generation,
		accessMap:   accessMap,
		batchSize:   batchSize,
	}

	timer := time.NewTimer(DefaultWriteTimeout)
	defer timer.Stop()

	select {
	case s.opsChannel <- req:
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
			// don't decrement pending bytes here, runLoop handles it
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

func (s *Store) pruneMutations(minReadVersion int64) {
	prunedCount := 0
	for _, m := range s.conflictQueue {
		if m.Offset >= minReadVersion {
			break
		}

		if offsets, ok := s.conflictIndex[m.Key]; ok && len(offsets) > 0 {
			if offsets[0] == m.Offset {
				s.conflictIndex[m.Key] = offsets[1:]
				if len(s.conflictIndex[m.Key]) == 0 {
					delete(s.conflictIndex, m.Key)
				}
			}
		}
		prunedCount++
	}

	if prunedCount > 0 {
		s.conflictQueue = s.conflictQueue[prunedCount:]
	}
}

func (s *Store) drainChannels() {
	for {
		select {
		case req := <-s.opsChannel:
			select {
			case req.resp <- ErrClosed:
			default:
			}
			atomic.AddInt64(&s.pendingWriteBytes, -req.batchSize)
		default:
			return
		}
	}
}

func (s *Store) Get(key string, readVersion int64, generation uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.generation != generation {
		return nil, ErrConflict
	}

	entry, ok := s.index.Get(key, readVersion)
	if !ok {
		return nil, ErrKeyNotFound
	}

	if entry.Offset() >= s.offset {
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
	payload := make([]byte, payloadLen)

	if _, err := s.journal.ReadAt(payload, entry.Offset()+HeaderSize); err != nil {
		return nil, fmt.Errorf("read payload error: %w", err)
	}
	if !s.skipCrc {
		digest := crc32.New(CrcTable)
		var scratch [8]byte
		binary.BigEndian.PutUint32(scratch[0:4], keyLen)
		binary.BigEndian.PutUint32(scratch[4:8], valLen)
		digest.Write(scratch[:])
		digest.Write(payload)

		if digest.Sum32() != binary.BigEndian.Uint32(header[16:20]) {
			return nil, ErrCrcMismatch
		}
	}
	val := payload[keyLen:]
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

// Sync reads raw log entries.
func (s *Store) Sync(reqGen uint64, reqOffset int64) (io.ReadCloser, int64, uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. If client is on current generation, normal read.
	if s.generation == reqGen {
		return s.readSyncFromCurrent(reqOffset)
	}

	// 2. If client is on previous generation, try to help them finish it.
	if reqGen == s.generation-1 {
		return s.readSyncFromPrevious(reqGen, reqOffset)
	}

	s.logger.Warn("Sync: Generation mismatch", "req_gen", reqGen, "server_gen", s.generation)
	return nil, 0, s.generation, ErrGenerationMismatch
}

func (s *Store) getBatchLength(startOffset int64, maxBytes int64) (int64, error) {
	var totalBytes int64
	nextOffset := startOffset

	// Lock holding Note: We hold s.mu.RLock from Sync caller.
	// Accessing BoltDB View is safe.
	err := s.bolt.View(func(tx *bbolt.Tx) error {
		// 1. Check BoltDB (Cold Storage)
		var c *bbolt.Cursor
		if b := tx.Bucket([]byte(BoltBucketPair)); b != nil {
			c = b.Cursor()
		}

		if c != nil {
			var kBuf [8]byte
			binary.BigEndian.PutUint64(kBuf[:], uint64(nextOffset))
			k, v := c.Seek(kBuf[:])

			// If found in BoltDB, iterate there first
			if k != nil && bytes.Equal(k, kBuf[:]) {
				for totalBytes < maxBytes {
					length := int64(binary.BigEndian.Uint64(v))
					totalBytes += length
					nextOffset += length

					k, v = c.Next()
					if k == nil {
						break // End of bucket
					}

					binary.BigEndian.PutUint64(kBuf[:], uint64(nextOffset))
					if !bytes.Equal(k, kBuf[:]) {
						break // Gap in BoltDB
					}
				}
			}
		}

		// 2. Check Memory Maps (Warm & Hot Storage)
		for totalBytes < maxBytes {
			var length int64
			found := false

			// Check Flushing (Warm)
			if s.index.flushing != nil {
				if l, ok := s.index.flushing.pairLengths[nextOffset]; ok {
					length = l
					found = true
				}
			}

			// Check Active (Hot)
			if !found {
				if l, ok := s.index.active.pairLengths[nextOffset]; ok {
					length = l
					found = true
				}
			}

			if !found {
				break // End of chain
			}

			totalBytes += length
			nextOffset += length
		}

		return nil
	})

	return totalBytes, err
}

func (s *Store) readSyncFromCurrent(reqOffset int64) (io.ReadCloser, int64, uint64, error) {
	if reqOffset < 0 {
		return nil, 0, s.generation, fmt.Errorf("invalid negative offset: %d", reqOffset)
	}

	currentOffset := s.offset
	if reqOffset > currentOffset {
		return nil, 0, s.generation, fmt.Errorf("client offset %d ahead of server %d", reqOffset, currentOffset)
	}

	if reqOffset == currentOffset {
		return nil, currentOffset, s.generation, nil
	}

	// Calculate batch size
	bytesToRead, err := s.getBatchLength(reqOffset, MaxSyncBytes)
	if err != nil {
		return nil, 0, s.generation, err
	}

	if bytesToRead == 0 {
		// Data exists in journal but not in index (corruption or race)
		return nil, 0, s.generation, fmt.Errorf("sync error: offset %d found in journal but missing from index", reqOffset)
	}

	// Create a SectionReader. Wrap in NopCloser to keep underlying file open.
	r := io.NewSectionReader(s.journal, reqOffset, bytesToRead)
	return io.NopCloser(r), reqOffset + bytesToRead, s.generation, nil
}

func (s *Store) readSyncFromPrevious(gen uint64, offset int64) (io.ReadCloser, int64, uint64, error) {
	dbPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", gen))
	idxPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx", gen))

	f, err := os.Open(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, s.generation, ErrGenerationMismatch
		}
		return nil, 0, s.generation, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, s.generation, err
	}

	if offset >= info.Size() {
		_ = f.Close()
		return nil, 0, s.generation, ErrGenerationMismatch
	}

	idxDB, err := bbolt.Open(idxPath, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 1 * time.Second})
	if err != nil {
		_ = f.Close()
		return nil, 0, s.generation, fmt.Errorf("failed to open prev index: %w", err)
	}
	defer func() { _ = idxDB.Close() }()

	var bytesToRead int64
	nextOffset := offset

	err = idxDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketPair))
		if b == nil {
			return nil
		}

		c := b.Cursor()
		var kBuf [8]byte
		binary.BigEndian.PutUint64(kBuf[:], uint64(nextOffset))
		k, v := c.Seek(kBuf[:])

		if k != nil && bytes.Equal(k, kBuf[:]) {
			for bytesToRead < MaxSyncBytes {
				length := int64(binary.BigEndian.Uint64(v))
				bytesToRead += length
				nextOffset += length

				k, v = c.Next()
				if k == nil {
					break
				}
				binary.BigEndian.PutUint64(kBuf[:], uint64(nextOffset))
				if !bytes.Equal(k, kBuf[:]) {
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		_ = f.Close()
		return nil, 0, s.generation, err
	}

	if bytesToRead == 0 {
		_ = f.Close()
		return nil, 0, s.generation, fmt.Errorf("corrupt index: offset %d not found", offset)
	}

	r := io.NewSectionReader(f, offset, bytesToRead)
	return &fileSectionReader{SectionReader: r, f: f}, offset + bytesToRead, gen, nil
}

type fileSectionReader struct {
	*io.SectionReader
	f *os.File
}

func (r *fileSectionReader) Close() error {
	return r.f.Close()
}

func (s *Store) SetReplicationState(gen uint64, offset int64) error {
	return s.bolt.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(BoltBucketMeta))
		if err != nil {
			return err
		}
		val := fmt.Sprintf("%d:%d", gen, offset)
		return b.Put([]byte(KeyReplState), []byte(val))
	})
}

func (s *Store) GetReplicationState() (uint64, int64, error) {
	var gen uint64
	var offset int64
	err := s.bolt.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BoltBucketMeta))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(KeyReplState))
		if v == nil {
			return nil
		}
		_, err := fmt.Sscanf(string(v), "%d:%d", &gen, &offset)
		return err
	})
	return gen, offset, err
}

func (s *Store) Stats() (int, string, int64, int, int64, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.Len(), time.Since(s.startTime).Round(time.Second).String(), atomic.LoadInt64(&s.pendingWriteBytes), len(s.activeSnapshots), s.offset, s.generation
}

func (s *Store) reopen() error {
	journalPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", s.generation))
	boltPath := filepath.Join(s.dataDir, fmt.Sprintf("%d.idx", s.generation))

	j, err := OpenJournal(journalPath)
	if err != nil {
		return err
	}
	s.journal = j

	boltOpts := &bbolt.Options{Timeout: 1 * time.Second, NoSync: !s.useFsync}
	b, err := bbolt.Open(boltPath, 0o600, boltOpts)
	if err != nil {
		return err
	}
	s.bolt = b
	s.index = NewIndex(b)
	return nil
}

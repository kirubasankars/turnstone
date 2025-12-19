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
	"strconv"
	"strings"
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
	useFsync      bool
	fsyncInterval time.Duration

	offset            int64
	minReadVersion    int64
	lastPrunedVersion int64
	generation        uint64

	// locking states
	compacting        bool
	compactionRunning atomic.Bool
	flushWg           sync.WaitGroup

	dataDir string

	activeSnapshots map[int64]int

	opsChannel chan *request
	done       chan struct{}
	wg         sync.WaitGroup

	conflictIndex map[string][]int64
	conflictQueue []Mutation

	pendingWriteBytes   int64
	compactionThreshold int64
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

	idx := NewIndex(boltDB)

	s := &Store{
		journal:             journal,
		bolt:                boltDB,
		index:               idx,
		conflictIndex:       make(map[string][]int64),
		conflictQueue:       make([]Mutation, 0, 1024),
		activeSnapshots:     make(map[int64]int),
		startTime:           time.Now(),
		logger:              logger,
		allowTruncate:       allowTruncate,
		skipCrc:             skipCrc,
		useFsync:            useFsync,
		fsyncInterval:       fsyncInterval,
		opsChannel:          make(chan *request, 5000),
		done:                make(chan struct{}),
		dataDir:             dataDir,
		compactionThreshold: DefaultCompactionThreshold,
		generation:          gen,
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

	bufReader := bufio.NewReader(s.journal.f)
	header := make([]byte, HeaderSize)
	payloadBuf := make([]byte, 4096)

	validOffset := startOffset
	count := 0

	for {
		if _, err := io.ReadFull(bufReader, header); err != nil {
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
		storedCrc := binary.BigEndian.Uint32(header[8:12])

		var payloadLen int64
		if valLen == Tombstone {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		if payloadLen > int64(MaxKeySize+MaxValueSize) {
			return fmt.Errorf("corruption: massive payload length")
		}

		if int64(cap(payloadBuf)) < payloadLen {
			payloadBuf = make([]byte, payloadLen)
		}
		payload := payloadBuf[:payloadLen]

		if _, err := io.ReadFull(bufReader, payload); err != nil {
			if !s.allowTruncate {
				return err
			}
			s.logger.Warn("Truncating corrupt payload", "offset", validOffset)
			return s.truncate(validOffset)
		}

		if !s.skipCrc {
			if crc32.Checksum(payload, CrcTable) != storedCrc {
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
	}

	s.offset = validOffset
	if !lastOffsetFound && count > 0 {
		s.triggerBoltFlush()
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

func (s *Store) flushJournal(pending []*request, batchBuf *bytes.Buffer) {
	if len(pending) == 0 {
		return
	}
	if err := s.flushBatch(pending, batchBuf); err != nil {
		s.logger.Error("Journal Flush failed", "err", err)
		for _, req := range pending {
			select {
			case req.resp <- err:
			default:
			}
		}
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
	for _, op := range req.ops {
		if op.opType == OpJournalGet {
			req.opLens = append(req.opLens, 0)
			continue
		}
		startLen := buf.Len()

		buf.Write(op.header[:])

		buf.WriteString(op.key)
		if op.opType == OpJournalSet {
			buf.Write(op.val)
		}

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

	// FIX: Use RLock for IO to prevent race with Compaction (Stop-The-World)
	// Compaction grabs Lock, so RLock here prevents it from running during WriteAt.
	s.mu.RLock()

	// Verify generation again to ensure we aren't writing old data to new file
	if len(pending) > 0 && s.generation != pending[0].generation {
		s.mu.RUnlock()
		return ErrConflict
	}

	writeOffset := s.offset

	if _, err := s.journal.WriteAt(buf.Bytes(), writeOffset); err != nil {
		s.mu.RUnlock()
		return err
	}

	if s.useFsync {
		if err := s.journal.Sync(); err != nil {
			s.mu.RUnlock()
			return err
		}
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Re-verify generation in case compaction happened in the tiny window between RUnlock and Lock
	if len(pending) > 0 && s.generation != pending[0].generation {
		// Data was written to the OLD file (validly), but generation switched.
		// We cannot update the index to point to the old file/offset.
		return ErrConflict
	}

	currentOffset := s.offset
	minVer := s.getMinReadVersion()

	for _, req := range pending {
		for i, op := range req.ops {
			if op.opType == OpJournalGet {
				s.conflictIndex[op.key] = append(s.conflictIndex[op.key], s.offset+1)
				continue
			}
			opLen := req.opLens[i]
			reqOffset := currentOffset
			currentOffset += int64(opLen)

			isDel := op.opType == OpJournalDelete
			s.index.Set(op.key, reqOffset, isDel, minVer)

			s.conflictIndex[op.key] = append(s.conflictIndex[op.key], reqOffset)
			s.conflictQueue = append(s.conflictQueue, Mutation{Key: op.key, Offset: reqOffset})
		}
		select {
		case req.resp <- nil:
		default:
		}
	}
	s.offset = currentOffset

	if minVer > s.lastPrunedVersion {
		s.pruneMutations(minVer)
		s.lastPrunedVersion = minVer
	}
	return nil
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
			req.resp <- ErrClosed
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
	// Optimization: Direct read, no atomic needed as we hold RLock
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
	bufPtr := getBuffer(payloadLen)
	defer putBuffer(bufPtr)

	if _, err := s.journal.ReadAt(*bufPtr, entry.Offset()+HeaderSize); err != nil {
		return nil, fmt.Errorf("read payload error: %w", err)
	}
	if !s.skipCrc {
		if crc32.Checksum(*bufPtr, CrcTable) != binary.BigEndian.Uint32(header[8:12]) {
			return nil, ErrCrcMismatch
		}
	}
	val := (*bufPtr)[keyLen:]
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

// Sync reads raw log entries.
// It supports reading from the previous generation to allow seamless handover during compaction.
func (s *Store) Sync(reqGen uint64, reqOffset int64) ([]byte, int64, uint64, error) {
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

	// 3. Otherwise, they are too far behind or ahead.
	return nil, 0, s.generation, ErrGenerationMismatch
}

func (s *Store) readSyncFromCurrent(reqOffset int64) ([]byte, int64, uint64, error) {
	if reqOffset < 0 {
		return nil, 0, s.generation, fmt.Errorf("invalid negative offset: %d", reqOffset)
	}

	currentOffset := s.offset // No atomic needed, we hold RLock
	if reqOffset > currentOffset {
		return nil, 0, s.generation, fmt.Errorf("client offset %d ahead of server %d", reqOffset, currentOffset)
	}

	available := currentOffset - reqOffset
	if available == 0 {
		return nil, currentOffset, s.generation, nil
	}

	bytesToRead := available
	if bytesToRead > MaxSyncBytes {
		bytesToRead = MaxSyncBytes
	}

	data := make([]byte, bytesToRead)
	n, err := s.journal.ReadAt(data, reqOffset)
	if err != nil && err != io.EOF {
		return nil, 0, s.generation, err
	}

	// Shrink buffer to what was actually read
	data = data[:n]

	// Ensure we don't send a partial entry
	data = trimBatch(data)

	return data, reqOffset + int64(len(data)), s.generation, nil
}

// readSyncFromPrevious attempts to read from a closed journal file.
// If EOF is reached, it signals ErrGenerationSwitch.
func (s *Store) readSyncFromPrevious(gen uint64, offset int64) ([]byte, int64, uint64, error) {
	path := filepath.Join(s.dataDir, fmt.Sprintf("%d.db", gen))

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Old file deleted? Too slow client. Hard Reset.
			return nil, 0, s.generation, ErrGenerationMismatch
		}
		return nil, 0, s.generation, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, 0, s.generation, err
	}

	// If client is already at EOF of old file, time to switch
	if offset >= info.Size() {
		return nil, 0, s.generation, ErrGenerationSwitch
	}

	available := info.Size() - offset
	bytesToRead := available
	if bytesToRead > MaxSyncBytes {
		bytesToRead = MaxSyncBytes
	}

	data := make([]byte, bytesToRead)
	n, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, 0, s.generation, err
	}

	// Shrink buffer to what was actually read
	data = data[:n]

	// Ensure we don't send a partial entry
	data = trimBatch(data)

	return data, offset + int64(len(data)), gen, nil
}

func (s *Store) ApplyBatch(ops []bufferedOp, readVersion int64, generation uint64) error {
	var batchSize int64
	accessMap := make(map[string]struct{}, len(ops))

	// Scan to build AccessMap (Reads+Writes) and calculate Write payload size.
	// We also pre-calculate the header (incl. CRC) here to offload the single-writer runloop.
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
			io.WriteString(digest, op.key)
			if op.opType == OpJournalSet {
				digest.Write(op.val)
			}
			crc := digest.Sum32()

			// Pack Header directly into the bufferedOp
			binary.BigEndian.PutUint32(op.header[0:4], uint32(keyLen))
			binary.BigEndian.PutUint32(op.header[4:8], valLen)
			binary.BigEndian.PutUint32(op.header[8:12], crc)
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

// trimBatch slices the data buffer at the last complete entry boundary.
// It assumes the buffer starts with a valid entry header.
func trimBatch(data []byte) []byte {
	var validBytes int
	total := len(data)
	pos := 0

	for pos < total {
		// Need at least header size to read length
		if total-pos < HeaderSize {
			break
		}

		keyLen := binary.BigEndian.Uint32(data[pos : pos+4])
		valLen := binary.BigEndian.Uint32(data[pos+4 : pos+8])

		var payloadLen int
		if valLen == Tombstone {
			payloadLen = int(keyLen)
		} else {
			payloadLen = int(keyLen) + int(valLen)
		}

		entrySize := HeaderSize + payloadLen

		if pos+entrySize > total {
			break // Partial entry at end
		}

		pos += entrySize
		validBytes = pos
	}
	return data[:validBytes]
}

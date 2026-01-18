package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CheckpointInterval = 512 * 1024 * 1024
	ReplicationTimeout = 30 * time.Second
	SlowOpThreshold    = 500 * time.Millisecond
)

type Store struct {
	// Synchronization & System
	mu        sync.RWMutex
	wg        sync.WaitGroup
	done      chan struct{}
	closing   atomic.Bool
	startTime time.Time
	logger    *slog.Logger

	// Components
	wal   *WAL
	index Index

	// Configuration
	allowTruncate bool
	minReplicas   int
	fsyncEnabled  bool
	dataDir       string

	// Storage State (Protected by mu)
	offset          int64
	nextTxID        uint64 // Previously NextLSN
	nextLogSeq      uint64 // Previously NextLogID
	minReadTxID     uint64 // Previously MinReadLSN
	compactedOffset int64
	checkpoints     []Checkpoint
	activeSnapshots map[uint64]int

	// Replication State
	replicationSlots map[string]ReplicaState
	ackCond          *sync.Cond

	// Metrics
	conflictCount          int64
	recoveryDuration       time.Duration
	bytesWritten           int64
	bytesRead              int64
	slowOps                int64
	lastCompactionDuration time.Duration

	// Operation Handling
	opsChannel        chan *request
	compactionChannel chan struct{}
	reqPool           *sync.Pool
}

func NewStore(dataDir string, logger *slog.Logger, allowTruncate bool, minReplicas int, fsyncEnabled bool) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to init data dir: %w", err)
	}

	wal, err := OpenWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	idx, err := NewIndex(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	s := &Store{
		wal:               wal,
		index:             idx,
		minReplicas:       0, // Initialize to 0 to allow bootstrap writes (like _id) without hanging
		fsyncEnabled:      fsyncEnabled,
		allowTruncate:     allowTruncate,
		dataDir:           dataDir,
		startTime:         time.Now(),
		logger:            logger,
		opsChannel:        make(chan *request, 5000),
		compactionChannel: make(chan struct{}, 1),
		done:              make(chan struct{}),
		checkpoints:       make([]Checkpoint, 0),
		activeSnapshots:   make(map[uint64]int),
		replicationSlots:  make(map[string]ReplicaState),
		nextTxID:          1,
		nextLogSeq:        1,
		reqPool: &sync.Pool{
			New: func() any {
				return &request{resp: make(chan error, 1), opLengths: make([]int, 0, 16)}
			},
		},
	}
	s.ackCond = sync.NewCond(&s.mu)

	if err := s.recover(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	s.minReadTxID = s.nextTxID - 1
	s.wg.Add(1)
	go s.runLoop()

	// Initialize _id (GUID) if missing
	// This write will proceed immediately because s.minReplicas is 0
	if _, err := s.Get("_id", atomic.LoadUint64(&s.nextTxID)); err == ErrKeyNotFound {
		uuid := make([]byte, 16)
		if _, err := rand.Read(uuid); err == nil {
			id := fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
			// Write directly to store via internal batch
			s.ApplyBatch([]bufferedOp{{
				opType: OpJournalSet,
				key:    "_id",
				val:    []byte(id),
			}}, 0)
			s.logger.Info("Generated new database ID", "id", id)
		}
	}

	// Set the actual configured MinReplicas now that bootstrap is done
	s.mu.Lock()
	s.minReplicas = minReplicas
	s.mu.Unlock()

	return s, nil
}

func (s *Store) Close() error {
	s.closing.Store(true)
	s.mu.Lock()
	s.ackCond.Broadcast()
	s.mu.Unlock()
	close(s.done)
	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.index.Close()
	return s.wal.Close()
}

// runLoop implements the Single Writer Principle.
func (s *Store) runLoop() {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("CRITICAL: Storage loop panicked", "err", r, "stack", string(debug.Stack()))
			os.Exit(1)
		}
	}()

	var batchBuf bytes.Buffer
	pending := make([]*request, 0, MaxBatchSize)
	batchTicker := time.NewTicker(BatchDelay)
	maintTicker := time.NewTicker(10 * time.Second)
	defer batchTicker.Stop()
	defer maintTicker.Stop()

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

		case req := <-s.opsChannel:
			if req.cancelled.Load() {
				continue
			}
			if s.hasConflict(req, pending) {
				atomic.AddInt64(&s.conflictCount, 1)
				req.resp <- ErrConflict
				continue
			}
			s.serializeBatch(req, &batchBuf)
			pending = append(pending, req)
			if len(pending) >= MaxBatchSize || batchBuf.Len() >= MaxBatchBytes {
				flush()
			}

		case <-batchTicker.C:
			flush()

		case <-maintTicker.C:
			s.handleMaintenance()

		case <-s.compactionChannel:
			flush()
			s.compactLogSync()
		}
	}
}

func (s *Store) handleMaintenance() {
	s.PruneStaleReplicas()

	s.mu.RLock()
	safeTxID := s.minReadTxID
	s.mu.RUnlock()

	count, err := s.index.OffloadColdKeys(safeTxID)
	if err != nil {
		s.logger.Error("Offload failed", "err", err)
	} else if count > 0 {
		s.logger.Debug("Offloaded cold keys", "count", count)
	}
}

// compactLogSync performs Stop-the-World compaction.
func (s *Store) compactLogSync() {
	s.logger.Info("Starting Stop-the-World Compaction...")
	start := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	safeEndOffset := s.offset - (10 * 1024 * 1024)
	startOffset := s.compactedOffset

	if safeEndOffset <= startOffset {
		s.logger.Info("Not enough data to compact")
		return
	}

	chunkSize := int64(4 * 1024 * 1024)
	buffer := make([]byte, chunkSize)
	currentRead := startOffset
	var moves []IndexUpdate
	var walBuffer bytes.Buffer
	var bytesReclaimed int64

	for currentRead < safeEndOffset {
		n, err := s.wal.ReadAt(buffer, currentRead)
		if err != nil && err != io.EOF {
			s.logger.Error("Compaction Read Error", "err", err)
			return
		}
		if n == 0 {
			break
		}

		parseOff := 0
		for parseOff < n {
			if parseOff+HeaderSize > n {
				break
			}
			// Skip Holes
			if buffer[parseOff] == 0 && buffer[parseOff+1] == 0 && buffer[parseOff+2] == 0 {
				aligned := (parseOff/4096 + 1) * 4096
				if aligned >= n {
					break
				}
				parseOff = aligned
				continue
			}

			packed := binary.BigEndian.Uint32(buffer[parseOff : parseOff+4])
			keyLen, valLen, isDel := UnpackMeta(packed)
			payloadLen := int(keyLen)
			if !isDel {
				payloadLen += int(valLen)
			}
			if parseOff+HeaderSize+payloadLen > n {
				break
			}

			key := string(buffer[parseOff+24 : parseOff+24+int(keyLen)])
			originalOffset := currentRead + int64(parseOff)

			latest, exists := s.index.GetLatest(key)
			if exists && latest.Offset == originalOffset {
				oldTxID := binary.BigEndian.Uint64(buffer[parseOff+4 : parseOff+12])
				entryData := buffer[parseOff : parseOff+HeaderSize+payloadLen]

				// Re-stamp LogSeq
				binary.BigEndian.PutUint64(entryData[12:20], s.nextLogSeq)
				s.nextLogSeq++

				crc := crc32.Checksum(entryData[:20], crc32Table)
				crc = crc32.Update(crc, crc32Table, entryData[24:])
				binary.BigEndian.PutUint32(entryData[20:24], crc)

				newOffset := s.offset + int64(walBuffer.Len())

				moves = append(moves, IndexUpdate{
					Key:     key,
					Offset:  newOffset,
					Length:  int64(len(entryData)),
					TxID:    oldTxID,
					Deleted: isDel,
				})

				walBuffer.Write(entryData)
			} else {
				bytesReclaimed += int64(HeaderSize + payloadLen)
			}
			parseOff += HeaderSize + payloadLen
		}
		currentRead += chunkSize
	}

	if walBuffer.Len() > 0 {
		data := walBuffer.Bytes()
		_, err := s.wal.Write(data)
		if err != nil {
			s.logger.Error("Compaction Write Error", "err", err)
			return
		}

		if s.fsyncEnabled {
			s.wal.Sync()
		}

		s.offset += int64(len(data))
		s.bytesWritten += int64(len(data))

		if err := s.index.SetBatch(moves); err != nil {
			s.logger.Error("CRITICAL: Compaction Index Update Failed", "err", err)
		}
	}

	punchSize := currentRead - startOffset
	if punchSize > 0 {
		s.wal.PunchHole(startOffset, punchSize)
		s.compactedOffset = currentRead
	}

	s.lastCompactionDuration = time.Since(start)
	s.logger.Info("Compaction done", "reclaimed", bytesReclaimed, "moved_entries", len(moves), "duration", s.lastCompactionDuration)
}

func (s *Store) flushBatch(pending []*request, buf *bytes.Buffer) {
	start := time.Now()
	hasCancelled := false
	for _, req := range pending {
		if req.cancelled.Load() {
			hasCancelled = true
			break
		}
	}

	var activeReqs []*request
	if hasCancelled {
		activeReqs = make([]*request, 0, len(pending))
		buf.Reset()
		for _, req := range pending {
			if !req.cancelled.Load() {
				activeReqs = append(activeReqs, req)
				req.opLengths = req.opLengths[:0]
				s.serializeBatch(req, buf)
			}
		}
	} else {
		activeReqs = pending
	}

	if len(activeReqs) == 0 {
		buf.Reset()
		return
	}

	s.mu.Lock()
	currentTxID := s.nextTxID
	currentLogSeq := s.nextLogSeq
	startBatchLogSeq := currentLogSeq
	bufBytes := buf.Bytes()
	bufPtr := 0

	// 1. Stamp TxIDs and CRCs
	for _, req := range activeReqs {
		commitTxID := currentTxID
		if !req.isReplication {
			currentTxID++
		}

		for i := range req.ops {
			op := &req.ops[i]
			if op.opType == OpJournalGet {
				continue
			}
			opLen := req.opLengths[i]
			txIDToWrite := commitTxID
			if req.isReplication {
				txIDToWrite = binary.BigEndian.Uint64(op.header[4:12])
			}

			// Format: [Meta:4][TxID:8][LogSeq:8][CRC:4]
			binary.BigEndian.PutUint64(bufBytes[bufPtr+4:bufPtr+12], txIDToWrite)
			binary.BigEndian.PutUint64(bufBytes[bufPtr+12:bufPtr+20], currentLogSeq)
			currentLogSeq++

			crc := crc32.Checksum(bufBytes[bufPtr:bufPtr+20], crc32Table)
			crc = crc32.Update(crc, crc32Table, bufBytes[bufPtr+24:bufPtr+opLen])
			binary.BigEndian.PutUint32(bufBytes[bufPtr+20:bufPtr+24], crc)

			bufPtr += opLen
		}
	}

	// 2. Write to Disk
	writeOffset, err := s.wal.Write(bufBytes)
	if err != nil {
		s.mu.Unlock()
		s.failBatch(activeReqs, err)
		buf.Reset()
		return
	}
	atomic.AddInt64(&s.bytesWritten, int64(len(bufBytes)))

	if s.fsyncEnabled {
		if err := s.wal.Sync(); err != nil {
			s.mu.Unlock()
			s.failBatch(activeReqs, err)
			buf.Reset()
			return
		}
	}

	var lastCkptOffset int64
	if len(s.checkpoints) > 0 {
		lastCkptOffset = s.checkpoints[len(s.checkpoints)-1].Offset
	}
	if writeOffset-lastCkptOffset >= CheckpointInterval {
		s.checkpoints = append(s.checkpoints, Checkpoint{LogSeq: startBatchLogSeq, Offset: writeOffset})
	}
	s.mu.Unlock()

	// 3. Wait for Replication Quorum
	batchMaxLogSeq := currentLogSeq - 1
	if batchMaxLogSeq >= startBatchLogSeq {
		if err := s.WaitForQuorum(batchMaxLogSeq); err != nil {
			s.logger.Error("Replication quorum failed", "err", err)
		}
	}

	// 4. Update Index (Read View)
	s.mu.Lock()
	defer s.mu.Unlock()

	currentOffset := writeOffset
	indexTxID := s.nextTxID
	var indexUpdates []IndexUpdate

	for _, req := range activeReqs {
		reqTxID := indexTxID
		if !req.isReplication {
			indexTxID++
		}
		for i := range req.ops {
			op := &req.ops[i]
			if op.opType == OpJournalGet {
				continue
			}
			opLen := req.opLengths[i]
			entryTxID := reqTxID
			if req.isReplication {
				entryTxID = binary.BigEndian.Uint64(op.header[4:12])
				// Ensure local clock advances to cover replicated TxIDs so they are visible
				if entryTxID >= indexTxID {
					indexTxID = entryTxID + 1
				}
			}

			isDel := op.opType == OpJournalDelete
			indexUpdates = append(indexUpdates, IndexUpdate{
				Key:     op.key,
				Offset:  currentOffset,
				Length:  int64(opLen),
				TxID:    entryTxID,
				Deleted: isDel,
			})
			currentOffset += int64(opLen)
		}
		select {
		case req.resp <- nil:
		default:
		}
	}

	if len(indexUpdates) > 0 {
		if err := s.index.SetBatch(indexUpdates); err != nil {
			s.logger.Error("Index batch update failed", "err", err)
		}
	}

	s.nextTxID = indexTxID
	s.nextLogSeq = currentLogSeq
	s.offset = currentOffset
	s.recalcMinReadTxID()

	// Persist state including the current KeyCount to allow fast recovery of stats
	if err := s.index.PutState(s.nextTxID, s.nextLogSeq, s.offset, int64(s.index.Len())); err != nil {
		s.logger.Warn("Failed to persist state", "err", err)
	}

	buf.Reset()
	if dur := time.Since(start); dur > SlowOpThreshold {
		atomic.AddInt64(&s.slowOps, 1)
		s.logger.Warn("Slow Batch Write", "ops", len(activeReqs), "duration", dur)
	}
}

func (s *Store) failBatch(reqs []*request, err error) {
	for _, req := range reqs {
		select {
		case req.resp <- err:
		default:
		}
	}
}

func (s *Store) hasConflict(req *request, pending []*request) bool {
	// Replication batches are authoritative and should not conflict check against local state
	if req.isReplication {
		return false
	}

	for _, pReq := range pending {
		if !pReq.cancelled.Load() {
			for _, op := range pReq.ops {
				if _, exists := req.accessMap[op.key]; exists {
					return true
				}
			}
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key := range req.accessMap {
		latest, exists := s.index.GetHead(key)
		if exists && latest.TxID > req.readTxID {
			return true
		}
	}
	return false
}

func (s *Store) recover() error {
	start := time.Now()
	defer func() {
		s.mu.Lock()
		s.recoveryDuration = time.Since(start)
		s.mu.Unlock()
	}()

	size := s.wal.Size()
	s.logger.Info("Recovering index...", "file_size", size)

	var offset int64
	var maxTxID, maxLogSeq uint64

	// Optimization: Try to fast-forward using state persisted in LevelDB
	stateTxID, stateLogSeq, stateOffset, _, err := s.index.GetState()
	if err == nil && stateOffset <= size {
		s.logger.Info("Fast recovery engaged", "offset", stateOffset, "txID", stateTxID, "logSeq", stateLogSeq)
		offset = stateOffset
		if stateTxID > 0 {
			maxTxID = stateTxID - 1
		}
		if stateLogSeq > 0 {
			maxLogSeq = stateLogSeq - 1
		}
	} else {
		s.logger.Info("Performing full recovery scan", "reason", err)
		offset = 0
	}

	var batch []IndexUpdate
	const recoveryBatchSize = 10000

	header := make([]byte, HeaderSize)
	for offset < size {
		if _, err := s.wal.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				break
			}
			if !s.allowTruncate {
				return fmt.Errorf("read error at %d: %w", offset, err)
			}
			s.wal.Truncate(offset)
			break
		}

		isZero := true
		for _, b := range header {
			if b != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			offset = (offset/4096 + 1) * 4096
			continue
		}

		packed := binary.BigEndian.Uint32(header[0:4])
		keyLen, valLen, isDel := UnpackMeta(packed)
		entryTxID := binary.BigEndian.Uint64(header[4:12])
		entryLogSeq := binary.BigEndian.Uint64(header[12:20])
		storedCrc := binary.BigEndian.Uint32(header[20:24])

		payloadLen := int(keyLen)
		if !isDel {
			payloadLen += int(valLen)
		}

		if payloadLen < 0 || int64(offset+HeaderSize+int64(payloadLen)) > size {
			s.wal.Truncate(offset)
			break
		}

		payload := make([]byte, payloadLen)
		if _, err := s.wal.ReadAt(payload, offset+HeaderSize); err != nil {
			s.wal.Truncate(offset)
			break
		}

		crc := crc32.Checksum(header[:20], crc32Table)
		crc = crc32.Update(crc, crc32Table, payload)
		if crc != storedCrc {
			s.logger.Warn("CRC mismatch", "offset", offset)
			s.wal.Truncate(offset)
			break
		}

		key := string(payload[:keyLen])
		entrySize := int64(HeaderSize) + int64(payloadLen)

		batch = append(batch, IndexUpdate{
			Key:     key,
			Offset:  offset,
			Length:  entrySize,
			TxID:    entryTxID,
			Deleted: isDel,
		})

		if len(batch) >= recoveryBatchSize {
			if err := s.index.SetBatch(batch); err != nil {
				return fmt.Errorf("batch index error at offset %d: %w", offset, err)
			}
			batch = batch[:0]
		}

		if entryTxID > maxTxID {
			maxTxID = entryTxID
		}
		if entryLogSeq > maxLogSeq {
			maxLogSeq = entryLogSeq
		}

		if len(s.checkpoints) == 0 || offset-s.checkpoints[len(s.checkpoints)-1].Offset >= CheckpointInterval {
			s.checkpoints = append(s.checkpoints, Checkpoint{LogSeq: entryLogSeq, Offset: offset})
		}
		offset += entrySize
	}

	if len(batch) > 0 {
		if err := s.index.SetBatch(batch); err != nil {
			return fmt.Errorf("final batch index error: %w", err)
		}
	}

	s.offset = offset
	s.nextTxID = maxTxID + 1
	s.nextLogSeq = maxLogSeq + 1
	return nil
}

func (s *Store) AcquireSnapshot() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	txID := s.nextTxID - 1
	if txID < 0 {
		txID = 0
	}
	s.activeSnapshots[txID]++
	s.recalcMinReadTxID()
	return txID
}

func (s *Store) ReleaseSnapshot(readTxID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count, ok := s.activeSnapshots[readTxID]; ok {
		s.activeSnapshots[readTxID] = count - 1
		if s.activeSnapshots[readTxID] <= 0 {
			delete(s.activeSnapshots, readTxID)
			s.recalcMinReadTxID()
		}
	}
}

func (s *Store) recalcMinReadTxID() {
	min := s.nextTxID - 1
	for txID := range s.activeSnapshots {
		if txID < min {
			min = txID
		}
	}
	s.minReadTxID = min
}

func (s *Store) Get(key string, readTxID uint64) ([]byte, error) {
	s.mu.RLock()
	entry, ok := s.index.Get(key, readTxID)
	safeLimit := s.offset
	s.mu.RUnlock()

	if !ok || entry.Offset >= safeLimit {
		return nil, ErrKeyNotFound
	}

	headerBuf := make([]byte, HeaderSize)
	n1, err := s.wal.ReadAt(headerBuf, entry.Offset)
	if err != nil {
		return nil, err
	}

	packed := binary.BigEndian.Uint32(headerBuf[0:4])
	keyLen, valLen, isDel := UnpackMeta(packed)
	payloadLen := int(keyLen)
	if !isDel {
		payloadLen += int(valLen)
	}

	payload := make([]byte, payloadLen)
	n2, err := s.wal.ReadAt(payload, entry.Offset+HeaderSize)
	if err != nil {
		return nil, err
	}

	atomic.AddInt64(&s.bytesRead, int64(n1+n2))

	crc := crc32.Checksum(headerBuf[:20], crc32Table)
	crc = crc32.Update(crc, crc32Table, payload)
	if crc != binary.BigEndian.Uint32(headerBuf[20:24]) {
		return nil, ErrCrcMismatch
	}

	return payload[keyLen:], nil
}

func (s *Store) ApplyBatch(ops []bufferedOp, readTxID uint64) error {
	req := s.reqPool.Get().(*request)

	if cap(req.ops) < len(ops) {
		req.ops = make([]bufferedOp, len(ops))
	} else {
		req.ops = req.ops[:len(ops)]
	}
	copy(req.ops, ops)

	req.readTxID = readTxID
	req.accessMap = make(map[string]struct{}, len(ops))
	for i := range req.ops {
		op := &req.ops[i]
		req.accessMap[op.key] = struct{}{}
		if op.opType != OpJournalGet {
			kL := len(op.key)
			vL := len(op.val)
			isDel := op.opType == OpJournalDelete
			if isDel {
				vL = 0
			}
			binary.BigEndian.PutUint32(op.header[0:4], PackMeta(uint32(kL), uint32(vL), isDel))
		}
	}
	req.cancelled.Store(false)
	// Simplified: Compaction doesn't run through this path
	req.isReplication = false
	req.opLengths = req.opLengths[:0]

	select {
	case <-req.resp:
	default:
	}

	return s.submitReq(req)
}

func (s *Store) submitReq(req *request) error {
	defer s.reqPool.Put(req)
	timer := time.NewTimer(DefaultWriteTimeout)
	defer timer.Stop()

	select {
	case s.opsChannel <- req:
		select {
		case err := <-req.resp:
			return err
		case <-timer.C:
			req.cancelled.Store(true)
			return ErrTransactionTimeout
		}
	case <-timer.C:
		return ErrBusy
	case <-s.done:
		return ErrClosed
	}
}

func (s *Store) ReplicateBatch(entries []LogEntry) error {
	if s.index.SizeBytes() >= MaxIndexBytes {
		return ErrMemoryLimitExceeded
	}

	ops := make([]bufferedOp, len(entries))
	for i, entry := range entries {
		ops[i] = bufferedOp{
			opType: int(entry.OpType),
			key:    string(entry.Key),
			val:    entry.Value,
		}
		binary.BigEndian.PutUint64(ops[i].header[4:12], entry.TxID)
		kLen := len(entry.Key)
		vLen := len(entry.Value)
		isDel := entry.OpType == OpJournalDelete
		if isDel {
			vLen = 0
		}
		packed := PackMeta(uint32(kLen), uint32(vLen), isDel)
		binary.BigEndian.PutUint32(ops[i].header[0:4], packed)
	}

	req := s.reqPool.Get().(*request)
	if cap(req.ops) < len(ops) {
		req.ops = make([]bufferedOp, len(ops))
	} else {
		req.ops = req.ops[:len(ops)]
	}
	copy(req.ops, ops)

	req.cancelled.Store(false)
	req.isReplication = true
	// Simplified: Compaction doesn't run through this path
	req.opLengths = req.opLengths[:0]

	select {
	case <-req.resp:
	default:
	}

	// Log that we are replicating a batch
	s.logger.Debug("ReplicateBatch submitting request", "count", len(entries))

	return s.submitReq(req)
}

func (s *Store) serializeBatch(req *request, buf *bytes.Buffer) {
	for i := range req.ops {
		op := &req.ops[i]
		if op.opType == OpJournalGet {
			req.opLengths = append(req.opLengths, 0)
			continue
		}
		startLen := buf.Len()
		buf.Write(op.header[:])
		buf.WriteString(op.key)
		if op.opType == OpJournalSet {
			buf.Write(op.val)
		}
		req.opLengths = append(req.opLengths, buf.Len()-startLen)
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
		default:
			return
		}
	}
}

func (s *Store) RegisterReplica(id string, startLogSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationSlots[id] = ReplicaState{LogSeq: startLogSeq, LastSeen: time.Now()}
	s.ackCond.Broadcast()
}

func (s *Store) UnregisterReplica(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replicationSlots, id)
	s.ackCond.Broadcast()
}

func (s *Store) UpdateReplicaLogSeq(id string, logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationSlots[id] = ReplicaState{LogSeq: logSeq, LastSeen: time.Now()}
	s.ackCond.Broadcast()
}

func (s *Store) FindOffsetForLogSeq(logSeq uint64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx := sort.Search(len(s.checkpoints), func(i int) bool {
		return s.checkpoints[i].LogSeq > logSeq
	})
	var hintOffset int64
	if idx == 0 {
		hintOffset = 0
	} else {
		hintOffset = s.checkpoints[idx-1].Offset
	}
	if hintOffset < s.compactedOffset {
		return s.compactedOffset
	}
	return hintOffset
}

func (s *Store) PruneStaleReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, state := range s.replicationSlots {
		if now.Sub(state.LastSeen) > ReplicationTimeout {
			delete(s.replicationSlots, id)
			s.ackCond.Broadcast()
		}
	}
}

func (s *Store) WaitForQuorum(targetLogSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.minReplicas <= 0 {
		return nil
	}
	s.logger.Debug("Waiting for quorum", "targetLogSeq", targetLogSeq, "minReplicas", s.minReplicas)
	for {
		if s.closing.Load() {
			return ErrClosed
		}
		acks := 0
		for _, state := range s.replicationSlots {
			if state.LogSeq >= targetLogSeq {
				acks++
			}
		}
		if acks >= s.minReplicas {
			s.logger.Debug("Quorum met", "targetLogSeq", targetLogSeq, "acks", acks)
			return nil
		}
		s.ackCond.Wait()
	}
}

func (s *Store) Compact() error {
	select {
	case s.compactionChannel <- struct{}{}:
		return nil
	default:
		return ErrCompactionInProgress
	}
}

func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StoreStats{
		KeyCount:               s.index.Len(),
		IndexSizeBytes:         s.index.SizeBytes(),
		Uptime:                 time.Since(s.startTime).Round(time.Second).String(),
		ActiveSnapshots:        len(s.activeSnapshots),
		Offset:                 s.offset,
		NextTxID:               atomic.LoadUint64(&s.nextTxID),
		NextLogSeq:             atomic.LoadUint64(&s.nextLogSeq),
		ConflictCount:          atomic.LoadInt64(&s.conflictCount),
		RecoveryDuration:       s.recoveryDuration,
		PendingOps:             len(s.opsChannel),
		QueueCapacity:          cap(s.opsChannel),
		BytesWritten:           atomic.LoadInt64(&s.bytesWritten),
		BytesRead:              atomic.LoadInt64(&s.bytesRead),
		SlowOps:                atomic.LoadInt64(&s.slowOps),
		LastCompactionDuration: s.lastCompactionDuration,
	}
}

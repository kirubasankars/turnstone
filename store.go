package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"runtime/debug" // Added for stack trace logging
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// CheckpointInterval defines how often we persist an LSN->Offset mapping.
const CheckpointInterval = 512 * 1024 * 1024

// ReplicationTimeout defines when a follower is considered dead and dropped.
const ReplicationTimeout = 30 * time.Second

// SlowOpThreshold defines the duration after which an operation is logged as slow.
const SlowOpThreshold = 500 * time.Millisecond

// Checkpoint maps a Logical Sequence Number to a physical WAL offset.
type Checkpoint struct {
	LSN    uint64
	Offset int64
}

// ReplicaState tracks the progress of a connected follower.
type ReplicaState struct {
	LSN      uint64
	LastSeen time.Time
}

// Store is the core storage engine.
// It orchestrates the WAL, the Hybrid Index, and the single-writer concurrency model.
type Store struct {
	mu        sync.RWMutex
	wal       *WAL
	index     Index // Interface to Index backend
	startTime time.Time
	logger    *slog.Logger

	// Configuration
	allowTruncate bool
	minReplicas   int // Synchronous replication requirement.
	fsyncEnabled  bool

	// State
	offset          int64        // Current physical write offset in the WAL.
	nextLSN         uint64       // Next sequence number to be assigned.
	minReadLSN      uint64       // The oldest active snapshot LSN (for GC).
	compactedOffset int64        // The offset up to which the WAL has been compacted/punched.
	checkpoints     []Checkpoint // In-memory cache of checkpoints for fast seeking.

	// Metrics (Day 2 Operations)
	conflictCount          int64
	recoveryDuration       time.Duration
	bytesWritten           int64         // Atomic: Total bytes written to WAL.
	bytesRead              int64         // Atomic: Total bytes read from WAL.
	slowOps                int64         // Atomic: Count of ops exceeding SlowOpThreshold.
	lastCompactionDuration time.Duration // Protected by mu.
	offloadInProgress      atomic.Bool   // Atomic: Prevents concurrent maintenance jobs.

	// Concurrency Channels
	opsChannel        chan *request // The main write queue.
	compactionChannel chan struct{}
	done              chan struct{}
	wg                sync.WaitGroup

	// Object Pooling
	reqPool *sync.Pool

	// Replication & MVCC State
	activeSnapshots  map[uint64]int          // Reference count of active readers per LSN.
	replicationSlots map[string]ReplicaState // Map of follower ID -> State.
	ackCond          *sync.Cond              // Condition variable to wake up writers waiting for replication.

	dataDir string
}

// FindOffsetForLSN uses binary search on checkpoints to find the approximate
// WAL offset for a given LSN. Used to quickly start replication.
func (s *Store) FindOffsetForLSN(lsn uint64) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	idx := sort.Search(len(s.checkpoints), func(i int) bool {
		return s.checkpoints[i].LSN > lsn
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

// hasConflict checks if the current request conflicts with any pending requests
// or if the data has changed since the transaction's snapshot.
func (s *Store) hasConflict(req *request, pending []*request) bool {
	// 1. Check against pending batch (Write-Write Conflict)
	for _, pReq := range pending {
		for _, op := range pReq.ops {
			if _, exists := req.accessMap[op.key]; exists {
				return true
			}
		}
	}

	// 2. Check against committed data (Read-Write Conflict)
	// CRITICAL FIX: We must hold RLock because concurrent maintenance tasks (OffloadColdKeys)
	// might be modifying the index (especially for MemoryIndex map).
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key := range req.accessMap {
		latest, exists := s.index.GetHead(key)
		if exists && latest.LSN > req.readLSN {
			return true
		}
	}
	return false
}

// NewStore initializes the storage engine, opens files, and performs recovery.
func NewStore(dataDir string, logger *slog.Logger, allowTruncate bool, minReplicas int, fsyncEnabled bool, indexType string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to init data dir: %w", err)
	}

	logger.Info("Opening store", "fsync", fsyncEnabled, "min_replicas", minReplicas, "index", indexType)

	wal, err := OpenWAL(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	idx, err := NewIndex(dataDir, indexType)
	if err != nil {
		return nil, fmt.Errorf("failed to open index: %w", err)
	}

	s := &Store{
		wal:               wal,
		index:             idx,
		minReplicas:       minReplicas,
		fsyncEnabled:      fsyncEnabled,
		checkpoints:       make([]Checkpoint, 0),
		activeSnapshots:   make(map[uint64]int),
		replicationSlots:  make(map[string]ReplicaState),
		startTime:         time.Now(),
		logger:            logger,
		allowTruncate:     allowTruncate,
		opsChannel:        make(chan *request, 5000),
		compactionChannel: make(chan struct{}, 1),
		done:              make(chan struct{}),
		dataDir:           dataDir,
		nextLSN:           1,
		reqPool: &sync.Pool{
			New: func() any {
				return &request{
					resp:   make(chan error, 1),
					opLens: make([]int, 0, 16),
				}
			},
		},
	}
	s.ackCond = sync.NewCond(&s.mu)

	if err := s.recover(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	s.minReadLSN = s.nextLSN - 1
	s.wg.Add(1)
	go s.runLoop()

	return s, nil
}

// recalcMinReadLSN updates the global minimum LSN needed by readers or replicas.
// Data older than this can be safely garbage collected.
func (s *Store) recalcMinReadLSN() {
	min := s.nextLSN - 1
	// 1. Check active transactions
	for lsn := range s.activeSnapshots {
		if lsn < min {
			min = lsn
		}
	}
	// 2. Check active replicas
	for _, state := range s.replicationSlots {
		if state.LSN < min {
			min = state.LSN
		}
	}
	s.minReadLSN = min
}

// RegisterReplica adds a new follower to track for garbage collection safety.
func (s *Store) RegisterReplica(id string, startLSN uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationSlots[id] = ReplicaState{LSN: startLSN, LastSeen: time.Now()}
	s.recalcMinReadLSN()
}

// UnregisterReplica removes a follower.
func (s *Store) UnregisterReplica(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replicationSlots, id)
	s.recalcMinReadLSN()
	s.ackCond.Broadcast()
}

// UpdateReplicaLSN updates the progress of a follower.
func (s *Store) UpdateReplicaLSN(id string, lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationSlots[id] = ReplicaState{LSN: lsn, LastSeen: time.Now()}
	s.recalcMinReadLSN()
	s.ackCond.Broadcast()
}

// PruneStaleReplicas removes followers that haven't sent ACKs recently.
func (s *Store) PruneStaleReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	changed := false
	for id, state := range s.replicationSlots {
		if now.Sub(state.LastSeen) > ReplicationTimeout {
			s.logger.Warn("Dropping stale replica", "id", id, "last_seen", state.LastSeen)
			delete(s.replicationSlots, id)
			changed = true
		}
	}

	if changed {
		s.recalcMinReadLSN()
		s.ackCond.Broadcast()
	}
}

// WaitForQuorum blocks until enough replicas have acknowledged the targetLSN.
func (s *Store) WaitForQuorum(targetLSN uint64) error {
	if s.minReplicas <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		acks := 0
		for _, state := range s.replicationSlots {
			if state.LSN >= targetLSN {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return nil
		}
		s.ackCond.Wait()
	}
}

// recover rebuilds the index by scanning the WAL from the beginning.
func (s *Store) recover() error {
	start := time.Now()
	defer func() {
		s.mu.Lock()
		s.recoveryDuration = time.Since(start)
		s.mu.Unlock()
	}()

	size := s.wal.Size()
	s.logger.Info("Recovering index...", "file_size", size)

	ckptMap, err := s.index.GetCheckpoints()
	if err == nil && len(ckptMap) > 0 {
		for lsn, off := range ckptMap {
			s.checkpoints = append(s.checkpoints, Checkpoint{LSN: lsn, Offset: off})
		}
		sort.Slice(s.checkpoints, func(i, j int) bool {
			return s.checkpoints[i].LSN < s.checkpoints[j].LSN
		})
		s.logger.Info("Loaded checkpoints", "count", len(s.checkpoints))
	}

	var offset int64 = 0
	header := make([]byte, HeaderSize)
	var maxLSN uint64 = 0
	count := 0
	var lastCheckpointOffset int64 = 0
	if len(s.checkpoints) > 0 {
		lastCheckpointOffset = s.checkpoints[len(s.checkpoints)-1].Offset
	}

	for offset < size {
		n, err := s.wal.ReadAt(header, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			if !s.allowTruncate {
				return fmt.Errorf("read error at %d: %w", offset, err)
			}
			s.wal.Truncate(offset)
			break
		}

		// Check for hole (zero block)
		isZero := true
		for _, b := range header {
			if b != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			nextBlock := (offset/4096 + 1) * 4096
			if nextBlock > size {
				break
			}
			offset = nextBlock
			continue
		}

		packed := binary.BigEndian.Uint32(header[0:4])
		keyLen, valLen, isDel := UnpackMeta(packed)
		entryLSN := binary.BigEndian.Uint64(header[4:12])
		storedCrc := binary.BigEndian.Uint32(header[12:16])

		var payloadLen int
		if isDel {
			payloadLen = int(keyLen)
		} else {
			payloadLen = int(keyLen) + int(valLen)
		}

		if payloadLen < 0 || payloadLen > MaxValueSize+MaxKeySize {
			s.wal.Truncate(offset)
			break
		}

		payload := make([]byte, payloadLen)
		n, err = s.wal.ReadAt(payload, offset+HeaderSize)
		if err != nil {
			s.wal.Truncate(offset)
			break
		}
		if n < int(payloadLen) {
			s.wal.Truncate(offset)
			break
		}

		digest := crc32.New(crc32Table)
		digest.Write(header[0:12])
		digest.Write(payload)
		if digest.Sum32() != storedCrc {
			s.logger.Warn("CRC mismatch", "offset", offset)
			s.wal.Truncate(offset)
			break
		}

		key := string(payload[:int(keyLen)])
		entrySize := int64(HeaderSize) + int64(payloadLen)

		// Fix recovery using strict Set semantics to ensure sorted history
		s.index.Set(key, offset, entrySize, entryLSN, isDel, 0)

		if entryLSN > maxLSN {
			maxLSN = entryLSN
		}
		if count == 0 {
			s.compactedOffset = offset
		}

		if offset-lastCheckpointOffset >= CheckpointInterval {
			s.checkpoints = append(s.checkpoints, Checkpoint{LSN: entryLSN, Offset: offset})
			if err := s.index.PutCheckpoint(entryLSN, offset); err != nil {
				s.logger.Error("Failed to save checkpoint", "err", err)
			}
			lastCheckpointOffset = offset
		}

		offset += entrySize
		count++
	}

	s.logger.Info("Recovery complete", "entries", count, "offset", offset, "checkpoints", len(s.checkpoints))
	s.offset = offset
	s.nextLSN = maxLSN + 1
	return nil
}

// Close gracefully shuts down the store, waiting for pending operations.
func (s *Store) Close() error {
	close(s.done)
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index.Close()
	return s.wal.Close()
}

// Compact triggers the WiscKey background compaction job.
func (s *Store) Compact() error {
	select {
	case s.compactionChannel <- struct{}{}:
		return nil
	default:
		return ErrCompactionInProgress
	}
}

// runLoop is the single-writer goroutine.
// It serializes all write requests from opsChannel to ensure consistency without row-level locks.
func (s *Store) runLoop() {
	defer s.wg.Done()

	// CRITICAL FIX: If this loop panics (e.g., LevelDB mmap fail), the server hangs.
	// We must recover and force a process exit to restart.
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("CRITICAL: Storage loop panicked (possible mmap/disk failure)", "err", r, "stack", string(debug.Stack()))
			// Force exit to prevent zombie state where clients wait forever.
			os.Exit(1)
		}
	}()

	var batchBuf bytes.Buffer
	pending := make([]*request, 0, MaxBatchSize)
	batchTicker := time.NewTicker(BatchDelay)
	defer batchTicker.Stop()

	maintenanceTicker := time.NewTicker(10 * time.Second)
	defer maintenanceTicker.Stop()

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

			activePending := make([]*request, 0, len(pending))
			for _, p := range pending {
				if !p.cancelled.Load() {
					activePending = append(activePending, p)
				}
			}
			if s.hasConflict(req, activePending) {
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

		case <-maintenanceTicker.C:
			s.PruneStaleReplicas()

			// Launch offload in background to avoid blocking the write loop.
			if s.offloadInProgress.CompareAndSwap(false, true) {
				go func() {
					defer s.offloadInProgress.Store(false)

					s.mu.RLock()
					safeLSN := s.minReadLSN
					s.mu.RUnlock()

					// CRITICAL FIX: MemoryIndex map modification needs exclusive lock
					// because hasConflict reads it concurrently (now protected by RLock).
					s.mu.Lock()
					count, err := s.index.OffloadColdKeys(safeLSN)
					s.mu.Unlock()

					if err != nil {
						s.logger.Error("Offload failed", "err", err)
					} else if count > 0 {
						s.logger.Info("Offloaded cold keys", "count", count, "safeLSN", safeLSN)
					}
				}()
			}

		case <-s.compactionChannel:
			flush()
			go s.doCompactionJob()
		}
	}
}

// doCompactionJob reads old WAL regions, moves valid live data to the head,
// and punches holes in the file to reclaim space (Linux only).
func (s *Store) doCompactionJob() {
	start := time.Now()
	s.logger.Info("Starting Compaction...")
	s.mu.RLock()
	safeEndOffset := s.offset - (10 * 1024 * 1024)
	startOffset := s.compactedOffset
	s.mu.RUnlock()

	if safeEndOffset <= startOffset {
		s.logger.Info("Not enough data to compact")
		return
	}

	chunkSize := int64(4 * 1024 * 1024)
	buffer := make([]byte, chunkSize)
	currentRead := startOffset
	var moves []bufferedOp
	var bytesReclaimed int64

	for currentRead < safeEndOffset {
		n, err := s.wal.ReadAt(buffer, currentRead)
		if err != nil && err != io.EOF {
			s.logger.Error("Compaction read failed", "err", err)
			return
		}
		if n == 0 {
			break
		}

		parseOff := 0
		moves = moves[:0]

		for parseOff < n {
			if parseOff+4 > n {
				break
			}
			// Skip existing holes
			if buffer[parseOff] == 0 && buffer[parseOff+1] == 0 && buffer[parseOff+2] == 0 && buffer[parseOff+3] == 0 {
				aligned := (parseOff/4096 + 1) * 4096
				if aligned >= n {
					break
				}
				bytesReclaimed += int64(aligned - parseOff)
				parseOff = aligned
				continue
			}
			if parseOff+HeaderSize > n {
				break
			}

			packed := binary.BigEndian.Uint32(buffer[parseOff : parseOff+4])
			keyLen, valLen, isDel := UnpackMeta(packed)
			var payloadLen int
			if isDel {
				payloadLen = int(keyLen)
			} else {
				payloadLen = int(keyLen) + int(valLen)
			}
			if parseOff+HeaderSize+payloadLen > n {
				break
			}

			key := string(buffer[parseOff+HeaderSize : parseOff+HeaderSize+int(keyLen)])
			originalOffset := currentRead + int64(parseOff)

			// Check if this is still the live version
			latest, exists := s.index.GetLatest(key)
			if exists && latest.Offset == originalOffset {
				valStart := parseOff + HeaderSize + int(keyLen)
				val := buffer[valStart : valStart+int(valLen)]
				op := bufferedOp{opType: OpJournalSet, key: key, val: make([]byte, len(val))}
				if isDel {
					op.opType = OpJournalDelete
				}
				copy(op.val, val)
				// Preserve ORIGINAL LSN
				copy(op.header[:], buffer[parseOff:parseOff+HeaderSize])
				moves = append(moves, op)
			} else {
				bytesReclaimed += int64(HeaderSize + payloadLen)
			}
			parseOff += HeaderSize + payloadLen
		}

		if len(moves) > 0 {
			errChan := make(chan error, 1)
			gcReq := &request{ops: moves, resp: errChan, isCompaction: true, accessMap: make(map[string]struct{})}
			s.opsChannel <- gcReq
			<-errChan
		}

		punchSize := (int64(parseOff) / 4096) * 4096
		if punchSize > 0 {
			s.wal.PunchHole(currentRead, punchSize)
		}

		s.mu.Lock()
		s.compactedOffset = currentRead + punchSize
		s.lastCompactionDuration = time.Since(start)
		s.mu.Unlock()
		currentRead += chunkSize
	}
	s.logger.Info("Compaction job done", "duration", time.Since(start), "reclaimed_bytes", bytesReclaimed)
}

// flushBatch writes the buffered operations to the WAL, Syncs, and updates the Index.
func (s *Store) flushBatch(pending []*request, buf *bytes.Buffer) {
	start := time.Now()
	activeReqs := pending[:0]
	for _, req := range pending {
		if req.cancelled.Load() {
			continue
		}
		activeReqs = append(activeReqs, req)
	}
	if len(activeReqs) == 0 {
		buf.Reset()
		return
	}

	defer func() {
		dur := time.Since(start)
		if dur > SlowOpThreshold {
			atomic.AddInt64(&s.slowOps, 1)
			s.logger.Warn("Slow Batch Write", "ops", len(activeReqs), "duration", dur)
		}
	}()

	// Phase 1: Write to Disk (Hold Lock)
	s.mu.Lock()

	currentLSN := s.nextLSN
	startBatchLSN := currentLSN
	minVer := s.getMinReadLSN()

	bufBytes := buf.Bytes()
	bufPtr := 0

	for _, req := range activeReqs {
		commitLSN := currentLSN
		if !req.isCompaction && !req.isReplication {
			currentLSN++
		}

		for i := range req.ops {
			op := &req.ops[i]
			if op.opType == OpJournalGet {
				continue
			}
			opLen := req.opLens[i]

			lsnToWrite := commitLSN
			if req.isCompaction || req.isReplication {
				lsnToWrite = binary.BigEndian.Uint64(op.header[4:12])
			}

			// Patch LSN and CRC into buffer
			binary.BigEndian.PutUint64(bufBytes[bufPtr+4:bufPtr+12], lsnToWrite)
			digest := crc32.New(crc32Table)
			digest.Write(bufBytes[bufPtr : bufPtr+12])
			digest.Write(bufBytes[bufPtr+16 : bufPtr+opLen])
			binary.BigEndian.PutUint32(bufBytes[bufPtr+12:bufPtr+16], digest.Sum32())
			bufPtr += opLen
		}
	}

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

	// Update Checkpoints if necessary
	var lastCkptOffset int64 = 0
	if len(s.checkpoints) > 0 {
		lastCkptOffset = s.checkpoints[len(s.checkpoints)-1].Offset
	}
	if writeOffset-lastCkptOffset >= CheckpointInterval {
		s.checkpoints = append(s.checkpoints, Checkpoint{LSN: startBatchLSN, Offset: writeOffset})
		if err := s.index.PutCheckpoint(startBatchLSN, writeOffset); err != nil {
			s.logger.Error("Failed to persist checkpoint", "err", err)
		}
	}

	// Phase 2: Replication (Release Lock)
	s.mu.Unlock()

	batchMaxLSN := currentLSN - 1
	if s.minReplicas > 0 && batchMaxLSN >= startBatchLSN {
		err := s.WaitForQuorum(batchMaxLSN)
		if err != nil {
			s.logger.Error("Replication quorum failed", "err", err)
		}
	}

	// Phase 3: Index Update (Re-acquire Lock)
	s.mu.Lock()
	defer s.mu.Unlock()

	currentOffset := writeOffset
	indexLSN := s.nextLSN
	maxReplicationLSN := uint64(0)

	for _, req := range activeReqs {
		reqLSN := indexLSN
		if !req.isCompaction && !req.isReplication {
			indexLSN++
		}

		for i := range req.ops {
			op := &req.ops[i]
			if op.opType == OpJournalGet {
				continue
			}
			opLen := req.opLens[i]
			reqOffset := currentOffset
			currentOffset += int64(opLen)
			isDel := op.opType == OpJournalDelete

			var entryLSN uint64
			if req.isCompaction || req.isReplication {
				entryLSN = binary.BigEndian.Uint64(op.header[4:12])
				if req.isReplication && entryLSN > maxReplicationLSN {
					maxReplicationLSN = entryLSN
				}
			} else {
				entryLSN = reqLSN
			}

			if req.isCompaction {
				s.index.UpdateHead(op.key, reqOffset, int64(opLen), entryLSN)
			} else {
				s.index.Set(op.key, reqOffset, int64(opLen), entryLSN, isDel, minVer)
			}
		}
		select {
		case req.resp <- nil:
		default:
		}
	}

	if maxReplicationLSN >= s.nextLSN {
		s.nextLSN = maxReplicationLSN + 1
	} else {
		s.nextLSN = currentLSN
	}

	s.offset = currentOffset
	buf.Reset()
}

func (s *Store) failBatch(reqs []*request, err error) {
	for _, req := range reqs {
		select {
		case req.resp <- err:
		default:
		}
	}
}

func (s *Store) getMinReadLSN() uint64 {
	return s.minReadLSN
}

// AcquireSnapshot grabs the current LSN for a read transaction.
// It increments a reference counter to prevent GC of this LSN.
func (s *Store) AcquireSnapshot() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	lsn := s.nextLSN - 1
	if lsn < 0 {
		lsn = 0
	}
	s.activeSnapshots[lsn]++
	s.recalcMinReadLSN()
	return lsn
}

// ReleaseSnapshot decrements the reference counter for a snapshot.
func (s *Store) ReleaseSnapshot(readLSN uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if count, ok := s.activeSnapshots[readLSN]; ok {
		s.activeSnapshots[readLSN] = count - 1
		if s.activeSnapshots[readLSN] <= 0 {
			delete(s.activeSnapshots, readLSN)
			s.recalcMinReadLSN()
		}
	}
}

func (s *Store) serializeBatch(req *request, buf *bytes.Buffer) {
	for i := range req.ops {
		op := &req.ops[i]
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
		length := buf.Len() - startLen
		req.opLens = append(req.opLens, length)
	}
}

// ReplicateBatch applies a batch of LogEntry items received from a leader.
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
		// Pack LSN into header for flushBatch to retrieve
		binary.BigEndian.PutUint64(ops[i].header[4:12], entry.LSN)

		keyLen := len(entry.Key)
		valLen := len(entry.Value)
		isDel := entry.OpType == OpJournalDelete
		if isDel {
			valLen = 0
		}

		packed := PackMeta(uint32(keyLen), uint32(valLen), isDel)
		binary.BigEndian.PutUint32(ops[i].header[0:4], packed)
	}

	req := s.reqPool.Get().(*request)
	req.ops = ops
	req.cancelled.Store(false)
	req.isReplication = true
	req.isCompaction = false
	req.opLens = req.opLens[:0]

	shouldPool := false
	defer func() {
		if shouldPool {
			s.reqPool.Put(req)
		}
	}()

	timer := time.NewTimer(DefaultWriteTimeout * 2)
	defer timer.Stop()

	select {
	case s.opsChannel <- req:
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		select {
		case err := <-req.resp:
			shouldPool = true
			return err
		case <-s.done:
			shouldPool = true
			return ErrClosed
		}
	case <-s.done:
		shouldPool = true
		return ErrClosed
	}
}

// ApplyBatch submits a transaction buffer to the write queue.
func (s *Store) ApplyBatch(ops []bufferedOp, readLSN uint64) error {
	if s.index.SizeBytes() >= MaxIndexBytes {
		return ErrMemoryLimitExceeded
	}
	accessMap := make(map[string]struct{}, len(ops))
	for i := range ops {
		op := &ops[i]
		accessMap[op.key] = struct{}{}
		if op.opType != OpJournalGet {
			keyLen := len(op.key)
			valLen := uint32(len(op.val))
			isDel := false
			if op.opType == OpJournalDelete {
				valLen = 0
				isDel = true
			}
			packed := PackMeta(uint32(keyLen), valLen, isDel)
			binary.BigEndian.PutUint32(op.header[0:4], packed)
		}
	}
	req := s.reqPool.Get().(*request)
	req.ops = ops
	req.readLSN = readLSN
	req.accessMap = accessMap
	req.cancelled.Store(false)
	req.isCompaction = false
	req.opLens = req.opLens[:0]
	shouldPool := false
	defer func() {
		if shouldPool {
			s.reqPool.Put(req)
		}
	}()
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
			shouldPool = true
			return err
		case <-timer.C:
			req.cancelled.Store(true)
			return ErrTransactionTimeout
		}
	case <-timer.C:
		shouldPool = true
		return ErrBusy
	case <-s.done:
		shouldPool = true
		return ErrClosed
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

// Get retrieves a value from the store using Snapshot Isolation.
func (s *Store) Get(key string, readLSN uint64) ([]byte, error) {
	start := time.Now()
	defer func() {
		dur := time.Since(start)
		if dur > SlowOpThreshold {
			atomic.AddInt64(&s.slowOps, 1)
			s.logger.Warn("Slow Read", "key", key, "duration", dur)
		}
	}()

	s.mu.RLock()
	entry, ok := s.index.Get(key, readLSN)
	safeLimit := s.offset
	s.mu.RUnlock()

	if !ok {
		return nil, ErrKeyNotFound
	}
	if entry.Offset >= safeLimit {
		return nil, ErrKeyNotFound
	}

	rawBuf := make([]byte, entry.Length)
	if _, err := s.wal.ReadAt(rawBuf, entry.Offset); err != nil {
		return nil, fmt.Errorf("read error: %w", err)
	}
	atomic.AddInt64(&s.bytesRead, int64(len(rawBuf)))

	allZeros := true
	for _, b := range rawBuf {
		if b != 0 {
			allZeros = false
			break
		}
	}
	if allZeros {
		return nil, ErrKeyNotFound
	}

	packed := binary.BigEndian.Uint32(rawBuf[0:4])
	keyLen, _, _ := UnpackMeta(packed)
	storedCrc := binary.BigEndian.Uint32(rawBuf[12:16])
	digest := crc32.New(crc32Table)
	digest.Write(rawBuf[0:12])
	digest.Write(rawBuf[16:])
	if digest.Sum32() != storedCrc {
		return nil, ErrCrcMismatch
	}
	valStart := 16 + int(keyLen)
	val := rawBuf[valStart:]
	valCopy := make([]byte, len(val))
	copy(valCopy, val)
	return valCopy, nil
}

// Stats returns a snapshot of the internal store metrics.
func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return StoreStats{
		KeyCount:               s.index.Len(),
		IndexSizeBytes:         s.index.SizeBytes(),
		Uptime:                 time.Since(s.startTime).Round(time.Second).String(),
		ActiveSnapshots:        len(s.activeSnapshots),
		Offset:                 s.offset,
		NextLSN:                atomic.LoadUint64(&s.nextLSN),
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

package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/engine"
	"turnstone/protocol"
	"turnstone/wal"
)

const (
	SysKeyLogSeq = "_sys_logseq"
)

// Checkpoint maps a Log Sequence Number (LogSeq) to a physical/virtual WAL offset.
type Checkpoint struct {
	LogSeq uint64
	Offset int64
}

// ReplicaState tracks the progress of a connected replica.
type ReplicaState struct {
	LogSeq   uint64
	LastSeen time.Time
}

// StoreStats holds a snapshot of internal metrics.
type StoreStats struct {
	KeyCount         int
	MemorySizeBytes  int64
	Uptime           string
	Offset           int64
	NextLogSeq       uint64
	ConflictCount    int64
	RecoveryDuration time.Duration
	PendingOps       int
	QueueCapacity    int
	BytesWritten     int64
	BytesRead        int64
	SlowOps          int64
}

type Store struct {
	dataDir           string
	isSystemPartition bool
	minReplicas       int
	logger            *slog.Logger
	startTime         time.Time

	wal    *wal.WAL
	engine *engine.StoneDB

	opsChannel chan protocol.BatchRequest
	ctx        context.Context
	cancel     context.CancelFunc

	mu               sync.RWMutex
	offset           int64
	nextLogSeq       uint64
	checkpoints      []Checkpoint
	replicationSlots map[string]ReplicaState
	ackCond          *sync.Cond

	recoveryDuration int64
	bytesWritten     int64
	bytesRead        int64
	conflictCount    int64
}

func NewStore(dataDir string, logger *slog.Logger, allowTruncate bool, minReplicas int, fsyncEnabled bool) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to init data dir: %w", err)
	}

	w, err := wal.OpenWAL(dataDir, fsyncEnabled)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	stoneDir := filepath.Join(dataDir, "stone")
	eng, err := engine.Open(stoneDir, 0)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("failed to open StoneDB: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Store{
		dataDir:           dataDir,
		isSystemPartition: filepath.Base(dataDir) == "0",
		logger:            logger,
		minReplicas:       0, // Init to 0 for bootstrap
		startTime:         time.Now(),
		wal:               w,
		engine:            eng,
		opsChannel:        make(chan protocol.BatchRequest, 5000),
		ctx:               ctx,
		cancel:            cancel,
		checkpoints:       make([]Checkpoint, 0),
		replicationSlots:  make(map[string]ReplicaState),
		nextLogSeq:        1,
	}
	s.ackCond = sync.NewCond(&s.mu)

	if err := s.recover(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("recovery failed: %w", err)
	}

	go s.runLoop()

	s.mu.Lock()
	s.minReplicas = minReplicas
	s.mu.Unlock()

	return s, nil
}

func (s *Store) Close() error {
	s.cancel()
	s.mu.Lock()
	s.ackCond.Broadcast()
	s.mu.Unlock()

	if err := s.engine.Close(); err != nil {
		s.logger.Error("Failed to close StoneDB", "err", err)
	}
	return s.wal.Close()
}

func (s *Store) runLoop() {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("CRITICAL: Storage loop panicked", "err", r)
			os.Exit(1)
		}
	}()

	pending := make([]protocol.BatchRequest, 0, protocol.MaxBatchSize)
	pendingCount := 0

	batchTicker := time.NewTicker(protocol.BatchDelay)
	defer batchTicker.Stop()

	flush := func() {
		if len(pending) > 0 {
			s.processBatch(pending)
			pending = pending[:0]
			pendingCount = 0
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			flush()
			return

		case req := <-s.opsChannel:
			pending = append(pending, req)
			pendingCount += len(req.Entries)

			if pendingCount >= protocol.MaxBatchSize {
				flush()
			}

		case <-batchTicker.C:
			flush()
		}
	}
}

func (s *Store) processBatch(reqs []protocol.BatchRequest) {
	s.mu.Lock()
	startLogSeq := s.nextLogSeq
	var totalOps uint64
	var allEntries []protocol.LogEntry

	for _, req := range reqs {
		totalOps += uint64(len(req.Entries))
	}
	s.nextLogSeq += totalOps
	currentLogSeq := startLogSeq

	for _, req := range reqs {
		for i := range req.Entries {
			req.Entries[i].LogSeq = currentLogSeq
			currentLogSeq++
			allEntries = append(allEntries, req.Entries[i])
		}
	}
	s.mu.Unlock()

	off, _, err := s.wal.WriteBatch(allEntries)
	if err != nil {
		s.notifyErrors(reqs, err)
		return
	}
	atomic.AddInt64(&s.bytesWritten, off-s.offset)

	if totalOps > 0 {
		lastLogSeq := startLogSeq + totalOps - 1
		if err := s.WaitForQuorum(lastLogSeq); err != nil {
			s.logger.Error("Replication quorum failed", "err", err)
		}
	}

	s.mu.Lock()
	var lastCkptOffset int64
	if len(s.checkpoints) > 0 {
		lastCkptOffset = s.checkpoints[len(s.checkpoints)-1].Offset
	}
	if off-lastCkptOffset >= protocol.CheckpointInterval {
		s.checkpoints = append(s.checkpoints, Checkpoint{LogSeq: startLogSeq, Offset: off})
	}
	s.offset = off
	s.mu.Unlock()

	tx := s.engine.BeginTx()

	for _, entry := range allEntries {
		if entry.OpCode == protocol.OpJournalSet {
			tx.Put(string(entry.Key), entry.Value)
		} else if entry.OpCode == protocol.OpJournalDelete {
			tx.Delete(string(entry.Key))
		}
	}

	logSeqBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(logSeqBuf, startLogSeq+totalOps-1)
	tx.Put(SysKeyLogSeq, logSeqBuf)

	if err := tx.Commit(); err != nil {
		s.logger.Error("Failed to commit to StoneDB", "err", err)
		// Track conflicts if it's a conflict error, though generally harder to distinguish without type check
		// For now just increment generic conflict/error count if needed
		atomic.AddInt64(&s.conflictCount, 1)
		s.notifyErrors(reqs, err)
		return
	}

	for _, req := range reqs {
		select {
		case req.Resp <- nil:
		default:
		}
	}
}

func (s *Store) notifyErrors(reqs []protocol.BatchRequest, err error) {
	for _, req := range reqs {
		select {
		case req.Resp <- err:
		default:
		}
	}
}

func (s *Store) recover() error {
	start := time.Now()
	defer func() {
		atomic.StoreInt64(&s.recoveryDuration, int64(time.Since(start)))
	}()

	s.logger.Info("Recovering Storage...")

	var lastAppliedLogSeq uint64 = 0
	val, err := s.engine.Get(SysKeyLogSeq, s.engine.CurrentTxID())
	if err == nil && len(val) == 8 {
		lastAppliedLogSeq = binary.BigEndian.Uint64(val)
	} else if err != nil && err != protocol.ErrKeyNotFound {
		return fmt.Errorf("failed to read sys log seq: %w", err)
	}

	s.logger.Info("StoneDB state", "last_log_seq", lastAppliedLogSeq)

	walEntries, finalOffset, err := s.wal.Recover()
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var maxLogSeq uint64
	var lastCkptOffset int64
	s.checkpoints = []Checkpoint{}
	replayBatch := make([]protocol.LogEntry, 0)

	for _, entry := range walEntries {
		if entry.LogSeq > maxLogSeq {
			maxLogSeq = entry.LogSeq
		}

		if entry.Offset-lastCkptOffset >= protocol.CheckpointInterval {
			s.checkpoints = append(s.checkpoints, Checkpoint{LogSeq: entry.LogSeq, Offset: entry.Offset})
			lastCkptOffset = entry.Offset
		}

		if entry.LogSeq > lastAppliedLogSeq {
			replayBatch = append(replayBatch, entry)
		}
	}

	s.offset = finalOffset
	s.nextLogSeq = maxLogSeq + 1

	if len(replayBatch) > 0 {
		s.logger.Info("Replaying WAL to StoneDB", "entries", len(replayBatch))
		tx := s.engine.BeginTx()
		for _, entry := range replayBatch {
			if entry.OpCode == protocol.OpJournalSet {
				tx.Put(string(entry.Key), entry.Value)
			} else if entry.OpCode == protocol.OpJournalDelete {
				tx.Delete(string(entry.Key))
			}
		}
		logSeqBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(logSeqBuf, maxLogSeq)
		tx.Put(SysKeyLogSeq, logSeqBuf)

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to replay batch to StoneDB: %w", err)
		}
	}

	count, _ := s.engine.KeyCount()
	s.logger.Info("Recovery complete", "keys", count, "offset", s.offset)
	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	if s.isSystemPartition {
		return nil, protocol.ErrSystemPartitionReadOnly
	}
	val, err := s.engine.Get(key, s.engine.CurrentTxID())
	if err == protocol.ErrKeyNotFound {
		return nil, protocol.ErrKeyNotFound
	}
	if err == nil {
		atomic.AddInt64(&s.bytesRead, int64(len(val)))
	}
	return val, err
}

func (s *Store) ApplyBatch(entries []protocol.LogEntry) error {
	if s.isSystemPartition {
		return protocol.ErrSystemPartitionReadOnly
	}
	return s.submitBatch(entries)
}

func (s *Store) ReplicateBatch(entries []protocol.LogEntry) error {
	return s.submitBatch(entries)
}

func (s *Store) submitBatch(entries []protocol.LogEntry) error {
	resp := make(chan error, 1)
	select {
	case s.opsChannel <- protocol.BatchRequest{Entries: entries, Resp: resp}:
		select {
		case err := <-resp:
			return err
		case <-s.ctx.Done():
			return protocol.ErrClosed
		}
	case <-s.ctx.Done():
		return protocol.ErrClosed
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
	return hintOffset
}

func (s *Store) WaitForQuorum(targetLogSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.minReplicas <= 0 {
		return nil
	}

	for {
		select {
		case <-s.ctx.Done():
			return protocol.ErrClosed
		default:
		}

		acks := 0
		for _, state := range s.replicationSlots {
			if state.LogSeq >= targetLogSeq {
				acks++
			}
		}
		if acks >= s.minReplicas {
			return nil
		}
		s.ackCond.Wait()
	}
}

func (s *Store) Stats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys, _ := s.engine.KeyCount()
	mem := s.engine.MemTableSize()

	return StoreStats{
		KeyCount:         int(keys),
		MemorySizeBytes:  mem,
		Uptime:           time.Since(s.startTime).Round(time.Second).String(),
		Offset:           s.offset,
		NextLogSeq:       atomic.LoadUint64(&s.nextLogSeq),
		RecoveryDuration: time.Duration(atomic.LoadInt64(&s.recoveryDuration)),
		PendingOps:       len(s.opsChannel),
		QueueCapacity:    cap(s.opsChannel),
		BytesWritten:     atomic.LoadInt64(&s.bytesWritten),
		BytesRead:        atomic.LoadInt64(&s.bytesRead),
		ConflictCount:    atomic.LoadInt64(&s.conflictCount),
	}
}

func (s *Store) WAL() *wal.WAL {
	return s.wal
}

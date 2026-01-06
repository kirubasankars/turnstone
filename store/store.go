package store

import (
	"log/slog"
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/stonedb"
)

// StoreStats holds basic metrics.
type StoreStats struct {
	KeyCount  int
	ActiveTxs int
	Uptime    string
	Offset    int64 // Represents the WAL/VLog offset or similar metric
	Conflicts uint64
}

// Store wraps stonedb.DB to provide a compatibility layer, stats, and replication logic.
type Store struct {
	*stonedb.DB
	logger      *slog.Logger
	startTime   time.Time
	isSystem    bool
	minReplicas int

	// Replication State
	mu       sync.Mutex
	replicas map[string]uint64 // ReplicaID -> LastAckLogSeq
	cond     *sync.Cond
}

func NewStore(dir string, logger *slog.Logger, walSync bool, minReplicas int, isSystem bool) (*Store, error) {
	opts := stonedb.Options{
		MaxWALSize:           10 * 1024 * 1024,
		CompactionMinGarbage: 4 * 1024 * 1024,
		// Enable truncation to recover from partial writes/corruption automatically
		TruncateCorruptWAL: true,
	}

	db, err := stonedb.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	s := &Store{
		DB:          db,
		logger:      logger,
		startTime:   time.Now(),
		isSystem:    isSystem,
		minReplicas: minReplicas,
		replicas:    make(map[string]uint64),
	}
	s.cond = sync.NewCond(&s.mu)
	return s, nil
}

// ApplyBatch applies a batch of protocol entries.
// If minReplicas > 0, it blocks until quorum is reached.
// Used for writes on the Leader.
func (s *Store) ApplyBatch(entries []protocol.LogEntry) error {
	if s.isSystem {
		// Enforce Read-Only System Partition logic if needed
		if len(entries) > 0 {
			return protocol.ErrSystemPartitionReadOnly
		}
	}

	// Leader writes uses stonedb internal ID generation via Transaction
	tx := s.DB.NewTransaction(true)
	for _, e := range entries {
		var err error
		if e.OpCode == protocol.OpJournalDelete {
			err = tx.Delete(e.Key)
		} else {
			err = tx.Put(e.Key, e.Value)
		}
		if err != nil {
			tx.Discard()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Get the committed ID to wait for
	committedOpID := s.DB.LastOpID()

	// Wait for Quorum if configured
	if s.minReplicas > 0 {
		s.waitForQuorum(committedOpID)
	}

	return nil
}

// ReplicateBatch applies a batch from a leader (no quorum wait).
// Used by Followers.
func (s *Store) ReplicateBatch(entries []protocol.LogEntry) error {
	var vlogEntries []stonedb.ValueLogEntry

	for _, e := range entries {
		vEntry := stonedb.ValueLogEntry{
			Key:           e.Key,
			Value:         e.Value,
			OperationID:   e.LogSeq,
			TransactionID: e.LogSeq, // Mapping LogSeq to TxID for replication
		}

		if e.OpCode == protocol.OpJournalDelete {
			vEntry.IsDelete = true
			vEntry.Value = nil
		}
		vlogEntries = append(vlogEntries, vEntry)
	}

	return s.DB.ApplyBatch(vlogEntries)
}

// Get retrieves a value by key.
func (s *Store) Get(key string) ([]byte, error) {
	if s.isSystem {
		return nil, protocol.ErrSystemPartitionReadOnly
	}
	tx := s.DB.NewTransaction(false)
	defer tx.Discard()
	val, err := tx.Get([]byte(key))
	if err == stonedb.ErrKeyNotFound {
		return nil, protocol.ErrKeyNotFound
	}
	return val, err
}

// Close closes the underlying StoneDB instance.
func (s *Store) Close() error {
	s.logger.Info("Closing StoneDB store")
	return s.DB.Close()
}

// Stats returns usage statistics.
func (s *Store) Stats() StoreStats {
	count, _ := s.DB.KeyCount()
	return StoreStats{
		KeyCount:  int(count),
		ActiveTxs: s.DB.ActiveTransactionCount(),
		Uptime:    time.Since(s.startTime).Round(time.Second).String(),
		Offset:    int64(s.DB.LastOpID()),
		Conflicts: s.DB.GetConflicts(),
	}
}

// ActiveTransactionCount returns the number of active transactions in the underlying DB.
func (s *Store) ActiveTransactionCount() int {
	return s.DB.ActiveTransactionCount()
}

// RegisterReplica adds a replica to the tracking map.
func (s *Store) RegisterReplica(id string, logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("Replica registered", "id", id, "seq", logSeq)
	s.replicas[id] = logSeq
}

// UnregisterReplica removes a replica from the tracking map.
func (s *Store) UnregisterReplica(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("Replica unregistered", "id", id)
	delete(s.replicas, id)
}

// UpdateReplicaLogSeq updates the acked sequence for a replica and wakes up waiters.
func (s *Store) UpdateReplicaLogSeq(id string, logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.replicas[id]; ok {
		s.replicas[id] = logSeq
		s.cond.Broadcast()
	}
}

// waitForQuorum blocks until enough replicas have acknowledged the given logSeq.
func (s *Store) waitForQuorum(logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		acks := 0
		for _, seq := range s.replicas {
			if seq >= logSeq {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return
		}
		s.cond.Wait()
	}
}

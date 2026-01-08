package store

import (
	"encoding/json"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"turnstone/protocol"
	"turnstone/stonedb"
)

// StoreStats holds basic metrics.
type StoreStats struct {
	ActiveTxs  int
	Uptime     string
	Offset     int64 // Represents the WAL/VLog offset or similar metric
	Conflicts  uint64
	ReplicaLag uint64 // Least replica lag among all replicas
	WALFiles   int
	WALSize    int64
	VLogFiles  int
	VLogSize   int64
}

// ReplicaSlot tracks the state of a connected replication consumer.
type ReplicaSlot struct {
	LogSeq   uint64    `json:"log_seq"`
	Role     string    `json:"role"`
	LastSeen time.Time `json:"last_seen"`
}

// Store wraps stonedb.DB to provide a compatibility layer, stats, and replication logic.
type Store struct {
	*stonedb.DB
	logger      *slog.Logger
	startTime   time.Time
	isSystem    bool
	minReplicas int

	// Replication State
	mu        sync.Mutex
	replicas  map[string]*ReplicaSlot // ReplicaID -> Slot State
	cond      *sync.Cond
	slotsFile string
	dirty     bool
}

func NewStore(dir string, logger *slog.Logger, walSync bool, minReplicas int, isSystem bool) (*Store, error) {
	s := &Store{
		logger:      logger,
		startTime:   time.Now(),
		isSystem:    isSystem,
		minReplicas: minReplicas,
		replicas:    make(map[string]*ReplicaSlot),
		slotsFile:   filepath.Join(dir, "replication.slots"),
	}
	s.cond = sync.NewCond(&s.mu)

	// Load existing persistence state (if any) - retained for reporting continuity, though not strictly required for WAL safety anymore
	s.loadSlots()

	// Start persistence loop (optional, but keeps stats across restarts)
	go s.runPersistence()

	opts := stonedb.Options{
		MaxWALSize:           10 * 1024 * 1024,
		CompactionMinGarbage: 4 * 1024 * 1024,
		// Enable truncation to recover from partial writes/corruption automatically
		TruncateCorruptWAL: true,
		// WALRetentionProvider removed: Retention is now purely time-based via stonedb options.
	}

	db, err := stonedb.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	s.DB = db

	return s, nil
}

// ApplyBatch applies a batch of protocol entries.
// If minReplicas > 0, it blocks until quorum is reached.
// Used for writes on the Leader.
func (s *Store) ApplyBatch(entries []protocol.LogEntry) error {
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
	// Save slots one last time
	s.mu.Lock()
	if s.dirty {
		s.saveSlotsLocked()
	}
	s.mu.Unlock()
	return s.DB.Close()
}

// Stats returns usage statistics.
func (s *Store) Stats() StoreStats {
	wf, ws, vf, vs := s.DB.StorageStats()

	// Calculate Least Replica Lag
	head := s.DB.LastOpID()
	minLag := uint64(0)
	first := true

	s.mu.Lock()
	for _, r := range s.replicas {
		lag := uint64(0)
		if head > r.LogSeq {
			lag = head - r.LogSeq
		}
		if first || lag < minLag {
			minLag = lag
			first = false
		}
	}
	s.mu.Unlock()

	// If no replicas, lag is 0
	if first {
		minLag = 0
	}

	return StoreStats{
		ActiveTxs:  s.DB.ActiveTransactionCount(),
		Uptime:     time.Since(s.startTime).Round(time.Second).String(),
		Offset:     int64(head),
		Conflicts:  s.DB.GetConflicts(),
		ReplicaLag: minLag,
		WALFiles:   wf,
		WALSize:    ws,
		VLogFiles:  vf,
		VLogSize:   vs,
	}
}

// ActiveTransactionCount returns the number of active transactions in the underlying DB.
func (s *Store) ActiveTransactionCount() int {
	return s.DB.ActiveTransactionCount()
}

// RegisterReplica adds a replica slot to the tracking map (for stats only).
func (s *Store) RegisterReplica(id string, logSeq uint64, role string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logger.Info("Replica registered", "id", id, "seq", logSeq, "role", role)
	s.replicas[id] = &ReplicaSlot{
		LogSeq:   logSeq,
		Role:     role,
		LastSeen: time.Now(),
	}
	s.dirty = true
}

// UnregisterReplica logs the disconnection.
func (s *Store) UnregisterReplica(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// We keep the slot in memory for stats visualization until restart/manual cleanup,
	// or we can remove it immediately since retention is time-based now.
	// Let's remove it to keep the stats clean for active connections.
	if _, ok := s.replicas[id]; ok {
		s.logger.Info("Replica disconnected", "id", id)
		delete(s.replicas, id) // Remove from map
		s.dirty = true
	}
}

// DeleteReplica explicitly removes a replication slot.
func (s *Store) DeleteReplica(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.replicas[id]; !ok {
		return protocol.ErrKeyNotFound // reusing error for "slot not found"
	}
	delete(s.replicas, id)
	s.dirty = true
	s.logger.Info("Replica slot deleted", "id", id)
	return nil
}

// UpdateReplicaLogSeq updates the acked sequence for a replica and wakes up waiters.
func (s *Store) UpdateReplicaLogSeq(id string, logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		// Only update if seq advances (or first connect)
		if logSeq > slot.LogSeq {
			slot.LogSeq = logSeq
			s.dirty = true
		}
		slot.LastSeen = time.Now()
		s.cond.Broadcast()
	}
}

// waitForQuorum blocks until enough replicas with Role="server" have acknowledged the given logSeq.
func (s *Store) waitForQuorum(logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		acks := 0
		for _, slot := range s.replicas {
			if slot.Role == "server" && slot.LogSeq >= logSeq {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return
		}
		s.cond.Wait()
	}
}

// getMinSlotLogSeq calculates the minimum LogSeq required by ANY connected client (Replicas AND CDC).
// This is now purely advisory or used for lag calculation, as WAL purging is time-based.
func (s *Store) getMinSlotLogSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	minSeq := uint64(math.MaxUint64)
	hasSlots := false

	for _, slot := range s.replicas {
		hasSlots = true
		if slot.LogSeq < minSeq {
			minSeq = slot.LogSeq
		}
	}

	if !hasSlots {
		return math.MaxUint64
	}
	return minSeq
}

func (s *Store) loadSlots() {
	data, err := os.ReadFile(s.slotsFile)
	if err != nil {
		if !os.IsNotExist(err) {
			s.logger.Error("Failed to load replication slots", "err", err)
		}
		return
	}
	if err := json.Unmarshal(data, &s.replicas); err != nil {
		s.logger.Error("Failed to parse replication slots", "err", err)
	} else {
		s.logger.Info("Loaded replication slots", "count", len(s.replicas))
	}
}

func (s *Store) runPersistence() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		if s.dirty {
			s.saveSlotsLocked()
			s.dirty = false
		}
		s.mu.Unlock()
	}
}

// saveSlotsLocked assumes mu is held
func (s *Store) saveSlotsLocked() {
	data, err := json.MarshalIndent(s.replicas, "", "  ")
	if err != nil {
		s.logger.Error("Failed to marshal slots", "err", err)
		return
	}
	tmp := s.slotsFile + ".tmp"

	// 1. Create/Truncate Temp File
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		s.logger.Error("Failed to create slots tmp file", "err", err)
		return
	}

	// 2. Write Data
	if _, err := f.Write(data); err != nil {
		f.Close()
		s.logger.Error("Failed to write to slots tmp file", "err", err)
		return
	}

	// 3. Fsync to ensure durability on disk
	if err := f.Sync(); err != nil {
		f.Close()
		s.logger.Error("Failed to sync slots tmp file", "err", err)
		return
	}

	// 4. Close
	if err := f.Close(); err != nil {
		s.logger.Error("Failed to close slots tmp file", "err", err)
		return
	}

	// 5. Atomic Rename
	if err := os.Rename(tmp, s.slotsFile); err != nil {
		s.logger.Error("Failed to rename slots file", "err", err)
	}
}

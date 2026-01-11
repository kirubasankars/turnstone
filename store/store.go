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
	LogSeq    uint64    `json:"log_seq"`
	Role      string    `json:"role"`
	LastSeen  time.Time `json:"last_seen"`
	Connected bool      `json:"connected"`
}

// Store wraps stonedb.DB to provide a compatibility layer, stats, and replication logic.
type Store struct {
	*stonedb.DB
	logger      *slog.Logger
	startTime   time.Time
	isSystem    bool
	minReplicas int

	// Replication State
	mu          sync.Mutex
	replicas    map[string]*ReplicaSlot // ReplicaID -> Slot State
	cond        *sync.Cond
	slotsFile   string
	dirty       bool
	walStrategy string
}

func NewStore(dir string, logger *slog.Logger, minReplicas int, isSystem bool, walStrategy string, maxDiskUsage int) (*Store, error) {
	s := &Store{
		logger:      logger,
		startTime:   time.Now(),
		isSystem:    isSystem,
		minReplicas: minReplicas,
		replicas:    make(map[string]*ReplicaSlot),
		slotsFile:   filepath.Join(dir, "replication.slots"),
		walStrategy: walStrategy,
	}
	s.cond = sync.NewCond(&s.mu)

	// Load existing persistence state (if any)
	s.loadSlots()

	// Start persistence loop
	go s.runPersistence()

	// Start WAL retention manager if strategy is replication
	if s.walStrategy == "replication" {
		go s.runRetentionManager()
	}

	opts := stonedb.Options{
		MaxWALSize:           10 * 1024 * 1024,
		CompactionMinGarbage: 4 * 1024 * 1024,
		// Enable truncation to recover from partial writes/corruption automatically
		TruncateCorruptWAL:  true,
		MaxDiskUsagePercent: maxDiskUsage,
	}

	// If strategy is "replication", disable time-based purge in DB by setting retention to 0.
	// Store will manage purging manually.
	if walStrategy == "replication" {
		opts.WALRetentionTime = 0
	} else {
		// Default time-based
		opts.WALRetentionTime = 2 * time.Hour
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

	if slot, ok := s.replicas[id]; ok {
		// Update existing slot status to Connected
		slot.Connected = true
		// We trust the handshake logSeq as the starting point for this session
		if logSeq > slot.LogSeq {
			slot.LogSeq = logSeq
		}
		slot.Role = role // Update role if changed
		slot.LastSeen = time.Now()
	} else {
		// Create new persistent slot
		s.replicas[id] = &ReplicaSlot{
			LogSeq:    logSeq,
			Role:      role,
			LastSeen:  time.Now(),
			Connected: true,
		}
	}
	s.dirty = true
}

// UnregisterReplica marks the replica as disconnected but keeps the slot.
func (s *Store) UnregisterReplica(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		s.logger.Info("Replica disconnected", "id", id)
		slot.Connected = false
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

// getMinSlotLogSeq calculates the minimum LogSeq required by ANY registered client.
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
		// If no slots exist, we can arguably purge everything up to current checkpoint
		// BUT to be safe and match behavior of "no slots = no retention constraints",
		// we return MaxUint64 which effectively allows full purge if used as a ceiling,
		// or if interpreted as "constraint", it means no constraint.
		// Let's interpret it as: No slots -> Safe to purge everything that is checkpointed.
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

func (s *Store) runRetentionManager() {
	// Check retention every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		minReplicaSeq := s.getMinSlotLogSeq()
		lastCkpt := s.DB.GetLastCheckpointOpID()

		// Determine safe purge ID
		// It must be safe regarding replicas (minReplicaSeq)
		// AND safe regarding storage consistency (lastCkpt)
		safeID := lastCkpt
		if minReplicaSeq < safeID {
			safeID = minReplicaSeq
		}

		if safeID > 0 && safeID != math.MaxUint64 {
			// Trigger purge
			if err := s.DB.PurgeWAL(safeID); err != nil {
				s.logger.Error("Replication-based WAL purge failed", "err", err)
			}
		} else if minReplicaSeq == math.MaxUint64 {
			// No replicas registered. If configured for "replication" strategy, this implies
			// we can purge everything up to checkpoint (no one to save data for).
			// This prevents infinite WAL growth if user sets strategy=replication but adds no replicas.
			if err := s.DB.PurgeWAL(lastCkpt); err != nil {
				s.logger.Error("Fallback WAL purge failed", "err", err)
			}
		}
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

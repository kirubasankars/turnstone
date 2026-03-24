package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
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
	KeyCount   int64 // Number of live keys in the database
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

	// Leader-Propagated Safety Barrier
	// If we are a follower, the leader tells us what the global minimum sequence is.
	// We must NOT delete WAL files newer than this, to ensure we can promote to leader
	// and serve other stragglers.
	leaderSafeSeq uint64
}

func NewStore(dir string, logger *slog.Logger, minReplicas int, isSystem bool, walStrategy string, maxDiskUsage int) (*Store, error) {
	s := &Store{
		logger:        logger,
		startTime:     time.Now(),
		isSystem:      isSystem,
		minReplicas:   minReplicas,
		replicas:      make(map[string]*ReplicaSlot),
		slotsFile:     filepath.Join(dir, "replication.slots"),
		walStrategy:   walStrategy,
		leaderSafeSeq: math.MaxUint64, // Default to "Safe to delete everything" until leader says otherwise
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

	// Default values
	maxWALSize := 10 * 1024 * 1024

	// Allow Test Override
	if v := os.Getenv("TS_TEST_WAL_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			maxWALSize = i
		}
	}

	opts := stonedb.Options{
		MaxWALSize:           uint32(maxWALSize),
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

// SetLeaderSafeSeq updates the retention barrier received from the upstream leader.
func (s *Store) SetLeaderSafeSeq(seq uint64) {
	atomic.StoreUint64(&s.leaderSafeSeq, seq)
}

// GetMinSlotLogSeq calculates the minimum LogSeq required by ANY registered client.
func (s *Store) GetMinSlotLogSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	minSeq := uint64(math.MaxUint64)
	hasSlots := false

	for _, slot := range s.replicas {
		hasSlots = true
		if slot.LogSeq < minSeq {
			minSeq = slot.LogSeq
		}
		// Debug logging for slot state (verbose, so check level if possible, or keep minimal)
		// Removed per "keep clean" instruction, relying on EnforceRetentionPolicy debug logs.
	}

	if !hasSlots {
		return math.MaxUint64
	}
	return minSeq
}

func (s *Store) runRetentionManager() {
	// Check retention every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.EnforceRetentionPolicy()
	}
}

// EnforceRetentionPolicy runs the logic to determine which WAL files can be safely deleted.
// It considers local checkpoints, downstream replicas, and upstream leader constraints.
// This is public to allow deterministic testing.
func (s *Store) EnforceRetentionPolicy() {
	// 1. Constraint from Downstream (Our Followers)
	minReplicaSeq := s.GetMinSlotLogSeq()

	// 2. Constraint from Upstream (Our Leader)
	// If we are a leader, this is MaxUint64 (ignored).
	// If we are a follower, this is the SafePoint sent by the Leader.
	leaderSafeSeq := atomic.LoadUint64(&s.leaderSafeSeq)

	// 3. Constraint from Local Disk (Checkpoint)
	lastCkpt := s.DB.GetLastCheckpointOpID()

	// Logic:
	// We can only delete logs that are safe according to ALL constraints.
	// SafeID = Min(LocalCheckpoint, DownstreamReplicas, UpstreamLeader)

	safeID := lastCkpt
	constraintSource := "checkpoint"

	// If a downstream replica needs log X, we must keep it.
	if minReplicaSeq < safeID {
		safeID = minReplicaSeq
		constraintSource = "replica_lag"
	}

	// If the upstream leader says "Someone in the cluster needs log Y", we must keep it.
	// This enables us to serve that straggler if we are promoted.
	if leaderSafeSeq < safeID {
		safeID = leaderSafeSeq
		constraintSource = "leader_constraint"
	}

	// Only log DEBUG unless we are blocked significantly or purging
	s.logger.Debug("Retention check",
		"safe_seq", safeID,
		"constraint", constraintSource,
		"replica_min", minReplicaSeq,
		"leader_min", leaderSafeSeq,
		"checkpoint", lastCkpt,
	)

	if safeID > 0 && safeID != math.MaxUint64 {
		// Trigger purge
		if err := s.DB.PurgeWAL(safeID); err != nil {
			s.logger.Error("Replication-based WAL purge failed", "err", err)
		} else {
			// Only log if something was potentially deleted (PurgeWAL doesn't return count, 
			// but we can assume if safeID advanced significantly since last log it might interest op)
			// For now, keep it clean and rely on DB logs or Metrics.
		}
	} else if minReplicaSeq == math.MaxUint64 && leaderSafeSeq == math.MaxUint64 {
		// No replicas registered AND no upstream leader constraints.
		// This implies we can purge everything up to checkpoint (standalone mode or leaf with no followers).
		// This prevents infinite WAL growth if user sets strategy=replication but adds no replicas.
		
		// Rate limit this log to avoid spamming every 30s if system is idle
		// (Logic for rate limiting omitted for simplicity, downgrading to Debug)
		s.logger.Debug("No replication constraints, purging up to checkpoint", "ckpt", lastCkpt)
		
		if err := s.DB.PurgeWAL(lastCkpt); err != nil {
			s.logger.Error("Fallback WAL purge failed", "err", err)
		}
	}
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

	// Wait for Quorum if configured
	if s.minReplicas > 0 {
		s.waitForQuorum(s.DB.LastOpID())
	}

	return nil
}

// ReplicateBatch applies a batch from a leader (no quorum wait).
// Used by Followers.
func (s *Store) ReplicateBatch(entries []protocol.LogEntry) error {
	return s.ReplicateBatches([][]protocol.LogEntry{entries})
}

// ReplicateBatches applies multiple batches from a leader using group commit.
// Used by Followers.
func (s *Store) ReplicateBatches(batches [][]protocol.LogEntry) error {
	var vlogBatches [][]stonedb.ValueLogEntry

	for _, entries := range batches {
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
		vlogBatches = append(vlogBatches, vlogEntries)
	}

	return s.DB.ApplyBatches(vlogBatches)
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
	s.logger.Info("Closing store")
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
	keyCount, _ := s.DB.KeyCount()

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
		KeyCount:   keyCount,
	}
}

// RegisterReplica adds a replica slot to the tracking map (for stats only).
func (s *Store) RegisterReplica(id string, logSeq uint64, role string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if slot, ok := s.replicas[id]; ok {
		// Only log info if connection status changed or seq jumped significantly (optional noise reduction)
		if !slot.Connected {
			s.logger.Info("Replica reconnected", "id", id, "seq", logSeq, "role", role)
		} else {
			s.logger.Debug("Replica state update", "id", id, "seq", logSeq)
		}
		
		slot.Connected = true
		if logSeq > slot.LogSeq {
			slot.LogSeq = logSeq
		}
		slot.Role = role
		slot.LastSeen = time.Now()
	} else {
		s.logger.Info("New replica registered", "id", id, "seq", logSeq, "role", role)
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
		if slot.Connected {
			s.logger.Info("Replica disconnected", "id", id)
			slot.Connected = false
			s.dirty = true
		}
	}
}

// DeleteReplica explicitly removes a replication slot.
func (s *Store) DeleteReplica(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.replicas[id]; !ok {
		return protocol.ErrKeyNotFound
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
		// Only update if seq advances
		if logSeq > slot.LogSeq {
			slot.LogSeq = logSeq
			s.dirty = true
			s.cond.Broadcast() // Wake up quorum waiters
		}
		slot.LastSeen = time.Now()
	}
}

// waitForQuorum blocks until enough replicas with Role="server" have acknowledged the given logSeq.
func (s *Store) waitForQuorum(logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	startWait := time.Now()
	warned := false

	for {
		acks := 0
		for _, slot := range s.replicas {
			// Only count servers towards quorum, not CDC clients
			if slot.Role == "server" && slot.LogSeq >= logSeq {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return
		}

		// Log warning if waiting too long (> 5s)
		if !warned && time.Since(startWait) > 5*time.Second {
			s.logger.Warn("Slow quorum commit", "target_seq", logSeq, "current_acks", acks, "needed", s.minReplicas)
			warned = true
		}

		s.cond.Wait()
	}
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
		s.logger.Error("Failed to parse replication slots file", "err", err)
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
			if err := s.saveSlotsLocked(); err != nil {
				s.logger.Error("Failed to persist replica slots", "err", err)
			} else {
				s.dirty = false
			}
		}
		s.mu.Unlock()
	}
}

// saveSlotsLocked assumes mu is held
func (s *Store) saveSlotsLocked() error {
	data, err := json.MarshalIndent(s.replicas, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	tmp := s.slotsFile + ".tmp"

	// 1. Create/Truncate Temp File
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	// 2. Write Data
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}

	// 3. Fsync to ensure durability on disk
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync: %w", err)
	}

	// 4. Close
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	// 5. Atomic Rename
	if err := os.Rename(tmp, s.slotsFile); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

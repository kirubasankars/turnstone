package store

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	// quitCh is used to signal the network handler to drop the connection.
	// It is not serialized to JSON.
	quitCh chan struct{} `json:"-"`
}

// Store wraps stonedb.DB to provide a compatibility layer, stats, and replication logic.
type Store struct {
	*stonedb.DB
	logger      *slog.Logger
	startTime   time.Time
	isSystem    bool
	minReplicas int

	// Persistence Context
	dir    string
	dbOpts stonedb.Options

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
		Logger:              logger, // Ensure logger is passed down
	}

	// If strategy is "replication", disable time-based purge in DB by setting retention to 0.
	// Store will manage purging manually.
	if walStrategy == "replication" {
		opts.WALRetentionTime = 0
	} else {
		// Default time-based
		opts.WALRetentionTime = 2 * time.Hour
	}

	s := &Store{
		logger:        logger,
		startTime:     time.Now(),
		isSystem:      isSystem,
		minReplicas:   minReplicas,
		replicas:      make(map[string]*ReplicaSlot),
		slotsFile:     filepath.Join(dir, "replication.slots"),
		walStrategy:   walStrategy,
		leaderSafeSeq: math.MaxUint64, // Default to "Safe to delete everything" until leader says otherwise
		dir:           dir,
		dbOpts:        opts,
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

	db, err := stonedb.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	s.DB = db

	return s, nil
}

// Reset wipes the database and restarts it. This is used when a full snapshot
// is received from the leader, requiring a clean slate.
func (s *Store) Reset() error {
	s.logger.Warn("Resetting database state (Snapshot detected)")

	// 1. Disconnect any downstream consumers to prevent them from reading invalid state
	s.RemoveAllReplicas()

	// 2. Close the existing DB instance
	if err := s.DB.Close(); err != nil {
		return fmt.Errorf("close failed during reset: %w", err)
	}

	// 3. Wipe Data Files
	// We preserve the directory but remove all contents
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read dir failed: %w", err)
	}

	for _, e := range entries {
		// Delete everything including replication slots, logs, and index
		path := filepath.Join(s.dir, e.Name())
		if err := os.RemoveAll(path); err != nil {
			s.logger.Error("Failed to delete file during reset", "path", path, "err", err)
			return err
		}
	}

	// 4. Re-Open Database
	newDB, err := stonedb.Open(s.dir, s.dbOpts)
	if err != nil {
		return fmt.Errorf("reopen failed during reset: %w", err)
	}

	// 5. Swap Pointer
	// Note: There is a brief window where s.DB was closed/invalid.
	// Background tasks on s.Store might have errored during this time, which is expected.
	s.DB = newDB

	s.logger.Info("Database reset complete")
	return nil
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
	leaderSafeSeq := atomic.LoadUint64(&s.leaderSafeSeq)

	// 3. Constraint from Local Disk (Checkpoint)
	lastCkpt := s.DB.GetLastCheckpointOpID()

	safeID := lastCkpt
	constraintSource := "checkpoint"

	if minReplicaSeq < safeID {
		safeID = minReplicaSeq
		constraintSource = "replica_lag"
	}

	if leaderSafeSeq < safeID {
		safeID = leaderSafeSeq
		constraintSource = "leader_constraint"
	}

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
			// Ignore closed errors if we are resetting
			if !strings.Contains(err.Error(), "closed") {
				s.logger.Error("Replication-based WAL purge failed", "err", err)
			}
		}
	} else if minReplicaSeq == math.MaxUint64 && leaderSafeSeq == math.MaxUint64 {
		s.logger.Debug("No replication constraints, purging up to checkpoint", "ckpt", lastCkpt)
		if err := s.DB.PurgeWAL(lastCkpt); err != nil {
			if !strings.Contains(err.Error(), "closed") {
				s.logger.Error("Fallback WAL purge failed", "err", err)
			}
		}
	}
}

// ApplyBatch applies a batch of protocol entries.
func (s *Store) ApplyBatch(entries []protocol.LogEntry) error {
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

	if s.minReplicas > 0 {
		s.waitForQuorum(s.DB.LastOpID())
	}

	return nil
}

// ReplicateBatch applies a batch from a leader (no quorum wait).
func (s *Store) ReplicateBatch(entries []protocol.LogEntry) error {
	return s.ReplicateBatches([][]protocol.LogEntry{entries})
}

// ReplicateBatches applies multiple batches from a leader using group commit.
func (s *Store) ReplicateBatches(batches [][]protocol.LogEntry) error {
	var vlogBatches [][]stonedb.ValueLogEntry

	for _, entries := range batches {
		var vlogEntries []stonedb.ValueLogEntry
		for _, e := range entries {
			vEntry := stonedb.ValueLogEntry{
				Key:           e.Key,
				Value:         e.Value,
				OperationID:   e.LogSeq,
				TransactionID: e.LogSeq,
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

// GetReplicaSignalChannel returns the kill-switch channel for a specific replica ID.
func (s *Store) GetReplicaSignalChannel(id string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		return slot.quitCh
	}
	return nil
}

// RegisterReplica adds or resets a replica slot in the tracking map.
func (s *Store) RegisterReplica(id string, logSeq uint64, role string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.replicas[id]; ok {
		s.logger.Info("Replica re-registered (slot reset for this db)",
			"id", id,
			"old_seq", old.LogSeq,
			"new_seq", logSeq,
			"role", role,
		)
	} else {
		s.logger.Info("New replica registered", "id", id, "seq", logSeq, "role", role)
	}

	s.replicas[id] = &ReplicaSlot{
		LogSeq:    logSeq,
		Role:      role,
		LastSeen:  time.Now(),
		Connected: true,
		quitCh:    make(chan struct{}),
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
	slot, ok := s.replicas[id]
	if !ok {
		return protocol.ErrKeyNotFound
	}

	if slot.quitCh != nil {
		select {
		case <-slot.quitCh:
		default:
			close(slot.quitCh)
		}
	}

	delete(s.replicas, id)
	s.dirty = true
	s.logger.Info("Replica slot deleted", "id", id)
	return nil
}

// RemoveAllReplicas drops all connected replicas and CDC clients.
func (s *Store) RemoveAllReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.replicas) == 0 {
		return
	}

	s.logger.Info("Disconnecting and removing all replicas (role change/reset)", "count", len(s.replicas))

	for _, slot := range s.replicas {
		if slot.quitCh != nil {
			select {
			case <-slot.quitCh:
			default:
				close(slot.quitCh)
			}
		}
	}

	s.replicas = make(map[string]*ReplicaSlot)
	s.dirty = true
	s.cond.Broadcast()
}

// UpdateReplicaLogSeq updates the acked sequence for a replica.
func (s *Store) UpdateReplicaLogSeq(id string, logSeq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		if logSeq > slot.LogSeq {
			slot.LogSeq = logSeq
			s.dirty = true
			s.cond.Broadcast()
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
			if slot.Role == "server" && slot.LogSeq >= logSeq {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return
		}

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

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	if err := os.Rename(tmp, s.slotsFile); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

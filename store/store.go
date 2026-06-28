// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
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
	Offset     int64
	Conflicts  uint64
	ReplicaLag uint64
	WALFiles   int   // always 1 (single data.log)
	WALSize    int64 // logical size of data.log
	VLogFiles  int   // deprecated, always 0
	VLogSize   int64 // allocated on-disk bytes (sparse)
	KeyCount   int64
}

const (
	StateUndefined    = "UNDEFINED"
	StatePrimary      = "PRIMARY"
	StateReplica      = "REPLICA"
	StateSteppingDown = "STEPPING_DOWN"
)

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
	minReplicas int

	// Persistence Context
	dir    string
	dbOpts stonedb.Options

	// Replication State
	mu             sync.Mutex
	dbMu           sync.RWMutex            // Protects s.DB pointer and state
	replicas       map[string]*ReplicaSlot // ReplicaID -> Slot State
	cond           *sync.Cond
	slotsFile      string
	dirty          bool
	walStrategy    string
	state          string // Current Database State (UNDEFINED, PRIMARY, REPLICA)
	replicaTimeout time.Duration

	// Coordination for StepDown
	safePointCh chan struct{} // Signal to force broadcast of SafePoint

	// Leader-Propagated Safety Barrier
	// If we are a follower, the leader tells us what the global minimum sequence is.
	// We must NOT delete WAL files newer than this, to ensure we can promote to leader
	// and serve other stragglers.
	leaderSafeSeq uint64

	// adminMu serializes administrative role-transition commands (REPLICAOF,
	// PROMOTE, STEPDOWN) for this database. Each of those handlers reads
	// GetState(), validates it, and only later calls SetState()/Promote()/
	// AddReplica() -- without a lock spanning that whole sequence, two
	// concurrent admin connections could both pass a stale state check
	// (e.g. both see UNDEFINED) and then race into conflicting transitions.
	adminMu sync.Mutex

	closeCh chan struct{}
	closed  int32
}

// LockAdmin acquires the per-database administrative serialization lock.
// Callers must pair this with UnlockAdmin (typically via defer) and hold it
// across the entire read-state -> validate -> mutate-state sequence of a
// REPLICAOF/PROMOTE/STEPDOWN command.
func (s *Store) LockAdmin() {
	s.adminMu.Lock()
}

// UnlockAdmin releases the per-database administrative serialization lock.
func (s *Store) UnlockAdmin() {
	s.adminMu.Unlock()
}

func NewStore(ctx context.Context, dir string, logger *slog.Logger, minReplicas int, walStrategy string, maxDiskUsage int) (*Store, error) {
	truncateWAL := false
	if os.Getenv("TS_TEST_WAL_TRUNCATE") == "true" {
		truncateWAL = true
	}

	opts := stonedb.Options{
		TruncateCorruptWAL:  truncateWAL,
		MaxDiskUsagePercent: maxDiskUsage,
		Logger:              logger,
		UnsafeDisableFsync:  os.Getenv("TS_UNSAFE_DISABLE_FSYNC") == "true",
	}

	s := &Store{
		logger:         logger,
		startTime:      time.Now(),
		minReplicas:    minReplicas,
		replicas:       make(map[string]*ReplicaSlot),
		slotsFile:      filepath.Join(dir, "replication.slots"),
		walStrategy:    walStrategy,
		leaderSafeSeq:  math.MaxUint64, // Default to "Safe to delete everything" until leader says otherwise
		dir:            dir,
		dbOpts:         opts,
		state:          StateUndefined,
		safePointCh:    make(chan struct{}),
		replicaTimeout: 1 * time.Minute, // Default strict timeout for lagging replicas
		closeCh:        make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)

	// Load existing persistence state (if any)
	s.loadSlots()

	db, err := stonedb.OpenContext(ctx, dir, opts)
	if err != nil {
		return nil, err
	}
	s.DB = db

	// Start background loops only after DB is ready (Open can take a while during replay).
	go s.runPersistence()
	if s.walStrategy == "replication" {
		go s.runRetentionManager()
		go s.runReplicaEviction()
	}

	return s, nil
}

// Reset performs a hard wipe of the database.
// WARNING: This operation deletes all data on disk and starts fresh.
// As per configuration, no backup is created.
func (s *Store) Reset() error {
	s.logger.Warn("Resetting database (Destructive wipe requested)")

	// 1. Disconnect any downstream consumers to prevent them from reading invalid state
	s.RemoveAllReplicas()

	// 2. Lock for Write: Exclusive access to swap s.DB
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	// 3. Close the existing DB instance
	if err := s.DB.Close(); err != nil {
		return fmt.Errorf("close failed during reset: %w", err)
	}

	// 4. Delete Data
	// Iterate and delete to preserve the root folder permissions if possible,
	// but failing that, we just recreate.
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read dir failed: %w", err)
	}

	for _, e := range entries {
		path := filepath.Join(s.dir, e.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("failed to wipe path %s: %w", path, err)
		}
	}

	// 5. Re-Open Database
	newDB, err := stonedb.OpenContext(context.Background(), s.dir, s.dbOpts)
	if err != nil {
		return fmt.Errorf("reopen failed after wipe: %w", err)
	}

	// 6. Swap Pointer
	s.DB = newDB

	// Reset leader constraint on reset (we are starting fresh)
	s.SetLeaderSafeSeq(math.MaxUint64)

	s.logger.Info("Database reset complete (Data wiped)")
	return nil
}

// IsValidReplicationCursor reports whether offset is a valid Hello resume point
// for physical WAL streaming on this node.
func (s *Store) IsValidReplicationCursor(offset uint64) bool {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return false
	}
	head := s.DB.LastLogOffset()
	cursor := int64(offset)
	if cursor < 0 {
		return false
	}
	if cursor == 0 || uint64(cursor) == uint64(head) {
		return true
	}
	if cursor > head {
		return false
	}
	return s.DB.IsValidFrameOffset(cursor)
}

// SetLeaderSafeSeq updates the retention barrier received from the upstream leader.
func (s *Store) SetLeaderSafeSeq(seq uint64) {
	atomic.StoreUint64(&s.leaderSafeSeq, seq)
}

// GetLeaderSafeSeq returns the leader-propagated retention barrier.
func (s *Store) GetLeaderSafeSeq() uint64 {
	return atomic.LoadUint64(&s.leaderSafeSeq)
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
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.EnforceRetentionPolicy()
		}
	}
}

// runReplicaEviction implements SELF-HEALING for stuck replicas.
func (s *Store) runReplicaEviction() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
			s.evictZombieReplicas()
		}
	}
}

func (s *Store) evictZombieReplicas() {
	// Get current head to determine if replicas are actually lagging.
	// Goes through the dbMu-guarded wrapper (not s.DB.LastLogOffset()
	// directly): s.DB itself can be swapped concurrently by Store.Reset(),
	// which holds dbMu.Lock() while doing so.
	headSeq := s.LastLogOffset()

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, slot := range s.replicas {
		// A replica is a "Zombie" if:
		// 1. It is lagging (slot.LogSeq < headSeq) -> It is holding back WAL purging.
		// 2. It hasn't been seen/acked in > replicaTimeout.
		// If a replica is caught up (LogSeq == headSeq), we tolerate idleness because it's not blocking WAL.

		if slot.LogSeq < headSeq && time.Since(slot.LastSeen) > s.replicaTimeout {
			s.logger.Warn("Evicting zombie replica (blocking WAL retention)",
				"replica_id", id,
				"lag", headSeq-slot.LogSeq,
				"last_seen", time.Since(slot.LastSeen),
			)

			// Signal network handler to close
			if slot.quitCh != nil {
				select {
				case <-slot.quitCh:
				default:
					close(slot.quitCh)
				}
			}

			// Remove from map immediately to unblock GetMinSlotLogSeq
			delete(s.replicas, id)
			s.dirty = true
		}
	}
}

// EnforceRetentionPolicy runs the logic to determine which WAL files can be safely deleted.
// It considers local checkpoints, downstream replicas, and upstream leader constraints.
// This is public to allow deterministic testing.
func (s *Store) EnforceRetentionPolicy() {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return
	}

	// 1. Constraint from Downstream (Our Followers)
	minReplicaSeq := s.GetMinSlotLogSeq()

	// 2. Constraint from Upstream (Our Leader)
	leaderSafeSeq := atomic.LoadUint64(&s.leaderSafeSeq)

	// 3. Constraint from Local Disk (Checkpoint)
	lastCkpt := s.DB.GetLastCheckpointOffset()

	safeID := lastCkpt
	constraintSource := "checkpoint"

	if minReplicaSeq != math.MaxUint64 {
		minReplica := int64(minReplicaSeq)
		if minReplica < safeID {
			safeID = minReplica
			constraintSource = "replica_lag"
		}
	}

	if leaderSafeSeq != math.MaxUint64 {
		leaderSafe := int64(leaderSafeSeq)
		if leaderSafe < safeID {
			safeID = leaderSafe
			constraintSource = "leader_constraint"
		}
	}

	s.logger.Debug("Retention check",
		"safe_seq", safeID,
		"constraint", constraintSource,
		"replica_min", minReplicaSeq,
		"leader_min", leaderSafeSeq,
		"checkpoint", lastCkpt,
	)

	if safeID > 0 {
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
	minReplicas := s.MinReplicas()

	// PATCH 3: Strict Sync Replication (Wait for Replicas BEFORE locking)
	// If configured for sync replication, verify we have enough healthy replicas connected
	// to satisfy quorum *before* attempting the commit. This fails fast if the cluster is degraded.
	if minReplicas > 0 {
		healthy := s.HealthyReplicaCount()
		if healthy < minReplicas {
			return fmt.Errorf("insufficient replicas for safe write: have %d, need %d", healthy, minReplicas)
		}
	}

	var commitEndOffset uint64
	err := func() error {
		s.dbMu.RLock()
		defer s.dbMu.RUnlock()

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
		commitEndOffset = uint64(s.DB.LastLogOffset())
		return nil
	}()
	if err != nil {
		return err
	}

	if minReplicas > 0 {
		// Deliberately called after releasing dbMu above: WaitForQuorum can
		// block for seconds, and holding dbMu.RLock() across it would stall
		// every other reader/writer (including Store.Reset's dbMu.Lock())
		// for as long as replicas are catching up -- a single slow write
		// could otherwise freeze the whole store.
		return s.WaitForQuorum(commitEndOffset, 0, nil)
	}

	return nil
}

// ApplyLogSegment appends a raw, statement-aligned WAL byte range.
func (s *Store) ApplyLogSegment(data []byte) (int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.DB.ApplyLogSegment(data)
}

// ReadLogSegment reads complete WAL frames from a byte offset on the leader log.
func (s *Store) ReadLogSegment(startOffset int64, maxBytes int64) ([]byte, int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.DB.ReadLogSegment(startOffset, maxBytes)
}

// Get retrieves a value by key.
func (s *Store) Get(key string) ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

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
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	s.logger.Debug("Closing store")
	close(s.closeCh)

	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

// LastLogOffset returns the exclusive end of the local WAL (next byte to read).
func (s *Store) LastLogOffset() uint64 {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return 0
	}
	return uint64(s.DB.LastLogOffset())
}

// Stats returns usage statistics.
func (s *Store) Stats() StoreStats {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	_, logical, allocated := s.DB.StorageStats()
	keyCount, _ := s.DB.KeyCount()
	head := s.LastLogOffset()

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
		WALFiles:   1,
		WALSize:    logical,
		VLogFiles:  0,
		VLogSize:   allocated,
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
	// Notify waiters that a new replica joined (might satisfy quorum)
	s.cond.Broadcast()
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

// RemoveAllReplicas drops all connected replicas.
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

// MinReplicas returns the configured minimum number of replicas required for quorum.
func (s *Store) MinReplicas() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.minReplicas
}

// SetMinReplicas updates the minimum number of replicas required for quorum.
func (s *Store) SetMinReplicas(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minReplicas = n
	// Broadcast to wake up any blocked WaitForQuorum calls
	s.cond.Broadcast()
}

// defaultQuorumTimeout bounds WaitForQuorum when the caller doesn't specify
// one. An unbounded wait would let a single write hang its caller (and,
// transitively, any client blocked on that response) forever if replicas
// never catch up -- e.g. every replica disconnected after the commit but
// before acking it.
const defaultQuorumTimeout = 30 * time.Second

// WaitForQuorum blocks until enough currently-connected replicas with
// Role="server" have acknowledged the given logSeq, or timeout elapses
// (defaultQuorumTimeout if timeout <= 0), in which case it returns an error
// instead of hanging. If cancel is non-nil and is closed while waiting,
// WaitForQuorum returns early with an error -- callers use this to give up
// promptly if e.g. the client that requested the commit has already
// disconnected, instead of always holding the wait (and whatever resources
// the caller holds, such as a connection-semaphore slot) for the full
// timeout.
func (s *Store) WaitForQuorum(logSeq uint64, timeout time.Duration, cancel <-chan struct{}) error {
	if timeout <= 0 {
		timeout = defaultQuorumTimeout
	}
	deadline := time.Now().Add(timeout)

	s.mu.Lock()
	defer s.mu.Unlock()

	startWait := time.Now()
	warned := false

	for {
		acks := 0
		for _, slot := range s.replicas {
			// Only a slot's *currently connected* replica counts towards
			// quorum. A disconnected slot's stale high-water LogSeq (e.g.
			// reloaded verbatim from slots.json on process restart, before
			// that replica has actually reconnected) must not be able to
			// satisfy quorum for a replica that isn't actually there right
			// now to receive/ack future writes.
			if slot.Connected && slot.Role == "server" && slot.LogSeq >= logSeq {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return nil
		}

		if !warned && time.Since(startWait) > 5*time.Second {
			s.logger.Warn("Slow quorum commit", "target_seq", logSeq, "current_acks", acks, "needed", s.minReplicas)
			warned = true
		}

		if cancel != nil {
			select {
			case <-cancel:
				return fmt.Errorf("quorum wait cancelled: have %d/%d acks for seq %d", acks, s.minReplicas, logSeq)
			default:
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("timeout waiting for replication quorum: have %d/%d acks for seq %d", acks, s.minReplicas, logSeq)
		}

		// sync.Cond has no timed-wait primitive; a timer that forces a
		// periodic wakeup (rebroadcasting into the same Cond) is what
		// actually enforces the deadline/cancel check, since otherwise
		// Wait() only returns when a replica ack/registration/
		// SetMinReplicas calls Broadcast -- which may never happen again
		// if every replica has gone silent.
		wake := remaining
		if wake > time.Second {
			wake = time.Second
		}
		timer := time.AfterFunc(wake, func() {
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		})
		s.cond.Wait()
		timer.Stop()
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
		return
	}
	// A freshly (re)started process has no live connections yet, no matter
	// what the persisted snapshot says: whatever TCP connection a slot had
	// when this file was last saved is long gone. Force every reloaded slot
	// to Connected=false so it can't falsely satisfy WaitForQuorum's quorum
	// gate until that replica actually reconnects and re-registers.
	for _, slot := range s.replicas {
		slot.Connected = false
	}
	// CHANGED: Reduced from INFO to DEBUG
	s.logger.Debug("Loaded replication slots", "count", len(s.replicas))
}

func (s *Store) runPersistence() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeCh:
			return
		case <-ticker.C:
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

// SetState updates the database state.
func (s *Store) SetState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.dirty = true
	// Trigger waiters (e.g. WaitForPrimary?)
	s.cond.Broadcast()
}

// Promote sets the database state to Primary.
func (s *Store) Promote() error {
	// Reset leader constraint as we are now the leader
	s.SetLeaderSafeSeq(math.MaxUint64)

	s.SetState(StatePrimary)
	s.TriggerSafePoint()
	return nil
}

// GetState returns the current database state.
func (s *Store) GetState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// HealthyReplicaCount returns the number of connected replicas with 'server' role.
func (s *Store) HealthyReplicaCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, slot := range s.replicas {
		if slot.Connected && slot.Role == "server" {
			count++
		}
	}
	return count
}

// SafePointSignal returns a channel that forces a SafePoint broadcast.
func (s *Store) SafePointSignal() <-chan struct{} {
	return s.safePointCh
}

// TriggerSafePoint signals replication streams to send a SafePoint immediately
func (s *Store) TriggerSafePoint() {
	select {
	case s.safePointCh <- struct{}{}:
	default:
	}
}

// WaitForActiveTransactions blocks until active transaction count is 0 or timeout
func (s *Store) WaitForActiveTransactions(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		s.dbMu.RLock()
		count := s.DB.ActiveTransactionCount()
		s.dbMu.RUnlock()
		if count == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for active transactions")
		}
		<-ticker.C
	}
}

// AbortAllActiveWriteTransactions force-aborts every in-progress, locally-
// owned RW transaction. Callers use this when a bounded drain
// (WaitForActiveTransactions) times out but they still need a hard
// guarantee that nothing can commit locally after this call returns -- e.g.
// STEPDOWN, which must not let a straggler transaction commit after the
// final safe-point has already been broadcast to replicas.
func (s *Store) AbortAllActiveWriteTransactions() {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.DB.AbortAllActiveWriteTransactions()
}

// WaitForReplication blocks until all connected replicas have acked the current log head.
func (s *Store) WaitForReplication(timeout time.Duration) error {
	headOffset := s.LastLogOffset()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		min := s.GetMinSlotLogSeq()
		// Infinite min seq (MaxUint64) means no replicas, which implies "synced" (nothing to sync to)
		if min == math.MaxUint64 {
			return nil // No replicas to wait for
		}

		if min >= headOffset {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for replication sync")
		}
		<-ticker.C
	}
}

// ResetReplicas clears all replica slots
func (s *Store) ResetReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas = make(map[string]*ReplicaSlot)
	if err := s.saveSlotsLocked(); err != nil {
		s.logger.Error("Failed to save slots after reset", "err", err)
	}
}

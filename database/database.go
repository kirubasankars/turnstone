// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package database

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

	"turnstone/engine"
	"turnstone/protocol"
)

// Stats holds basic metrics.
type Stats struct {
	ActiveTxs    int
	Uptime       string
	Offset       int64
	Conflicts    uint64
	ReplicaLag   uint64
	LogSize      int64 // logical size of data.log
	LogAllocated int64 // allocated on-disk bytes (sparse)
	KeyCount     int64
}

const (
	StateUndefined    = "UNDEFINED"
	StatePrimary      = "PRIMARY"
	StateReplica      = "REPLICA"
	StateSteppingDown = "STEPPING_DOWN"
)

// ReplicaSlot tracks the state of a connected replication consumer.
type ReplicaSlot struct {
	Offset    uint64    `json:"offset"`
	Role      string    `json:"role"`
	LastSeen  time.Time `json:"last_seen"`
	Connected bool      `json:"connected"`

	// quitCh is used to signal the network handler to drop the connection.
	// It is not serialized to JSON.
	quitCh chan struct{} `json:"-"`
}

// Database is one keyspace: an engine.DB plus role state, replica slots, and retention.
type Database struct {
	*engine.DB
	logger      *slog.Logger
	startTime   time.Time
	minReplicas int

	// Persistence Context
	dir    string
	dbOpts engine.Options

	// Replication State
	mu                sync.Mutex
	dbMu              sync.RWMutex            // Protects s.DB pointer and state
	replicas          map[string]*ReplicaSlot // ReplicaID -> Slot State
	cond              *sync.Cond
	slotsFile         string
	dirty             bool
	retentionStrategy string
	state             string // Current Database State (UNDEFINED, PRIMARY, REPLICA)
	replicaTimeout    time.Duration

	// Coordination for StepDown
	safePointCh chan struct{} // Signal to force broadcast of SafePoint

	// Leader-Propagated Safety Barrier
	// If we are a follower, the leader tells us the cluster retain offset.
	// We must not raise the local scan floor past this, so we can promote
	// and still serve stragglers.
	leaderRetainOffset uint64

	// adminMu serializes administrative role-transition commands (REPLICAOF,
	// PROMOTE, STEPDOWN) for this database. Each of those handlers reads
	// GetState(), validates it, and only later calls SetState()/Promote()/
	// Follow() -- without a lock spanning that whole sequence, two
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
func (s *Database) LockAdmin() {
	s.adminMu.Lock()
}

// UnlockAdmin releases the per-database administrative serialization lock.
func (s *Database) UnlockAdmin() {
	s.adminMu.Unlock()
}

func Open(ctx context.Context, dir string, logger *slog.Logger, minReplicas int, retentionStrategy string, maxDiskUsage int) (*Database, error) {
	truncateTail := false
	if os.Getenv("TS_TEST_LOG_TRUNCATE") == "true" {
		truncateTail = true
	}

	opts := engine.Options{
		TruncateCorruptTail: truncateTail,
		MaxDiskUsagePercent: maxDiskUsage,
		Logger:              logger,
		UnsafeDisableFsync:  os.Getenv("TS_UNSAFE_DISABLE_FSYNC") == "true",
	}

	s := &Database{
		logger:             logger,
		startTime:          time.Now(),
		minReplicas:        minReplicas,
		replicas:           make(map[string]*ReplicaSlot),
		slotsFile:          filepath.Join(dir, "repl.slots"),
		retentionStrategy:  retentionStrategy,
		leaderRetainOffset: math.MaxUint64, // Default to "Safe to delete everything" until leader says otherwise
		dir:                dir,
		dbOpts:             opts,
		state:              StateUndefined,
		safePointCh:        make(chan struct{}),
		replicaTimeout:     1 * time.Minute, // Default strict timeout for lagging replicas
		closeCh:            make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)

	// Load existing persistence state (if any)
	s.loadSlots()

	db, err := engine.OpenContext(ctx, dir, opts)
	if err != nil {
		return nil, err
	}
	s.DB = db

	// Start background loops only after DB is ready (Open can take a while during replay).
	go s.runPersistence()
	if s.retentionStrategy == "replication" {
		go s.runRetentionManager()
		go s.runReplicaEviction()
	}

	return s, nil
}

// Reset performs a hard wipe of the database.
// WARNING: This operation deletes all data on disk and starts fresh.
// As per configuration, no backup is created.
func (s *Database) Reset() error {
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
	newDB, err := engine.OpenContext(context.Background(), s.dir, s.dbOpts)
	if err != nil {
		return fmt.Errorf("reopen failed after wipe: %w", err)
	}

	// 6. Swap Pointer
	s.DB = newDB

	// Reset leader constraint on reset (we are starting fresh)
	s.SetLeaderRetainOffset(math.MaxUint64)

	s.logger.Info("Database reset complete (Data wiped)")
	return nil
}

// IsValidReplicationCursor reports whether offset is a valid Hello resume point
// for physical log streaming on this node.
func (s *Database) IsValidReplicationCursor(offset uint64) bool {
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

// SetLeaderRetainOffset updates the retention barrier received from the upstream leader.
func (s *Database) SetLeaderRetainOffset(offset uint64) {
	atomic.StoreUint64(&s.leaderRetainOffset, offset)
}

// GetLeaderRetainOffset returns the leader-propagated retention barrier.
func (s *Database) GetLeaderRetainOffset() uint64 {
	return atomic.LoadUint64(&s.leaderRetainOffset)
}

// MinReplicaOffset calculates the minimum Offset required by ANY registered client.
func (s *Database) MinReplicaOffset() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	minOffset := uint64(math.MaxUint64)
	hasSlots := false

	for _, slot := range s.replicas {
		hasSlots = true
		if slot.Offset < minOffset {
			minOffset = slot.Offset
		}
	}

	if !hasSlots {
		return math.MaxUint64
	}
	return minOffset
}

func (s *Database) runRetentionManager() {
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
func (s *Database) runReplicaEviction() {
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

func (s *Database) evictZombieReplicas() {
	// Get current head to determine if replicas are actually lagging.
	// Goes through the dbMu-guarded wrapper (not s.DB.LastLogOffset()
	// directly): s.DB itself can be swapped concurrently by Database.Reset(),
	// which holds dbMu.Lock() while doing so.
	headOffset := s.LastLogOffset()

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, slot := range s.replicas {
		// A replica is a "Zombie" if:
		// 1. It is lagging (slot.Offset < headOffset) -> It is holding back retention.
		// 2. It hasn't been seen/acked in > replicaTimeout.
		// If a replica is caught up (Offset == headOffset), we tolerate idleness because it's not blocking retention.

		if slot.Offset < headOffset && time.Since(slot.LastSeen) > s.replicaTimeout {
			s.logger.Warn("Evicting zombie replica (blocking log retention)",
				"replica_id", id,
				"lag", headOffset-slot.Offset,
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

			// Remove from map immediately to unblock MinReplicaOffset
			delete(s.replicas, id)
			s.dirty = true
		}
	}
}

// EnforceRetentionPolicy raises the log scan floor from replica acks, the
// leader retain offset, and the local retention mark. The log file is not truncated.
func (s *Database) EnforceRetentionPolicy() {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return
	}

	// 1. Constraint from Downstream (Our Followers)
	minReplicaOffset := s.MinReplicaOffset()

	// 2. Constraint from Upstream (Our Leader)
	leaderRetainOffset := atomic.LoadUint64(&s.leaderRetainOffset)

	// 3. Constraint from Local Disk (MarkRetention)
	lastRetention := s.DB.RetentionOffset()

	safeID := lastRetention
	constraintSource := "retention"

	if minReplicaOffset != math.MaxUint64 {
		minReplica := int64(minReplicaOffset)
		if minReplica < safeID {
			safeID = minReplica
			constraintSource = "replica_lag"
		}
	}

	if leaderRetainOffset != math.MaxUint64 {
		leaderSafe := int64(leaderRetainOffset)
		if leaderSafe < safeID {
			safeID = leaderSafe
			constraintSource = "leader_constraint"
		}
	}

	s.logger.Debug("Retention check",
		"safe_offset", safeID,
		"constraint", constraintSource,
		"replica_min", minReplicaOffset,
		"leader_min", leaderRetainOffset,
		"retention", lastRetention,
	)

	if safeID > 0 {
		// Trigger purge
		if err := s.DB.SetScanFloor(safeID); err != nil {
			// Ignore closed errors if we are resetting
			if !strings.Contains(err.Error(), "closed") {
				s.logger.Error("replication scan floor update failed", "err", err)
			}
		}
	} else if minReplicaOffset == math.MaxUint64 && leaderRetainOffset == math.MaxUint64 {
		s.logger.Debug("No replication constraints, raising scan floor to retention mark", "offset", lastRetention)
		if err := s.DB.SetScanFloor(lastRetention); err != nil {
			if !strings.Contains(err.Error(), "closed") {
				s.logger.Error("fallback scan floor update failed", "err", err)
			}
		}
	}
}

// ApplyLogRange appends a raw, statement-aligned log byte range.
func (s *Database) ApplyLogRange(data []byte) (int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.DB.ApplyLogRange(data)
}

// ReadLogRange reads complete log frames from a byte offset on the leader log.
func (s *Database) ReadLogRange(startOffset int64, maxBytes int64) ([]byte, int64, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	return s.DB.ReadLogRange(startOffset, maxBytes)
}

// Get retrieves a value by key.
func (s *Database) Get(key string) ([]byte, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	tx := s.DB.NewTransaction(false)
	defer tx.Discard()
	val, err := tx.Get([]byte(key))
	if err == engine.ErrKeyNotFound {
		return nil, protocol.ErrKeyNotFound
	}
	return val, err
}

// Close closes the underlying StoneDB instance.
func (s *Database) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}
	s.logger.Debug("Closing database")
	close(s.closeCh)

	s.dbMu.Lock()
	defer s.dbMu.Unlock()
	if s.DB == nil {
		return nil
	}
	return s.DB.Close()
}

// LastLogOffset returns the exclusive end of the local log (next byte to write).
func (s *Database) LastLogOffset() uint64 {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.DB == nil {
		return 0
	}
	return uint64(s.DB.LastLogOffset())
}

// Stats returns usage statistics.
func (s *Database) Stats() Stats {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	logical, allocated := s.DB.StorageStats()
	keyCount, _ := s.DB.KeyCount()
	head := s.LastLogOffset()

	minLag := uint64(0)
	first := true

	s.mu.Lock()
	for _, r := range s.replicas {
		lag := uint64(0)
		if head > r.Offset {
			lag = head - r.Offset
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

	return Stats{
		ActiveTxs:    s.DB.ActiveTransactionCount(),
		Uptime:       time.Since(s.startTime).Round(time.Second).String(),
		Offset:       int64(head),
		Conflicts:    s.DB.GetConflicts(),
		ReplicaLag:   minLag,
		LogSize:      logical,
		LogAllocated: allocated,
		KeyCount:     keyCount,
	}
}

// GetReplicaSignalChannel returns the kill-switch channel for a specific replica ID.
func (s *Database) GetReplicaSignalChannel(id string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		return slot.quitCh
	}
	return nil
}

// RegisterReplica adds or resets a replica slot in the tracking map.
func (s *Database) RegisterReplica(id string, offset uint64, role string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.replicas[id]; ok {
		s.logger.Info("Replica re-registered (slot reset for this db)",
			"id", id,
			"old_offset", old.Offset,
			"new_offset", offset,
			"role", role,
		)
	} else {
		s.logger.Info("New replica registered", "id", id, "offset", offset, "role", role)
	}

	s.replicas[id] = &ReplicaSlot{
		Offset:    offset,
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
func (s *Database) UnregisterReplica(id string) {
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
func (s *Database) RemoveAllReplicas() {
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

// UpdateReplicaOffset updates the acked sequence for a replica.
func (s *Database) UpdateReplicaOffset(id string, offset uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot, ok := s.replicas[id]; ok {
		if offset > slot.Offset {
			slot.Offset = offset
			s.dirty = true
			s.cond.Broadcast()
		}
		slot.LastSeen = time.Now()
	}
}

// MinReplicas returns the configured minimum number of replicas required for quorum.
func (s *Database) MinReplicas() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.minReplicas
}

// SetMinReplicas updates the minimum number of replicas required for quorum.
func (s *Database) SetMinReplicas(n int) {
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
// Role="server" have acknowledged the given offset, or timeout elapses
// (defaultQuorumTimeout if timeout <= 0), in which case it returns an error
// instead of hanging. If cancel is non-nil and is closed while waiting,
// WaitForQuorum returns early with an error -- callers use this to give up
// promptly if e.g. the client that requested the commit has already
// disconnected, instead of always holding the wait (and whatever resources
// the caller holds, such as a connection-semaphore slot) for the full
// timeout.
func (s *Database) WaitForQuorum(offset uint64, timeout time.Duration, cancel <-chan struct{}) error {
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
			// quorum. A disconnected slot's stale high-water Offset (e.g.
			// reloaded verbatim from slots.json on process restart, before
			// that replica has actually reconnected) must not be able to
			// satisfy quorum for a replica that isn't actually there right
			// now to receive/ack future writes.
			if slot.Connected && slot.Role == "server" && slot.Offset >= offset {
				acks++
			}
		}

		if acks >= s.minReplicas {
			return nil
		}

		if !warned && time.Since(startWait) > 5*time.Second {
			s.logger.Warn("Slow quorum commit", "target_seq", offset, "current_acks", acks, "needed", s.minReplicas)
			warned = true
		}

		if cancel != nil {
			select {
			case <-cancel:
				return fmt.Errorf("quorum wait cancelled: have %d/%d acks for offset %d", acks, s.minReplicas, offset)
			default:
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("timeout waiting for replication quorum: have %d/%d acks for offset %d", acks, s.minReplicas, offset)
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

func (s *Database) loadSlots() {
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

func (s *Database) runPersistence() {
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
func (s *Database) saveSlotsLocked() error {
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
func (s *Database) SetState(state string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.dirty = true
	// Trigger waiters (e.g. WaitForPrimary?)
	s.cond.Broadcast()
}

// Promote sets the database state to Primary.
func (s *Database) Promote() error {
	// Reset leader constraint as we are now the leader
	s.SetLeaderRetainOffset(math.MaxUint64)

	s.SetState(StatePrimary)
	s.TriggerSafePoint()
	return nil
}

// GetState returns the current database state.
func (s *Database) GetState() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// HealthyReplicaCount returns the number of connected replicas with 'server' role.
func (s *Database) HealthyReplicaCount() int {
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
func (s *Database) SafePointSignal() <-chan struct{} {
	return s.safePointCh
}

// TriggerSafePoint signals replication streams to send a SafePoint immediately
func (s *Database) TriggerSafePoint() {
	select {
	case s.safePointCh <- struct{}{}:
	default:
	}
}

// WaitForActiveTransactions blocks until active transaction count is 0 or timeout
func (s *Database) WaitForActiveTransactions(timeout time.Duration) error {
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
func (s *Database) AbortAllActiveWriteTransactions() {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.DB.AbortAllActiveWriteTransactions()
}

// WaitForReplication blocks until all connected replicas have acked the current log head.
func (s *Database) WaitForReplication(timeout time.Duration) error {
	headOffset := s.LastLogOffset()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		min := s.MinReplicaOffset()
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
func (s *Database) ResetReplicas() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicas = make(map[string]*ReplicaSlot)
	if err := s.saveSlotsLocked(); err != nil {
		s.logger.Error("Failed to save slots after reset", "err", err)
	}
}

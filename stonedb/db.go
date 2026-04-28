package stonedb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/protocol"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type commitRequest struct {
	tx   *Transaction
	resp chan error
}

type TimelineHistoryItem struct {
	TLI     uint64 `json:"tli"`
	StartOp uint64 `json:"start_op"`
	EndOp   uint64 `json:"end_op"`
}

type TimelineMeta struct {
	CurrentTimeline uint64                `json:"current_timeline"`
	History         []TimelineHistoryItem `json:"history"`
}

// DB is the main database struct.
type DB struct {
	dir                string
	ldb                *leveldb.DB
	writeAheadLog      *WriteAheadLog
	valueLog           *ValueLog
	deletedBytesByFile map[uint32]int64
	logger             *slog.Logger

	mu       sync.RWMutex
	commitMu sync.Mutex

	// Clocks
	transactionID uint64
	operationID   uint64
	keyCount      int64 // Persistent Key Count

	// Metrics (Atomic counters)
	metricsConflicts uint64

	// Transaction State (snapshot horizon tracking; value = snapshot.Xmax)
	activeTxnsMu   sync.Mutex
	activeTxns     map[*Transaction]uint64
	pendingDeletes []pendingFile

	// Postgres-style eager transaction bookkeeping.
	txMu         sync.Mutex
	activeXids   map[uint64]*Transaction // in-progress RW xids -> owner (nil = applied via replication)
	keyLocks     map[string]uint64       // key -> owning xid (first-writer-wins, NOWAIT)
	txStartTimes map[uint64]time.Time    // xid -> start time, local RW only (liveness reaper)
	beginOpIDs   map[uint64]uint64       // xid -> opID of its BEGIN record (WAL purge floor)
	txTimeout    time.Duration           // MaxTxDuration override for the liveness reaper

	// pendingClogRebuild holds clog decisions reconstructed from the WAL during
	// recovery, before LevelDB is open. Flushed by persistClogRebuild.
	pendingClogRebuild map[uint64]TxStatus

	// replImpact accumulates per-xid key-count/garbage impact for replicated
	// (ApplyRecord) writes, mirroring what a local Transaction tracks in
	// memory between Put/Delete and Commit. Flushed to keyCount/
	// deletedBytesByFile on a replicated COMMIT, discarded on ABORT.
	replImpact map[uint64]*replTxImpact

	// Background Tasks
	closeCh      chan struct{}
	wg           sync.WaitGroup
	lastCkptOpID uint64
	closed       int32

	// Group Commit Pipeline
	commitCh chan commitRequest

	// Config
	minGarbageThreshold    int64
	checksumInterval       time.Duration
	autoCheckpointInterval time.Duration
	compactionInterval     time.Duration
	maxDiskUsagePercent    int   // Configured threshold
	blockCacheSize         int   // Configured cache size
	isDiskFull             int32 // Atomic boolean (1=Full, 0=OK)
	isCorrupt              int32 // Atomic boolean (1=Corrupt, 0=OK) - Prevents writes after critical failure

	// Timeline Meta
	timelineMeta TimelineMeta
}

// Open initializes the DB.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}

	if opts.MaxVLogSize == 0 {
		opts.MaxVLogSize = int64(protocol.DefaultMaxVLogSize)
	}
	if opts.CompactionMinGarbage == 0 {
		opts.CompactionMinGarbage = 1024 * 1024
	}
	if opts.AutoCheckpointInterval == 0 {
		opts.AutoCheckpointInterval = 60 * time.Second
	}
	if opts.CompactionInterval == 0 {
		opts.CompactionInterval = 20 * time.Second
	}
	// Default Block Cache to 64MB if not specified
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 64 * 1024 * 1024
	}
	if opts.TxTimeout == 0 {
		opts.TxTimeout = protocol.MaxTxDuration
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	// Enrich logger with DB directory context for easier debugging in multi-db environments
	logger = logger.With("db_dir", filepath.Base(dir))

	meta, err := loadTimelineMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("load timeline meta: %w", err)
	}

	// WAL now depends on explicit rotation, MaxWALSize used for safety check only
	wal, err := OpenWriteAheadLog(filepath.Join(dir, "wal"), 0, meta.CurrentTimeline, logger)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	vl, err := OpenValueLog(filepath.Join(dir, "vlog"), opts.MaxVLogSize, logger)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("open vlog: %w", err)
	}

	db := &DB{
		dir:                    dir,
		writeAheadLog:          wal,
		valueLog:               vl,
		deletedBytesByFile:     make(map[uint32]int64),
		activeTxns:             make(map[*Transaction]uint64),
		activeXids:             make(map[uint64]*Transaction),
		keyLocks:               make(map[string]uint64),
		txStartTimes:           make(map[uint64]time.Time),
		beginOpIDs:             make(map[uint64]uint64),
		replImpact:             make(map[uint64]*replTxImpact),
		txTimeout:              opts.TxTimeout,
		closeCh:                make(chan struct{}),
		commitCh:               make(chan commitRequest, 500),
		minGarbageThreshold:    opts.CompactionMinGarbage,
		checksumInterval:       opts.ChecksumInterval,
		autoCheckpointInterval: opts.AutoCheckpointInterval,
		compactionInterval:     opts.CompactionInterval,
		maxDiskUsagePercent:    opts.MaxDiskUsagePercent,
		blockCacheSize:         opts.BlockCacheSize,
		timelineMeta:           meta,
		logger:                 logger,
	}

	logger.Debug("Opening Database", "vlog_max_size", opts.MaxVLogSize)

	if err := db.recoverValueLog(); err != nil {
		db.Close()
		return nil, fmt.Errorf("recover vlog: %w", err)
	}

	// FIX: Pass timeline history to ensure we don't replay orphaned writes
	if err := db.syncWALToValueLog(opts.TruncateCorruptWAL, meta.History); err != nil {
		db.Close()
		return nil, fmt.Errorf("sync wal: %w", err)
	}

	if err := db.openLevelDB(dir); err != nil {
		db.Close()
		return nil, err
	}

	// The clog can only be persisted once LevelDB is open; apply whatever was
	// reconstructed from the WAL replay above now.
	if err := db.persistClogRebuild(); err != nil {
		db.Close()
		return nil, fmt.Errorf("persist recovered clog: %w", err)
	}

	db.writeAheadLog.SetOnRotate(db.onWALRotate)

	if err := db.loadDeletedBytesStats(); err != nil {
		db.logger.Warn("Failed to load garbage stats", "err", err)
	}
	if err := db.loadKeyCount(); err != nil {
		db.logger.Warn("Failed to load key count (recounting)", "err", err)
	}

	db.startBackgroundTasks()

	return db, nil
}

func loadTimelineMeta(dir string) (TimelineMeta, error) {
	path := filepath.Join(dir, "timeline.meta")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// DEFAULT CHANGED: Start at Timeline 0 so first promotion is 1
		return TimelineMeta{CurrentTimeline: 0}, nil
	}
	if err != nil {
		return TimelineMeta{}, err
	}
	var meta TimelineMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return TimelineMeta{}, err
	}
	return meta, nil
}

func (db *DB) saveTimelineMeta() error {
	path := filepath.Join(db.dir, "timeline.meta")
	data, err := json.MarshalIndent(db.timelineMeta, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (db *DB) Promote() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	currentTL := db.timelineMeta.CurrentTimeline
	newTL := currentTL + 1
	lastOp := atomic.LoadUint64(&db.operationID)

	historyEntry := TimelineHistoryItem{
		TLI:     currentTL,
		StartOp: 0,
		EndOp:   lastOp,
	}
	if len(db.timelineMeta.History) > 0 {
		historyEntry.StartOp = db.timelineMeta.History[len(db.timelineMeta.History)-1].EndOp
	}

	db.timelineMeta.History = append(db.timelineMeta.History, historyEntry)
	db.timelineMeta.CurrentTimeline = newTL

	if err := db.saveTimelineMeta(); err != nil {
		return fmt.Errorf("failed to save timeline meta: %w", err)
	}

	if err := db.writeAheadLog.ForceNewTimeline(newTL); err != nil {
		return fmt.Errorf("failed to switch WAL timeline: %w", err)
	}

	// Keep INFO: Timeline Event is critical
	db.logger.Info("Promoted to new timeline", "old_timeline", currentTL, "new_timeline", newTL, "at_op", lastOp)
	return nil
}

func (db *DB) SetTimeline(newTL uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	currentTL := db.timelineMeta.CurrentTimeline
	if newTL == currentTL {
		return nil
	}
	if newTL < currentTL {
		return fmt.Errorf("cannot rewind timeline from %d to %d", currentTL, newTL)
	}

	// Record history transition
	lastOp := atomic.LoadUint64(&db.operationID)
	historyEntry := TimelineHistoryItem{
		TLI:     currentTL,
		StartOp: 0,
		EndOp:   lastOp,
	}
	if len(db.timelineMeta.History) > 0 {
		historyEntry.StartOp = db.timelineMeta.History[len(db.timelineMeta.History)-1].EndOp
	}
	db.timelineMeta.History = append(db.timelineMeta.History, historyEntry)
	db.timelineMeta.CurrentTimeline = newTL

	if err := db.saveTimelineMeta(); err != nil {
		return fmt.Errorf("failed to save timeline meta: %w", err)
	}

	if err := db.writeAheadLog.ForceNewTimeline(newTL); err != nil {
		return fmt.Errorf("failed to switch WAL timeline: %w", err)
	}

	// Keep INFO: Timeline Event is critical
	db.logger.Info("Switched to leader timeline", "old_timeline", currentTL, "new_timeline", newTL, "at_op", lastOp)
	return nil
}

func (db *DB) ForceSetClocks(txID, opID uint64) {
	currentTx := atomic.LoadUint64(&db.transactionID)
	if txID > currentTx {
		atomic.StoreUint64(&db.transactionID, txID)
	}
	currentOp := atomic.LoadUint64(&db.operationID)
	if opID > currentOp {
		atomic.StoreUint64(&db.operationID, opID)
	}
}

func (db *DB) openLevelDB(dir string) error {
	indexPath := filepath.Join(dir, "index")
	ldbOpts := &opt.Options{
		BlockCacheCapacity: db.blockCacheSize,
		Compression:        opt.SnappyCompression,
	}

	var err error
	db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)

	needsRebuild := false
	if err != nil {
		db.logger.Error("LevelDB open failed, attempting rebuild", "err", err)
		needsRebuild = true
		os.RemoveAll(indexPath)
		db.ldb, err = leveldb.OpenFile(indexPath, ldbOpts)
		if err != nil {
			return fmt.Errorf("open fresh leveldb: %w", err)
		}
	} else if !db.isIndexConsistent() {
		db.logger.Warn("Index state inconsistent with WAL, rebuilding")
		needsRebuild = true
	}

	if needsRebuild {
		start := time.Now()
		if err := db.RebuildIndexFromVLog(); err != nil {
			return fmt.Errorf("rebuild index: %w", err)
		}
		// CHANGED: Reduced from INFO to DEBUG
		db.logger.Debug("Index rebuild complete", "duration", time.Since(start))
	}
	return nil
}

func (db *DB) startBackgroundTasks() {
	db.lastCkptOpID = db.operationID
	waitCount := 4
	if db.checksumInterval > 0 {
		waitCount++
	}
	if db.maxDiskUsagePercent > 0 {
		waitCount++
	}
	db.wg.Add(waitCount)

	go db.runAutoCheckpoint()
	go db.runAutoCompaction()
	go db.runGroupCommits()
	go db.runLivenessReaper()

	if db.checksumInterval > 0 {
		go db.runBackgroundChecksum()
	}
	if db.maxDiskUsagePercent > 0 {
		go db.runDiskMonitor()
	}
}

// runLivenessReaper force-aborts any locally-owned RW transaction that has
// been open longer than db.txTimeout, independent of client activity. This
// bounds how long a stalled/forgotten connection can hold first-writer-wins
// key locks.
func (db *DB) runLivenessReaper() {
	defer db.wg.Done()
	interval := db.txTimeout / 4
	if interval <= 0 || interval > 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			db.reapExpiredTransactions()
		}
	}
}

func (db *DB) reapExpiredTransactions() {
	now := time.Now()
	var expired []*Transaction

	db.txMu.Lock()
	for xid, start := range db.txStartTimes {
		if now.Sub(start) > db.txTimeout {
			if tx, ok := db.activeXids[xid]; ok && tx != nil {
				expired = append(expired, tx)
			}
		}
	}
	db.txMu.Unlock()

	for _, tx := range expired {
		tx.markAborted()
		db.logger.Warn("Liveness reaper force-aborted stalled transaction", "xid", tx.xid, "timeout", db.txTimeout)
		db.abortTransaction(tx)
	}
}

func (db *DB) KeyCount() (int64, error) {
	return atomic.LoadInt64(&db.keyCount), nil
}

// TotalGarbageBytes calculates the total number of stale bytes in the ValueLog.
func (db *DB) TotalGarbageBytes() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var total int64
	for _, size := range db.deletedBytesByFile {
		total += size
	}
	return total
}

func (db *DB) scanKeyCount() (int64, error) {
	count := int64(0)
	if db.ldb == nil {
		return 0, nil
	}
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	var lastKey []byte

	for iter.Next() {
		if bytes.HasPrefix(iter.Key(), []byte("!sys!")) {
			continue
		}
		key, _, err := decodeIndexKey(iter.Key())
		if err != nil {
			continue
		}
		if !bytes.Equal(key, lastKey) {
			meta, err := decodeEntryMeta(iter.Value())
			if err == nil && !meta.IsTombstone {
				count++
			}
			lastKey = append([]byte(nil), key...)
		}
	}
	return count, nil
}

func (db *DB) StorageStats() (walCount int, walSize int64, vlogCount int, vlogSize int64) {
	walDir := filepath.Join(db.dir, "wal")
	if walEntries, err := os.ReadDir(walDir); err == nil {
		for _, e := range walEntries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
				walCount++
				if info, err := e.Info(); err == nil {
					walSize += info.Size()
				}
			}
		}
	}
	vlogDir := filepath.Join(db.dir, "vlog")
	if vlogEntries, err := os.ReadDir(vlogDir); err == nil {
		for _, e := range vlogEntries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".vlog") {
				vlogCount++
				if info, err := e.Info(); err == nil {
					vlogSize += info.Size()
				}
			}
		}
	}
	return
}

func (db *DB) LastOpID() uint64 {
	return atomic.LoadUint64(&db.operationID)
}

func (db *DB) GetLastCheckpointOpID() uint64 {
	return atomic.LoadUint64(&db.lastCkptOpID)
}

func (db *DB) GetConflicts() uint64 {
	return atomic.LoadUint64(&db.metricsConflicts)
}

func (db *DB) CurrentTimeline() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.timelineMeta.CurrentTimeline
}

func (db *DB) ActiveTransactionCount() int {
	db.activeTxnsMu.Lock()
	defer db.activeTxnsMu.Unlock()
	return len(db.activeTxns)
}

func (db *DB) runGroupCommits() {
	defer db.wg.Done()
	var batch []commitRequest

	for {
		select {
		case <-db.closeCh:
			return
		case req := <-db.commitCh:
			batch = append(batch, req)
		}

	Loop:
		for len(batch) < 128 {
			select {
			case req := <-db.commitCh:
				batch = append(batch, req)
			default:
				break Loop
			}
		}
		db.processCommitBatch(batch)
		batch = batch[:0]
	}
}

func (db *DB) SetCompactionMinGarbage(minGarbage int64) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.minGarbageThreshold = minGarbage
}

func (db *DB) runAutoCheckpoint() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.autoCheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			currentOp := atomic.LoadUint64(&db.operationID)
			lastOp := atomic.LoadUint64(&db.lastCkptOpID)

			if currentOp > lastOp {
				if err := db.Checkpoint(); err != nil {
					if !strings.Contains(err.Error(), "closed") {
						db.logger.Error("Auto-checkpoint failed", "err", err)
					}
				}
			}
		}
	}
}

func (db *DB) runAutoCompaction() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.compactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			for i := 0; i < 10; i++ {
				didWork, err := db.RunCompaction()
				if err != nil {
					if !strings.Contains(err.Error(), "closed") {
						db.logger.Error("Auto-compaction failed", "err", err)
					}
					break
				}
				if !didWork {
					break
				}
			}
		}
	}
}

func (db *DB) runBackgroundChecksum() {
	defer db.wg.Done()
	if db.checksumInterval <= 0 {
		return
	}
	ticker := time.NewTicker(db.checksumInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			if err := db.VerifyChecksums(); err != nil {
				if !strings.Contains(err.Error(), "closed") {
					// WARN: Integrity check failed
					db.logger.Warn("Background checksum verification failed", "err", err)
				}
			}
		}
	}
}

func (db *DB) VerifyChecksums() error {
	fids, err := db.valueLog.GetImmutableFileIDs()
	if err != nil {
		return err
	}
	for _, fid := range fids {
		if isClosed(db.closeCh) {
			return nil
		}
		err := db.valueLog.IterateFile(fid, func(_ ValueLogEntry, _ EntryMeta) error {
			if isClosed(db.closeCh) {
				return errors.New("closed")
			}
			return nil
		})
		if err != nil && !strings.Contains(err.Error(), "closed") {
			db.logger.Error("Corrupt VLog file detected", "file_id", fid, "err", err)
		}
	}
	return nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// NewTransaction starts a new transaction. Read-write transactions are
// assigned an xid immediately and log a BEGIN WAL record (Postgres-style);
// read-only transactions merely capture a snapshot and never touch the WAL.
func (db *DB) NewTransaction(update bool) *Transaction {
	if !update {
		db.activeTxnsMu.Lock()
		db.txMu.Lock()
		snap := db.buildSnapshotLocked()
		db.txMu.Unlock()
		tx := &Transaction{db: db, update: false, snapshot: snap}
		db.activeTxns[tx] = snap.Xmax
		db.activeTxnsMu.Unlock()
		return tx
	}

	// xid allocation and snapshot construction must be atomic with each
	// other under txMu: if we incremented db.transactionID before taking
	// the lock, a concurrent NewTransaction's buildSnapshotLocked (running
	// between our increment and our own txMu acquisition) could compute an
	// Xmax that already covers our xid (since Xmax is derived from the same
	// counter) while our xid is not yet in activeXids/Xip -- making that
	// other transaction's snapshot treat us as neither future nor
	// in-progress. A write that later lands on a key we've already
	// committed would then wrongly look "old enough" to overwrite, silently
	// dropping our update (a lost update / torn snapshot).
	db.txMu.Lock()
	xid := atomic.AddUint64(&db.transactionID, 1)
	snap := db.buildSnapshotLocked()
	tx := &Transaction{
		db:              db,
		update:          true,
		xid:             xid,
		snapshot:        snap,
		keyLocks:        make(map[string]struct{}),
		dispositionSeen: make(map[string]bool),
		ownPriorMeta:    make(map[string]*EntryMeta),
		staleBytes:      make(map[uint32]int64),
		readSet:         make(map[string]struct{}),
	}
	db.activeXids[xid] = tx
	db.txStartTimes[xid] = time.Now()
	db.txMu.Unlock()

	db.activeTxnsMu.Lock()
	db.activeTxns[tx] = xid
	db.activeTxnsMu.Unlock()

	opID, err := db.appendRecord(WALRecordBegin, xid, nil, nil)
	if err != nil {
		// Best-effort: mark the transaction unusable; Put/Commit will surface the failure.
		tx.aborted = true
		tx.beginErr = err
	} else {
		tx.beginOpID = opID
		db.txMu.Lock()
		db.beginOpIDs[xid] = opID
		db.txMu.Unlock()
	}

	return tx
}

// appendRecord assigns an opID and appends one WAL record atomically: opID
// allocation and the physical write happen inside the same WAL-locked
// critical section, so file order always matches opID order.
func (db *DB) appendRecord(recType WALRecordType, xid uint64, key, value []byte) (uint64, error) {
	nextOpID := func() uint64 { return atomic.AddUint64(&db.operationID, 1) }
	build := func(opID uint64) []byte {
		return encodeWALRecord(WALRecord{Type: recType, XID: xid, OpID: opID, Key: key, Value: value})
	}
	opIDs, err := db.writeAheadLog.AppendRecordsWithOpIDs(nextOpID, []func(uint64) []byte{build}, false)
	if err != nil {
		return 0, err
	}
	return opIDs[0], nil
}

// ScanWAL streams typed WAL records at or after startOpID, in physical (and
// therefore logical) order.
func (db *DB) ScanWAL(startOpID uint64, fn func([]WALRecord) error) error {
	loc, found, err := db.locateWALStart(startOpID)
	if err != nil {
		return err
	}
	if !found {
		return ErrLogUnavailable
	}
	return db.writeAheadLog.Scan(loc, func(recs []WALRecord) error {
		var filtered []WALRecord
		for _, r := range recs {
			if r.OpID >= startOpID {
				filtered = append(filtered, r)
			}
		}
		if len(filtered) > 0 {
			return fn(filtered)
		}
		return nil
	})
}

// PurgeWAL deletes WAL files older than minOpID, but never past the oldest
// still-open BEGIN (local or replicated) so an in-progress transaction's
// records always remain replayable.
func (db *DB) PurgeWAL(minOpID uint64) error {
	db.txMu.Lock()
	for _, op := range db.beginOpIDs {
		if op < minOpID {
			minOpID = op
		}
	}
	db.txMu.Unlock()
	return db.writeAheadLog.PurgeOlderThan(minOpID)
}

// ApplyRecord applies a single replicated WAL record directly to the engine,
// mirroring the eager write path used locally: SET/DEL land in VLog+index
// immediately (uncommitted), and COMMIT/ABORT resolve the clog. opIDs/xids
// are taken verbatim from the leader, not reassigned.
func (db *DB) ApplyRecord(rec WALRecord) error {
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		return errors.New("database is corrupt")
	}

	payload := encodeWALRecord(rec)

	switch rec.Type {
	case WALRecordBegin:
		if err := db.writeAheadLog.AppendReplicatedRecord(payload, false); err != nil {
			return err
		}
		db.txMu.Lock()
		db.activeXids[rec.XID] = nil
		db.beginOpIDs[rec.XID] = rec.OpID
		db.txMu.Unlock()
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil

	case WALRecordSet, WALRecordDelete:
		if err := db.writeAheadLog.AppendReplicatedRecord(payload, false); err != nil {
			return err
		}
		fileID, offset, err := db.valueLog.AppendEntries([]ValueLogEntry{{
			Key: rec.Key, Value: rec.Value, TransactionID: rec.XID, OperationID: rec.OpID, IsDelete: rec.Type == WALRecordDelete,
		}})
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			return err
		}
		meta := EntryMeta{
			FileID: fileID, ValueOffset: offset, ValueLen: uint32(len(rec.Value)),
			TransactionID: rec.XID, OperationID: rec.OpID, IsTombstone: rec.Type == WALRecordDelete,
		}
		if err := db.ldb.Put(encodeIndexKey(rec.Key, rec.XID), meta.Encode(), nil); err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			return err
		}
		db.accountReplicatedWrite(rec)
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil

	case WALRecordCommit:
		if err := db.writeAheadLog.AppendReplicatedRecord(payload, true); err != nil {
			return err
		}
		if err := db.ldb.Put(encodeClogKey(rec.XID), []byte{byte(TxCommitted)}, nil); err != nil {
			panic("CRITICAL: replica clog commit persist failed: " + err.Error())
		}
		db.txMu.Lock()
		delete(db.activeXids, rec.XID)
		delete(db.beginOpIDs, rec.XID)
		impact := db.replImpact[rec.XID]
		delete(db.replImpact, rec.XID)
		db.txMu.Unlock()
		db.applyReplicatedImpact(impact)
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil

	case WALRecordAbort:
		if err := db.writeAheadLog.AppendReplicatedRecord(payload, false); err != nil {
			return err
		}
		if err := db.ldb.Put(encodeClogKey(rec.XID), []byte{byte(TxAborted)}, nil); err != nil {
			db.logger.Error("failed to persist replicated abort clog entry", "xid", rec.XID, "err", err)
		}
		db.txMu.Lock()
		delete(db.activeXids, rec.XID)
		delete(db.beginOpIDs, rec.XID)
		delete(db.replImpact, rec.XID)
		db.txMu.Unlock()
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil
	}

	return fmt.Errorf("unknown WAL record type: %d", rec.Type)
}

// replTxImpact accumulates the key-count/garbage impact of a single
// replicated (ApplyRecord) transaction's writes, mirroring the bookkeeping a
// local *Transaction keeps between Put/Delete and Commit.
type replTxImpact struct {
	keyDelta        int64
	staleBytes      map[uint32]int64
	dispositionSeen map[string]bool
}

// accountReplicatedWrite updates the per-xid impact accumulator for a
// replicated SET/DELETE record, using the same "current DB truth" comparison
// a local write makes on its first touch of a key. Must be called after the
// index write for rec has already landed, so latestResolvedMeta's exclusion
// of rec.XID correctly finds the prior version (if any).
func (db *DB) accountReplicatedWrite(rec WALRecord) {
	keyStr := string(rec.Key)
	isDelete := rec.Type == WALRecordDelete

	db.txMu.Lock()
	impact, ok := db.replImpact[rec.XID]
	if !ok {
		impact = &replTxImpact{staleBytes: make(map[uint32]int64), dispositionSeen: make(map[string]bool)}
		db.replImpact[rec.XID] = impact
	}
	wasLiveIfSeen, seenBefore := impact.dispositionSeen[keyStr]
	db.txMu.Unlock()

	var wasLive bool
	var staleFileID uint32
	var staleSize int64
	haveStale := false

	if seenBefore {
		// Second+ write to this key within the same replicated xid: compare
		// against our own prior write in this xid, not the DB-wide index
		// (a single-writer xid can't race itself).
		wasLive = wasLiveIfSeen
	} else {
		iter := db.ldb.NewIterator(nil, nil)
		meta, _, found := db.latestResolvedMeta(iter, rec.Key, rec.XID)
		iter.Release()
		wasLive = found && meta != nil && !meta.IsTombstone
		if found && meta != nil {
			staleFileID = meta.FileID
			staleSize = int64(ValueLogHeaderSize) + int64(len(rec.Key)) + int64(meta.ValueLen)
			haveStale = true
		}
	}

	db.txMu.Lock()
	if haveStale {
		impact.staleBytes[staleFileID] += staleSize
	}
	if isDelete {
		if wasLive {
			impact.keyDelta--
		}
	} else if !wasLive {
		impact.keyDelta++
	}
	impact.dispositionSeen[keyStr] = !isDelete
	db.txMu.Unlock()
}

// applyReplicatedImpact durably applies an accumulated replicated
// transaction's key-count/garbage impact once its COMMIT record has landed.
func (db *DB) applyReplicatedImpact(impact *replTxImpact) {
	if impact == nil {
		return
	}
	if impact.keyDelta != 0 {
		atomic.AddInt64(&db.keyCount, impact.keyDelta)
	}
	if len(impact.staleBytes) > 0 {
		db.mu.Lock()
		for fid, sz := range impact.staleBytes {
			db.deletedBytesByFile[fid] += sz
		}
		db.mu.Unlock()
	}
}

func (db *DB) locateWALStart(targetOpID uint64) (WALLocation, bool, error) {
	if loc, ok := db.writeAheadLog.FindInMemory(targetOpID); ok {
		return loc, true, nil
	}
	if db.ldb == nil {
		return WALLocation{}, false, nil
	}
	iter := db.ldb.NewIterator(util.BytesPrefix(sysWALIndexPrefix), nil)
	defer iter.Release()
	seekKey := encodeWALIndexKey(targetOpID)
	if iter.Seek(seekKey) {
		key := iter.Key()
		if decodeWALIndexKey(key) == targetOpID {
			var loc WALLocation
			err := json.Unmarshal(iter.Value(), &loc)
			return loc, true, err
		}
		if iter.Prev() && bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
			var loc WALLocation
			err := json.Unmarshal(iter.Value(), &loc)
			return loc, true, err
		}
	} else if iter.Last() && bytes.HasPrefix(iter.Key(), sysWALIndexPrefix) {
		var loc WALLocation
		err := json.Unmarshal(iter.Value(), &loc)
		return loc, true, err
	}
	return WALLocation{}, false, nil
}

func (db *DB) Close() error {
	if !atomic.CompareAndSwapInt32(&db.closed, 0, 1) {
		return nil
	}
	// CHANGED: Reduced from INFO to DEBUG
	db.logger.Debug("Closing database instance", "dir", db.dir)
	close(db.closeCh)
	db.wg.Wait()

	db.commitMu.Lock()
	defer db.commitMu.Unlock()

	if err := db.Checkpoint(); err != nil {
		if !strings.Contains(err.Error(), "closed") {
			db.logger.Error("Error checkpointing on close", "err", err)
		}
	}
	db.persistSequences()

	var errs []string
	if db.writeAheadLog != nil {
		if err := db.writeAheadLog.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if db.valueLog != nil {
		if err := db.valueLog.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if db.ldb != nil {
		if err := db.ldb.Close(); err != nil {
			errs = append(errs, err.Error())
		}
		db.ldb = nil
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (db *DB) Checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.ldb == nil {
		return nil
	}

	// FIX: Clean up ALL existing garbage stats first to prevent resurrection of deleted files
	iter := db.ldb.NewIterator(util.BytesPrefix(sysStaleBytesPrefix), nil)
	cleanupBatch := new(leveldb.Batch)
	for iter.Next() {
		cleanupBatch.Delete(iter.Key())
	}
	iter.Release()
	if cleanupBatch.Len() > 0 {
		if err := db.ldb.Write(cleanupBatch, nil); err != nil {
			return err
		}
	}

	batch := new(leveldb.Batch)
	for fileID, size := range db.deletedBytesByFile {
		k := make([]byte, len(sysStaleBytesPrefix)+4)
		copy(k, sysStaleBytesPrefix)
		binary.BigEndian.PutUint32(k[len(sysStaleBytesPrefix):], fileID)
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, uint64(size))
		batch.Put(k, v)
	}

	// Always rotate VLog during checkpoint (Rotate is a no-op if the current
	// file is empty), so compaction can treat "checkpointed" files as sealed
	// candidates instead of waiting for MaxVLogSize to be reached.
	if err := db.valueLog.Rotate(); err != nil {
		return fmt.Errorf("vlog rotate failed: %w", err)
	}

	// Always rotate WAL during checkpoint
	if err := db.writeAheadLog.Rotate(); err != nil {
		return fmt.Errorf("wal rotate failed: %w", err)
	}

	atomic.StoreUint64(&db.lastCkptOpID, atomic.LoadUint64(&db.operationID))
	db.persistSequences()

	if batch.Len() == 0 {
		return nil
	}
	return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
}

func (db *DB) onWALRotate(index map[uint64]WALLocation) error {
	if len(index) == 0 || db.ldb == nil {
		return nil
	}
	batch := new(leveldb.Batch)
	var keys []uint64
	for k := range index {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	for _, opID := range keys {
		locBytes, _ := json.Marshal(index[opID])
		batch.Put(encodeWALIndexKey(opID), locBytes)
	}
	return db.ldb.Write(batch, nil)
}

func (db *DB) runDiskMonitor() {
	defer db.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	db.checkDisk()

	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			db.checkDisk()
		}
	}
}

func (db *DB) checkDisk() {
	usage, err := getDiskUsage(db.dir)
	if err != nil {
		db.logger.Error("Disk usage check failed", "err", err)
		return
	}

	if int(usage) > db.maxDiskUsagePercent {
		if atomic.CompareAndSwapInt32(&db.isDiskFull, 0, 1) {
			// WARN: Resource exhaustion
			db.logger.Warn("Disk usage exceeds limit, stopping writes", "usage_percent", usage, "limit_percent", db.maxDiskUsagePercent)
		}
	} else {
		if atomic.CompareAndSwapInt32(&db.isDiskFull, 1, 0) {
			// INFO: Recovery
			db.logger.Info("Disk usage returned to normal, resuming writes", "usage_percent", usage)
		}
	}
}

func (db *DB) loadKeyCount() error {
	val, err := db.ldb.Get(sysKeyCountKey, nil)
	if err == nil && len(val) == 8 {
		db.keyCount = int64(binary.BigEndian.Uint64(val))
		return nil
	}
	if err == leveldb.ErrNotFound {
		// CHANGED: Reduced from INFO to DEBUG
		db.logger.Debug("Key count not found, scanning index for initial count", "action", "full_scan")
		count, err := db.scanKeyCount()
		if err != nil {
			return err
		}
		db.keyCount = count
		return nil
	}
	return err
}

func (db *DB) loadDeletedBytesStats() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	iter := db.ldb.NewIterator(util.BytesPrefix(sysStaleBytesPrefix), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		if len(key) < len(sysStaleBytesPrefix)+4 {
			continue
		}

		fileID := binary.BigEndian.Uint32(key[len(sysStaleBytesPrefix):])
		val := iter.Value()
		if len(val) != 8 {
			continue
		}
		size := int64(binary.BigEndian.Uint64(val))
		db.deletedBytesByFile[fileID] = size
	}
	return iter.Error()
}

func (db *DB) persistSequences() error {
	if db.ldb == nil {
		return nil
	}

	batch := new(leveldb.Batch)

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, atomic.LoadUint64(&db.transactionID))
	batch.Put(sysTransactionIDKey, buf)

	buf2 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf2, atomic.LoadUint64(&db.operationID))
	batch.Put(sysOperationIDKey, buf2)

	buf3 := make([]byte, 8)
	binary.BigEndian.PutUint64(buf3, uint64(atomic.LoadInt64(&db.keyCount)))
	batch.Put(sysKeyCountKey, buf3)

	return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
}

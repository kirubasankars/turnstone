// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package stonedb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/protocol"
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
	dir    string
	log    *DataLog
	index  *Index
	clog   map[uint64]TxStatus
	clogMu sync.RWMutex
	logger *slog.Logger

	mu         sync.RWMutex
	commitMu   sync.Mutex
	shutdownMu sync.RWMutex

	transactionID uint64
	operationID   uint64
	keyCount      int64
	staleBytes    int64
	scanWALFloor  uint64

	metricsConflicts uint64

	activeTxnsMu sync.Mutex
	activeTxns   map[*Transaction]uint64

	txMu         sync.Mutex
	activeXids   map[uint64]*Transaction
	keyLocks     map[string]uint64
	txStartTimes map[uint64]time.Time
	beginOpIDs   map[uint64]uint64
	txTimeout    time.Duration

	replImpact map[uint64]*replTxImpact

	closeCh      chan struct{}
	wg           sync.WaitGroup
	lastCkptOpID uint64
	closed       int32

	commitCh           chan commitRequest
	commitDelay        time.Duration
	commitSiblings     int
	unsafeDisableFsync bool

	minGarbageThreshold    int64
	checksumInterval       time.Duration
	autoCheckpointInterval time.Duration
	compactionInterval     time.Duration
	maxDiskUsagePercent    int
	isDiskFull             int32
	isCorrupt              int32
	vacuuming              int32

	timelineMeta TimelineMeta
}

// Open opens a database, replaying data.log to rebuild the ephemeral index.
func Open(dir string, opts Options) (*DB, error) {
	return OpenContext(context.Background(), dir, opts)
}

// OpenContext is like Open but honors cancellation during log replay.
func OpenContext(ctx context.Context, dir string, opts Options) (*DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
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
	if opts.TxTimeout == 0 {
		opts.TxTimeout = protocol.MaxTxDuration
	}
	switch {
	case opts.CommitDelay < 0:
		opts.CommitDelay = 0
	case opts.CommitDelay == 0:
		opts.CommitDelay = 2 * time.Millisecond
	}
	if opts.CommitSiblings <= 0 {
		opts.CommitSiblings = 2
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	logger = logger.With("db_dir", filepath.Base(dir))

	meta, err := loadTimelineMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("load timeline meta: %w", err)
	}

	logFile, err := OpenDataLog(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	index, err := OpenIndex(dir)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("open index: %w", err)
	}

	db := &DB{
		dir:                    dir,
		log:                    logFile,
		index:                  index,
		clog:                   make(map[uint64]TxStatus),
		activeTxns:             make(map[*Transaction]uint64),
		activeXids:             make(map[uint64]*Transaction),
		keyLocks:               make(map[string]uint64),
		txStartTimes:           make(map[uint64]time.Time),
		beginOpIDs:             make(map[uint64]uint64),
		replImpact:             make(map[uint64]*replTxImpact),
		txTimeout:              opts.TxTimeout,
		closeCh:                make(chan struct{}),
		commitCh:               make(chan commitRequest, 500),
		commitDelay:            opts.CommitDelay,
		commitSiblings:         opts.CommitSiblings,
		unsafeDisableFsync:     opts.UnsafeDisableFsync,
		minGarbageThreshold:    opts.CompactionMinGarbage,
		checksumInterval:       opts.ChecksumInterval,
		autoCheckpointInterval: opts.AutoCheckpointInterval,
		compactionInterval:     opts.CompactionInterval,
		maxDiskUsagePercent:    opts.MaxDiskUsagePercent,
		timelineMeta:           meta,
		logger:                 logger,
	}

	if opts.UnsafeDisableFsync {
		logger.Warn("UnsafeDisableFsync enabled")
	}

	if err := db.replayLog(ctx, opts.TruncateCorruptWAL); err != nil {
		db.Close()
		return nil, fmt.Errorf("replay log: %w", err)
	}

	db.startBackgroundTasks()
	return db, nil
}

func loadTimelineMeta(dir string) (TimelineMeta, error) {
	path := filepath.Join(dir, "timeline.meta")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return TimelineMeta{CurrentTimeline: 0}, nil
	}
	if err != nil {
		return TimelineMeta{}, err
	}
	var meta TimelineMeta
	return meta, json.Unmarshal(data, &meta)
}

func (db *DB) saveTimelineMeta() error {
	path := filepath.Join(db.dir, "timeline.meta")
	data, err := json.MarshalIndent(db.timelineMeta, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, fileMode); err != nil {
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

	historyEntry := TimelineHistoryItem{TLI: currentTL, StartOp: 0, EndOp: lastOp}
	if len(db.timelineMeta.History) > 0 {
		historyEntry.StartOp = db.timelineMeta.History[len(db.timelineMeta.History)-1].EndOp
	}
	db.timelineMeta.History = append(db.timelineMeta.History, historyEntry)
	db.timelineMeta.CurrentTimeline = newTL

	if err := db.saveTimelineMeta(); err != nil {
		return fmt.Errorf("save timeline meta: %w", err)
	}
	db.logger.Info("Promoted to new timeline", "old", currentTL, "new", newTL, "at_op", lastOp)
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

	lastOp := atomic.LoadUint64(&db.operationID)
	historyEntry := TimelineHistoryItem{TLI: currentTL, StartOp: 0, EndOp: lastOp}
	if len(db.timelineMeta.History) > 0 {
		historyEntry.StartOp = db.timelineMeta.History[len(db.timelineMeta.History)-1].EndOp
	}
	db.timelineMeta.History = append(db.timelineMeta.History, historyEntry)
	db.timelineMeta.CurrentTimeline = newTL

	if err := db.saveTimelineMeta(); err != nil {
		return fmt.Errorf("save timeline meta: %w", err)
	}
	db.logger.Info("Switched timeline", "old", currentTL, "new", newTL, "at_op", lastOp)
	return nil
}

func (db *DB) ForceSetClocks(txID, opID uint64) {
	if txID > atomic.LoadUint64(&db.transactionID) {
		atomic.StoreUint64(&db.transactionID, txID)
	}
	if opID > atomic.LoadUint64(&db.operationID) {
		atomic.StoreUint64(&db.operationID, opID)
	}
}

func (db *DB) startBackgroundTasks() {
	db.lastCkptOpID = atomic.LoadUint64(&db.operationID)
	waitCount := 4
	if db.checksumInterval > 0 {
		waitCount++
	}
	if db.maxDiskUsagePercent > 0 {
		waitCount++
	}
	db.wg.Add(waitCount)

	go db.runAutoCheckpoint()
	go db.runAutoVacuum()
	go db.runGroupCommits()
	go db.runLivenessReaper()
	if db.checksumInterval > 0 {
		go db.runBackgroundChecksum()
	}
	if db.maxDiskUsagePercent > 0 {
		go db.runDiskMonitor()
	}
}

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
		db.logger.Warn("Force-aborted stalled transaction", "xid", tx.xid)
		db.abortTransaction(tx)
	}
}

func (db *DB) AbortAllActiveWriteTransactions() {
	var active []*Transaction
	db.txMu.Lock()
	for _, tx := range db.activeXids {
		if tx != nil {
			active = append(active, tx)
		}
	}
	db.txMu.Unlock()
	for _, tx := range active {
		tx.markAborted()
		db.abortTransaction(tx)
	}
}

func (db *DB) KeyCount() (int64, error) {
	return atomic.LoadInt64(&db.keyCount), nil
}

func (db *DB) TotalGarbageBytes() int64 {
	return atomic.LoadInt64(&db.staleBytes)
}

func (db *DB) StorageStats() (logCount int, logicalSize int64, allocatedSize int64) {
	logCount = 1
	logicalSize = db.log.LogicalSize()
	allocatedSize = db.log.AllocatedSize()
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

const maxCommitBatchSize = 128
const shutdownBackgroundWait = 2 * time.Second
const shutdownCommitWait = 2 * time.Second

func (db *DB) failCommitBatch(batch []commitRequest) {
	for _, req := range batch {
		select {
		case req.resp <- ErrDatabaseClosed:
		default:
		}
	}
}

func (db *DB) drainCommitCh() {
	for {
		select {
		case req := <-db.commitCh:
			select {
			case req.resp <- ErrDatabaseClosed:
			default:
			}
		default:
			return
		}
	}
}

func (db *DB) shutdownGroupCommits(batch []commitRequest) {
	db.failCommitBatch(batch)
	db.drainCommitCh()
}

func (db *DB) runGroupCommits() {
	defer db.wg.Done()
	var batch []commitRequest
	for {
		select {
		case <-db.closeCh:
			db.shutdownGroupCommits(batch)
			return
		case req := <-db.commitCh:
			batch = append(batch, req)
		}

		if db.commitDelay > 0 && !db.unsafeDisableFsync && db.ActiveTransactionCount() >= db.commitSiblings {
			timer := time.NewTimer(db.commitDelay)
		DelayLoop:
			for len(batch) < maxCommitBatchSize {
				select {
				case <-db.closeCh:
					timer.Stop()
					db.shutdownGroupCommits(batch)
					return
				case req := <-db.commitCh:
					batch = append(batch, req)
				case <-timer.C:
					break DelayLoop
				}
			}
			timer.Stop()
		}

	Loop:
		for len(batch) < maxCommitBatchSize {
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

func (db *DB) runAutoCheckpoint() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.autoCheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			if atomic.LoadUint64(&db.operationID) > atomic.LoadUint64(&db.lastCkptOpID) {
				if err := db.Checkpoint(); err != nil && !strings.Contains(err.Error(), "closed") {
					db.logger.Error("Auto-checkpoint failed", "err", err)
				}
			}
		}
	}
}

func (db *DB) runAutoVacuum() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			for i := 0; i < 10; i++ {
				select {
				case <-db.closeCh:
					return
				default:
				}
				didWork, err := db.RunVacuum()
				if err != nil {
					if !strings.Contains(err.Error(), "closed") {
						db.logger.Error("Auto-vacuum failed", "err", err)
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
	ticker := time.NewTicker(db.checksumInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			_ = db.VerifyChecksums()
		}
	}
}

func (db *DB) VerifyChecksums() error {
	if atomic.LoadInt32(&db.closed) == 1 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-db.closeCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return db.log.Replay(ctx, false, db.timelineMeta.History, func(rec WALRecord, span recordSpan) {})
}

func (db *DB) NewTransaction(update bool) *Transaction {
	if !update {
		db.activeTxnsMu.Lock()
		db.txMu.Lock()
		snap := db.buildSnapshotLocked()
		snapOpID := atomic.LoadUint64(&db.operationID)
		db.txMu.Unlock()
		tx := &Transaction{db: db, update: false, snapshot: snap, snapOpID: snapOpID}
		db.activeTxns[tx] = snap.Xmax
		db.activeTxnsMu.Unlock()
		return tx
	}

	db.txMu.Lock()
	xid := atomic.AddUint64(&db.transactionID, 1)
	snap := db.buildSnapshotLocked()
	tx := &Transaction{
		db: db, update: true, xid: xid, snapshot: snap,
		keyLocks: make(map[string]struct{}), dispositionSeen: make(map[string]bool),
		ownPriorVer: make(map[string]*indexVersion), staleBytes: make(map[int64]int64),
		readSet: make(map[string]struct{}),
	}
	db.activeXids[xid] = tx
	db.txStartTimes[xid] = time.Now()
	db.txMu.Unlock()

	db.activeTxnsMu.Lock()
	db.activeTxns[tx] = xid
	db.activeTxnsMu.Unlock()

	opID, err := db.appendRecord(WALRecordBegin, xid, nil, nil)
	if err != nil {
		tx.markAborted()
		tx.beginErr = err
	} else {
		tx.beginOpID = opID
		db.txMu.Lock()
		db.beginOpIDs[xid] = opID
		db.txMu.Unlock()
	}
	return tx
}

func (db *DB) appendRecord(recType WALRecordType, xid uint64, key, value []byte) (uint64, error) {
	nextOpID := func() uint64 { return atomic.AddUint64(&db.operationID, 1) }
	build := func(opID uint64) []byte {
		return encodeWALRecord(WALRecord{Type: recType, XID: xid, OpID: opID, Key: key, Value: value})
	}
	opIDs, _, err := db.log.AppendRecordsWithOpIDs(nextOpID, []func(uint64) []byte{build}, false)
	if err != nil {
		return 0, err
	}
	return opIDs[0], nil
}

func (db *DB) appendRecordWithOffset(recType WALRecordType, xid uint64, key, value []byte) (uint64, int64, error) {
	nextOpID := func() uint64 { return atomic.AddUint64(&db.operationID, 1) }
	build := func(opID uint64) []byte {
		return encodeWALRecord(WALRecord{Type: recType, XID: xid, OpID: opID, Key: key, Value: value})
	}
	opIDs, offsets, err := db.log.AppendRecordsWithOpIDs(nextOpID, []func(uint64) []byte{build}, false)
	if err != nil {
		return 0, 0, err
	}
	return opIDs[0], offsets[0], nil
}

func (db *DB) ScanWAL(startOpID uint64, fn func([]WALRecord) error) error {
	if startOpID < atomic.LoadUint64(&db.scanWALFloor) {
		return ErrLogUnavailable
	}
	return db.log.Scan(startOpID, fn)
}

func (db *DB) PurgeWAL(minOpID uint64) error {
	db.txMu.Lock()
	for _, op := range db.beginOpIDs {
		if op < minOpID {
			minOpID = op
		}
	}
	db.txMu.Unlock()
	atomic.StoreUint64(&db.scanWALFloor, minOpID)
	db.log.TrimOpOffsetsBelow(minOpID)
	return nil
}

func (db *DB) ApplyRecord(rec WALRecord) error {
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		return errors.New("database is corrupt")
	}
	payload := encodeWALRecord(rec)

	switch rec.Type {
	case WALRecordBegin:
		if _, err := db.log.AppendReplicatedRecord(payload, false); err != nil {
			return err
		}
		db.txMu.Lock()
		db.activeXids[rec.XID] = nil
		db.beginOpIDs[rec.XID] = rec.OpID
		db.txMu.Unlock()
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil

	case WALRecordSet, WALRecordDelete:
		off, err := db.log.AppendReplicatedRecord(payload, false)
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			return err
		}
		isDelete := rec.Type == WALRecordDelete
		db.index.Put(rec.Key, indexVersion{
			offset: off, valueLen: uint32(len(rec.Value)),
			xmin: rec.XID, opID: rec.OpID, tombstone: isDelete,
		})
		db.accountReplicatedWrite(rec)
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil

	case WALRecordCommit:
		if _, err := db.log.AppendReplicatedRecord(payload, true); err != nil {
			return err
		}
		db.forgetClog(rec.XID)
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
		if _, err := db.log.AppendReplicatedRecord(payload, false); err != nil {
			return err
		}
		db.setClog(rec.XID, TxAborted)
		db.index.DropXid(rec.XID)
		db.txMu.Lock()
		delete(db.activeXids, rec.XID)
		delete(db.beginOpIDs, rec.XID)
		delete(db.replImpact, rec.XID)
		db.txMu.Unlock()
		db.forgetClog(rec.XID)
		db.ForceSetClocks(rec.XID, rec.OpID)
		return nil
	}
	return fmt.Errorf("unknown WAL record type: %d", rec.Type)
}

type replTxImpact struct {
	keyDelta        int64
	staleBytes      int64
	dispositionSeen map[string]bool
}

func (db *DB) accountReplicatedWrite(rec WALRecord) {
	keyStr := string(rec.Key)
	isDelete := rec.Type == WALRecordDelete

	db.txMu.Lock()
	impact, ok := db.replImpact[rec.XID]
	if !ok {
		impact = &replTxImpact{dispositionSeen: make(map[string]bool)}
		db.replImpact[rec.XID] = impact
	}
	wasLiveIfSeen, seenBefore := impact.dispositionSeen[keyStr]
	db.txMu.Unlock()

	var wasLive bool
	if seenBefore {
		wasLive = wasLiveIfSeen
	} else {
		ver, _, found := db.index.LatestResolved(rec.Key, rec.XID, db.clogStatus)
		wasLive = found && ver != nil && !ver.tombstone
		if found && ver != nil {
			impact.staleBytes += recordSpanSize(len(rec.Key), int(ver.valueLen), WALRecordSet)
		}
	}

	db.txMu.Lock()
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

func (db *DB) applyReplicatedImpact(impact *replTxImpact) {
	if impact == nil {
		return
	}
	if impact.keyDelta != 0 {
		atomic.AddInt64(&db.keyCount, impact.keyDelta)
	}
	if impact.staleBytes > 0 {
		atomic.AddInt64(&db.staleBytes, impact.staleBytes)
	}
}

func (db *DB) Close() error {
	if !atomic.CompareAndSwapInt32(&db.closed, 0, 1) {
		return nil
	}
	start := time.Now()
	db.shutdownMu.Lock()
	db.shutdownMu.Unlock()

	close(db.closeCh)
	bgDone := make(chan struct{})
	go func() {
		db.wg.Wait()
		close(bgDone)
	}()
	select {
	case <-bgDone:
	case <-time.After(shutdownBackgroundWait):
		db.logger.Warn("Shutdown: background tasks did not finish in time", "wait_ms", shutdownBackgroundWait.Milliseconds())
	}
	db.logger.Debug("Shutdown phase complete", "phase", "background_tasks", "elapsed_ms", time.Since(start).Milliseconds())

	commitDone := make(chan struct{})
	go func() {
		db.commitMu.Lock()
		close(commitDone)
	}()
	select {
	case <-commitDone:
		_ = db.Checkpoint()
		db.commitMu.Unlock()
	case <-time.After(shutdownCommitWait):
		db.logger.Warn("Shutdown: commit batch still active, skipping checkpoint", "wait_ms", shutdownCommitWait.Milliseconds())
	}

	if db.index != nil {
		_ = db.index.Close()
	}
	db.logger.Debug("Shutdown phase complete", "phase", "index_dropped", "elapsed_ms", time.Since(start).Milliseconds())

	if db.log != nil {
		err := db.log.Close()
		db.logger.Debug("Shutdown phase complete", "phase", "log_closed", "elapsed_ms", time.Since(start).Milliseconds())
		return err
	}
	return nil
}

func (db *DB) Checkpoint() error {
	atomic.StoreUint64(&db.lastCkptOpID, atomic.LoadUint64(&db.operationID))
	return nil
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
		atomic.StoreInt32(&db.isDiskFull, 1)
	} else {
		atomic.StoreInt32(&db.isDiskFull, 0)
	}
}

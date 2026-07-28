// Copyright (c) 2026 Kiruba Sankar Swaminathan
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root of this source tree.

package engine

import (
	"context"
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

// DB is the main database struct.
type DB struct {
	dir    string
	log    *DataLog
	index  *Index
	clog   map[uint64]TxStatus
	clogMu sync.RWMutex
	logger *slog.Logger

	commitMu   sync.Mutex
	shutdownMu sync.RWMutex

	transactionID   uint64
	keyCount        int64
	scanFloor       int64
	retentionOffset int64

	metricsConflicts uint64

	activeTxnsMu sync.Mutex
	activeTxns   map[*Transaction]uint64

	txMu         sync.Mutex
	activeXids   map[uint64]*Transaction
	keyLocks     map[string]uint64
	txStartTimes map[uint64]time.Time
	beginOffsets map[uint64]int64
	txTimeout    time.Duration

	replImpact map[uint64]*replTxImpact

	closeCh chan struct{}
	wg      sync.WaitGroup
	closed  int32

	commitCh           chan commitRequest
	commitDelay        time.Duration
	commitSiblings     int
	unsafeDisableFsync bool

	checksumInterval    time.Duration
	retentionInterval   time.Duration
	maxDiskUsagePercent int
	isDiskFull          int32
	isCorrupt           int32
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
	if opts.RetentionInterval == 0 {
		opts.RetentionInterval = 60 * time.Second
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

	logFile, err := OpenDataLog(dir, logger)
	if err != nil {
		return nil, fmt.Errorf("open log: %w", err)
	}

	if err := os.RemoveAll(filepath.Join(dir, "index")); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("remove leftover index dir: %w", err)
	}
	index := NewIndex()

	db := &DB{
		dir:                 dir,
		log:                 logFile,
		index:               index,
		clog:                make(map[uint64]TxStatus),
		activeTxns:          make(map[*Transaction]uint64),
		activeXids:          make(map[uint64]*Transaction),
		keyLocks:            make(map[string]uint64),
		txStartTimes:        make(map[uint64]time.Time),
		beginOffsets:        make(map[uint64]int64),
		replImpact:          make(map[uint64]*replTxImpact),
		txTimeout:           opts.TxTimeout,
		closeCh:             make(chan struct{}),
		commitCh:            make(chan commitRequest, 500),
		commitDelay:         opts.CommitDelay,
		commitSiblings:      opts.CommitSiblings,
		unsafeDisableFsync:  opts.UnsafeDisableFsync,
		checksumInterval:    opts.ChecksumInterval,
		retentionInterval:   opts.RetentionInterval,
		maxDiskUsagePercent: opts.MaxDiskUsagePercent,
		logger:              logger,
	}

	if opts.UnsafeDisableFsync {
		logger.Warn("UnsafeDisableFsync enabled")
	}

	if err := db.replayLog(ctx, opts.TruncateCorruptTail); err != nil {
		db.Close()
		return nil, fmt.Errorf("replay log: %w", err)
	}

	db.startBackgroundTasks()
	return db, nil
}

func (db *DB) AdvanceXID(txID uint64) {
	if txID > atomic.LoadUint64(&db.transactionID) {
		atomic.StoreUint64(&db.transactionID, txID)
	}
}

func (db *DB) startBackgroundTasks() {
	atomic.StoreInt64(&db.retentionOffset, db.log.WriteOffset())
	waitCount := 3
	if db.checksumInterval > 0 {
		waitCount++
	}
	if db.maxDiskUsagePercent > 0 {
		waitCount++
	}
	db.wg.Add(waitCount)

	go db.runRetentionMarker()
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

func (db *DB) StorageStats() (logicalSize int64, allocatedSize int64) {
	return db.log.LogicalSize(), db.log.AllocatedSize()
}

func (db *DB) LastLogOffset() int64 {
	return db.log.WriteOffset()
}

func (db *DB) RetentionOffset() int64 {
	return atomic.LoadInt64(&db.retentionOffset)
}

func (db *DB) ScanFloor() int64 {
	return atomic.LoadInt64(&db.scanFloor)
}

func (db *DB) IsValidFrameOffset(offset int64) bool {
	return db.log.IsFrameBoundary(offset)
}

func (db *DB) GetConflicts() uint64 {
	return atomic.LoadUint64(&db.metricsConflicts)
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

func (db *DB) runRetentionMarker() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.retentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeCh:
			return
		case <-ticker.C:
			if db.log.WriteOffset() > atomic.LoadInt64(&db.retentionOffset) {
				if err := db.MarkRetention(); err != nil && !strings.Contains(err.Error(), "closed") {
					db.logger.Error("retention mark failed", "err", err)
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
	return db.log.Replay(ctx, false, func(rec Record, span recordSpan) {})
}

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

	db.txMu.Lock()
	xid := atomic.AddUint64(&db.transactionID, 1)
	snap := db.buildSnapshotLocked()
	tx := &Transaction{
		db: db, update: true, xid: xid, snapshot: snap,
		keyLocks: make(map[string]struct{}), dispositionSeen: make(map[string]bool),
		ownPriorVer: make(map[string]*indexVersion),
		readSet:     make(map[string]struct{}),
	}
	db.activeXids[xid] = tx
	db.txStartTimes[xid] = time.Now()
	db.txMu.Unlock()

	db.activeTxnsMu.Lock()
	db.activeTxns[tx] = xid
	db.activeTxnsMu.Unlock()

	beginOff, err := db.appendRecord(RecordBegin, xid, nil, nil)
	if err != nil {
		tx.markAborted()
		tx.beginErr = err
	} else {
		db.txMu.Lock()
		db.beginOffsets[xid] = beginOff
		db.txMu.Unlock()
	}
	return tx
}

func (db *DB) appendRecord(recType RecordType, xid uint64, key, value []byte) (int64, error) {
	build := func() []byte {
		return encodeRecord(Record{Type: recType, XID: xid, Key: key, Value: value})
	}
	offsets, err := db.log.AppendRecords([]func() []byte{build}, false)
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (db *DB) ScanLog(startOffset int64, fn func([]Record) error) error {
	if startOffset < atomic.LoadInt64(&db.scanFloor) {
		return ErrLogUnavailable
	}
	return db.log.Scan(startOffset, fn)
}

func (db *DB) SetScanFloor(minOffset int64) error {
	db.txMu.Lock()
	for _, off := range db.beginOffsets {
		if off < minOffset {
			minOffset = off
		}
	}
	db.txMu.Unlock()
	atomic.StoreInt64(&db.scanFloor, minOffset)
	return nil
}

// ApplyLogRange appends a raw byte range of complete log frames and applies
// each statement to the in-memory index. Segments must be statement-aligned
// (whole frames only); partial frames are rejected.
func (db *DB) ApplyLogRange(data []byte) (int64, error) {
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		return 0, errors.New("database is corrupt")
	}
	frames, err := validateFrames(data)
	if err != nil {
		return 0, err
	}
	if len(frames) == 0 {
		return db.log.WriteOffset(), nil
	}

	fsync := frames[len(frames)-1].rec.Type == RecordCommit
	startOff, err := db.log.AppendRawFrames(data, fsync)
	if err != nil {
		return 0, err
	}

	off := startOff
	for _, fr := range frames {
		rec := fr.rec
		switch rec.Type {
		case RecordBegin:
			db.txMu.Lock()
			db.activeXids[rec.XID] = nil
			db.beginOffsets[rec.XID] = off
			db.txMu.Unlock()
		case RecordSet, RecordDelete:
			isDelete := rec.Type == RecordDelete
			db.index.Put(rec.Key, indexVersion{
				offset: off, valueLen: uint32(len(rec.Value)),
				xmin: rec.XID, tombstone: isDelete,
			})
			db.accountReplicatedWrite(rec)
		case RecordCommit:
			db.forgetClog(rec.XID)
			db.txMu.Lock()
			delete(db.activeXids, rec.XID)
			delete(db.beginOffsets, rec.XID)
			impact := db.replImpact[rec.XID]
			delete(db.replImpact, rec.XID)
			db.txMu.Unlock()
			db.applyReplicatedImpact(impact)
		case RecordAbort:
			db.setClog(rec.XID, TxAborted)
			db.index.DropXid(rec.XID)
			db.txMu.Lock()
			delete(db.activeXids, rec.XID)
			delete(db.beginOffsets, rec.XID)
			delete(db.replImpact, rec.XID)
			db.txMu.Unlock()
			db.forgetClog(rec.XID)
		default:
			return 0, fmt.Errorf("unknown log record type: %d", rec.Type)
		}
		db.AdvanceXID(rec.XID)
		off += fr.length
	}
	return off, nil
}

func (db *DB) ReadLogRange(startOffset int64, maxBytes int64) ([]byte, int64, error) {
	return db.log.ReadLogRange(startOffset, maxBytes)
}

func (db *DB) ApplyRecord(rec Record) error {
	if atomic.LoadInt32(&db.isCorrupt) == 1 {
		return errors.New("database is corrupt")
	}
	payload := encodeRecord(rec)

	switch rec.Type {
	case RecordBegin:
		off, err := db.log.AppendEncoded(payload, false)
		if err != nil {
			return err
		}
		db.txMu.Lock()
		db.activeXids[rec.XID] = nil
		db.beginOffsets[rec.XID] = off
		db.txMu.Unlock()
		db.AdvanceXID(rec.XID)
		return nil

	case RecordSet, RecordDelete:
		off, err := db.log.AppendEncoded(payload, false)
		if err != nil {
			atomic.StoreInt32(&db.isCorrupt, 1)
			return err
		}
		isDelete := rec.Type == RecordDelete
		db.index.Put(rec.Key, indexVersion{
			offset: off, valueLen: uint32(len(rec.Value)),
			xmin: rec.XID, tombstone: isDelete,
		})
		db.accountReplicatedWrite(rec)
		db.AdvanceXID(rec.XID)
		return nil

	case RecordCommit:
		if _, err := db.log.AppendEncoded(payload, true); err != nil {
			return err
		}
		db.forgetClog(rec.XID)
		db.txMu.Lock()
		delete(db.activeXids, rec.XID)
		delete(db.beginOffsets, rec.XID)
		impact := db.replImpact[rec.XID]
		delete(db.replImpact, rec.XID)
		db.txMu.Unlock()
		db.applyReplicatedImpact(impact)
		db.AdvanceXID(rec.XID)
		return nil

	case RecordAbort:
		if _, err := db.log.AppendEncoded(payload, false); err != nil {
			return err
		}
		db.setClog(rec.XID, TxAborted)
		db.index.DropXid(rec.XID)
		db.txMu.Lock()
		delete(db.activeXids, rec.XID)
		delete(db.beginOffsets, rec.XID)
		delete(db.replImpact, rec.XID)
		db.txMu.Unlock()
		db.forgetClog(rec.XID)
		db.AdvanceXID(rec.XID)
		return nil
	}
	return fmt.Errorf("unknown log record type: %d", rec.Type)
}

type replTxImpact struct {
	keyDelta        int64
	dispositionSeen map[string]bool
}

func (db *DB) accountReplicatedWrite(rec Record) {
	keyStr := string(rec.Key)
	isDelete := rec.Type == RecordDelete

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
		_ = db.MarkRetention()
		db.commitMu.Unlock()
	case <-time.After(shutdownCommitWait):
		db.logger.Warn("Shutdown: commit batch still active, skipping retention mark", "wait_ms", shutdownCommitWait.Milliseconds())
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

func (db *DB) MarkRetention() error {
	atomic.StoreInt64(&db.retentionOffset, db.log.WriteOffset())
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

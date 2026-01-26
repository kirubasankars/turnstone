package engine

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turnstone/protocol"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	vlogHeaderSize         = 20
	defaultMemCap          = 4 * 1024 * 1024
	maxOpenFiles           = 200
	maxRecyclableBufferCap = 4 * 1024 * 1024
)

var vlogMaxFileSize = 16 * 1024 * 1024

var (
	ErrFlushPending     = errors.New("previous flush still in progress")
	ErrVersionConflict  = errors.New("write conflict: key modified after snapshot")
	ErrReadOOB          = errors.New("read OOB")
	ErrShortHeader      = errors.New("short header")
	ErrShortEntry       = errors.New("short entry")
	ErrSizeMismatch     = errors.New("size mismatch")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrKeyTooLarge      = errors.New("key too large")
)

type Entry struct {
	Key             string
	Value           []byte
	IsDelete        bool
	ExpectedVersion uint64
}

type ValuePointer struct {
	FileID      uint32
	Offset      uint64
	Size        uint32
	TxID        uint64
	IsTombstone bool
}

type cachedFile struct {
	file    *os.File
	element *list.Element
}

type StoneDB struct {
	mu      sync.RWMutex
	dataDir string
	closed  bool

	activeTxMu sync.Mutex
	activeTxns map[uint64]struct{}

	activeFileID int
	activeMem    *bytes.Buffer

	frozenFileID int
	frozenMem    *bytes.Buffer

	emptyBuffers chan *bytes.Buffer

	ldb      *leveldb.DB
	nextTxID uint64

	fileCache map[int]*cachedFile
	lruList   *list.List
	cacheMu   sync.RWMutex

	flushCh chan chan struct{}
	quitCh  chan struct{}
	wg      sync.WaitGroup
}

func Open(dir string, initialTxID uint64) (*StoneDB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	indexDir := filepath.Join(dir, "index")
	_, err := os.Stat(indexDir)
	indexMissing := os.IsNotExist(err)

	ldbOpts := &opt.Options{
		BlockCacheCapacity:     8 * 1024 * 1024,
		WriteBuffer:            4 * 1024 * 1024,
		OpenFilesCacheCapacity: 20,
	}

	ldb, err := leveldb.OpenFile(indexDir, ldbOpts)
	if err != nil {
		if !indexMissing {
			log.Printf("LevelDB corrupted, rebuilding: %v", err)
			os.RemoveAll(indexDir)
			ldb, err = leveldb.OpenFile(indexDir, ldbOpts)
			if err != nil {
				return nil, err
			}
			indexMissing = true
		} else {
			return nil, err
		}
	}

	db := &StoneDB{
		dataDir:      dir,
		ldb:          ldb,
		nextTxID:     initialTxID,
		activeTxns:   make(map[uint64]struct{}),
		activeMem:    bytes.NewBuffer(make([]byte, 0, defaultMemCap)),
		emptyBuffers: make(chan *bytes.Buffer, 2),
		fileCache:    make(map[int]*cachedFile),
		lruList:      list.New(),
		flushCh:      make(chan chan struct{}, 1),
		quitCh:       make(chan struct{}),
	}

	maxID, err := db.setupActiveID()
	if err != nil {
		ldb.Close()
		return nil, err
	}

	if indexMissing {
		if err := db.rebuildIndex(); err != nil {
			db.Close()
			return nil, err
		}
	} else if maxID >= 0 {
		if err := db.recoverVLogFile(maxID); err != nil {
			db.Close()
			return nil, err
		}
	}

	db.wg.Add(2)
	go db.runFlusher()
	go db.runAutoGarbageCollector()

	return db, nil
}

func (db *StoneDB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return protocol.ErrClosed
	}
	db.closed = true

	if db.activeMem.Len() > 0 {
		if db.frozenMem == nil {
			doneCh := make(chan struct{})
			db.rotateMemTableLocked(doneCh)
			db.mu.Unlock()
			<-doneCh
			db.mu.Lock()
		} else {
			db.flushBufferToDisk(db.activeFileID, db.activeMem)
		}
	}
	db.mu.Unlock()

	close(db.quitCh)
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	db.closeAllCachedFiles()
	return db.ldb.Close()
}

func (db *StoneDB) Get(key string, txID uint64) ([]byte, error) {
	seekKey := encodeKey(key, txID)
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	if ok := iter.Seek(seekKey); !ok {
		return nil, protocol.ErrKeyNotFound
	}

	foundKey := iter.Key()
	if len(foundKey) < 9 {
		return nil, protocol.ErrKeyNotFound
	}
	userKeyLen := len(foundKey) - 9
	if string(foundKey[:userKeyLen]) != key {
		return nil, protocol.ErrKeyNotFound
	}

	ptr := decodeValuePointer(iter.Value())
	if ptr.IsTombstone {
		return nil, protocol.ErrKeyNotFound
	}

	var val []byte
	var err error

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, protocol.ErrClosed
	}

	if int(ptr.FileID) == db.activeFileID {
		val, err = db.readMemEntry(db.activeMem, int64(ptr.Offset), int64(ptr.Size))
		db.mu.RUnlock()
		return val, err
	}

	if db.frozenMem != nil && int(ptr.FileID) == db.frozenFileID {
		val, err = db.readMemEntry(db.frozenMem, int64(ptr.Offset), int64(ptr.Size))
		db.mu.RUnlock()
		return val, err
	}
	db.mu.RUnlock()

	val, err = db.readVLogEntry(int(ptr.FileID), int64(ptr.Offset), int64(ptr.Size))
	return val, err
}

func (db *StoneDB) KeyCount() (int64, error) {
	snap, err := db.ldb.GetSnapshot()
	if err != nil {
		return 0, err
	}
	defer snap.Release()

	iter := snap.NewIterator(nil, nil)
	defer iter.Release()

	var count int64
	var lastKey string
	first := true

	for iter.Next() {
		compositeKey := iter.Key()
		if bytes.HasPrefix(compositeKey, []byte("_sys_")) {
			continue
		}
		if len(compositeKey) < 9 {
			continue
		}
		key := string(compositeKey[:len(compositeKey)-9])
		if !first && key == lastKey {
			continue
		}
		ptr := decodeValuePointer(iter.Value())
		if !ptr.IsTombstone {
			count++
		}
		lastKey = key
		first = false
	}
	return count, nil
}

func (db *StoneDB) MemTableSize() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return int64(db.activeMem.Len())
}

func (db *StoneDB) CurrentTxID() uint64 {
	return atomic.LoadUint64(&db.nextTxID)
}

func (db *StoneDB) BeginTx() *Transaction {
	id := atomic.AddUint64(&db.nextTxID, 1)
	db.activeTxMu.Lock()
	db.activeTxns[id] = struct{}{}
	db.activeTxMu.Unlock()
	return &Transaction{ID: id, db: db, readCache: make(map[string]uint64)}
}

type Transaction struct {
	ID        uint64
	Entries   []Entry
	db        *StoneDB
	readCache map[string]uint64
}

func (tx *Transaction) Put(key string, value []byte) {
	entry := Entry{Key: key, Value: value}
	if ver, ok := tx.readCache[key]; ok {
		entry.ExpectedVersion = ver
	}
	tx.Entries = append(tx.Entries, entry)
}

func (tx *Transaction) Delete(key string) {
	entry := Entry{Key: key, Value: nil, IsDelete: true}
	if ver, ok := tx.readCache[key]; ok {
		entry.ExpectedVersion = ver
	}
	tx.Entries = append(tx.Entries, entry)
}

func (tx *Transaction) Abort() {
	tx.db.activeTxMu.Lock()
	delete(tx.db.activeTxns, tx.ID)
	tx.db.activeTxMu.Unlock()
}

func (tx *Transaction) Commit() error {
	return tx.db.CommitTx(tx)
}

func (db *StoneDB) CommitTx(tx *Transaction) error {
	defer func() {
		db.activeTxMu.Lock()
		delete(db.activeTxns, tx.ID)
		db.activeTxMu.Unlock()
	}()

	entries := tx.Entries
	txID := tx.ID
	var totalLen int
	for _, e := range entries {
		valLen := len(e.Value)
		if e.IsDelete {
			valLen = 0
		}
		totalLen += vlogHeaderSize + len(e.Key) + valLen
	}

	type ptrInfo struct {
		compositeKey []byte
		ptr          []byte
	}
	pointers := make([]ptrInfo, 0, len(entries))

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return protocol.ErrClosed
	}

	// Conflict Detection (OCC)
	iter := db.ldb.NewIterator(nil, nil)
	defer iter.Release()

	for _, e := range entries {
		if e.ExpectedVersion > 0 {
			prefix := encodeKeyPrefix(e.Key)
			// Seek to the latest version of the key (since keys are stored as prefix + ^TxID)
			if iter.Seek(prefix) && bytes.HasPrefix(iter.Key(), prefix) {
				ptr := decodeValuePointer(iter.Value())
				if ptr.TxID != e.ExpectedVersion {
					// The key has been updated since we read it
					return ErrVersionConflict
				}
			} else {
				// Key should exist but doesn't
				return ErrVersionConflict
			}
		}
	}

	if db.activeMem.Len()+totalLen > vlogMaxFileSize {
		if db.frozenMem == nil {
			db.rotateMemTableLocked(nil)
		}
	}

	fileID := db.activeFileID
	currentOffset := int64(db.activeMem.Len())
	startLen := db.activeMem.Len()
	db.activeMem.Grow(totalLen)

	for _, e := range entries {
		valToWrite := e.Value
		if e.IsDelete {
			valToWrite = []byte{}
		}
		entrySize, err := appendEntryToBuffer(db.activeMem, e.Key, valToWrite, txID, e.IsDelete)
		if err != nil {
			db.activeMem.Truncate(startLen)
			return err
		}
		compositeKey := encodeKey(e.Key, txID)
		ptrBytes := encodeValuePointer(uint32(fileID), uint64(currentOffset), uint32(len(valToWrite)), txID, e.IsDelete)
		pointers = append(pointers, ptrInfo{compositeKey, ptrBytes})
		currentOffset += int64(entrySize)
	}

	batch := new(leveldb.Batch)
	for _, p := range pointers {
		batch.Put(p.compositeKey, p.ptr)
	}
	return db.ldb.Write(batch, &opt.WriteOptions{Sync: false})
}

func (db *StoneDB) rotateMemTableLocked(doneCh chan struct{}) {
	db.frozenMem = db.activeMem
	db.frozenFileID = db.activeFileID
	db.activeFileID++
	select {
	case buf := <-db.emptyBuffers:
		db.activeMem = buf
	default:
		db.activeMem = bytes.NewBuffer(make([]byte, 0, defaultMemCap))
	}
	select {
	case db.flushCh <- doneCh:
	default:
		if doneCh != nil {
			close(doneCh)
		}
	}
}

func (db *StoneDB) runFlusher() {
	defer db.wg.Done()
	for {
		select {
		case doneCh := <-db.flushCh:
			db.mu.Lock()
			if db.frozenMem == nil {
				db.mu.Unlock()
				if doneCh != nil {
					close(doneCh)
				}
				continue
			}
			id := db.frozenFileID
			buf := db.frozenMem
			db.mu.Unlock()

			backoff := 100 * time.Millisecond
			for {
				if err := db.flushBufferToDisk(id, buf); err == nil {
					break
				}
				select {
				case <-db.quitCh:
					return
				case <-time.After(backoff):
					if backoff < 5*time.Second {
						backoff *= 2
					}
				}
			}

			db.mu.Lock()
			buf.Reset()
			db.frozenMem = nil
			db.frozenFileID = 0
			if buf.Cap() <= maxRecyclableBufferCap {
				select {
				case db.emptyBuffers <- buf:
				default:
				}
			}
			db.mu.Unlock()

			if doneCh != nil {
				close(doneCh)
			}
		case <-db.quitCh:
			return
		}
	}
}

func (db *StoneDB) flushBufferToDisk(id int, buf *bytes.Buffer) error {
	vlogName := filepath.Join(db.dataDir, fmt.Sprintf("%d.vlog", id))
	f, err := os.OpenFile(vlogName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func appendEntryToBuffer(buf *bytes.Buffer, key string, value []byte, txID uint64, isDelete bool) (int, error) {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))
	if keyLen > 65535 {
		return 0, ErrKeyTooLarge
	}
	meta := packStoneMeta(keyLen, valLen, isDelete)

	var header [vlogHeaderSize]byte
	binary.BigEndian.PutUint64(header[4:12], txID)
	binary.BigEndian.PutUint64(header[12:20], meta)
	binary.BigEndian.PutUint32(header[0:4], 0) // CRC placeholder

	startOffset := buf.Len()
	buf.Write(header[:])
	buf.WriteString(key)
	buf.Write(value)

	data := buf.Bytes()[startOffset:]
	crc := crc32.Checksum(data[4:], protocol.Crc32Table)
	binary.BigEndian.PutUint32(data[0:4], crc)

	return vlogHeaderSize + int(keyLen) + int(valLen), nil
}

func (db *StoneDB) readMemEntry(buf *bytes.Buffer, offset int64, valSize int64) ([]byte, error) {
	data := buf.Bytes()
	if offset < 0 || offset >= int64(len(data)) {
		return nil, ErrReadOOB
	}
	if offset+vlogHeaderSize > int64(len(data)) {
		return nil, ErrShortHeader
	}
	headerBuf := data[offset : offset+vlogHeaderSize]
	meta := binary.BigEndian.Uint64(headerBuf[12:20])
	keySize, diskValSize, _ := unpackStoneMeta(meta)

	if int64(diskValSize) != valSize {
		return nil, ErrSizeMismatch
	}

	totalSize := int64(vlogHeaderSize + keySize + diskValSize)
	if offset+totalSize > int64(len(data)) {
		return nil, ErrShortEntry
	}

	entryData := data[offset : offset+totalSize]
	storedCrc := binary.BigEndian.Uint32(entryData[0:4])
	if crc32.Checksum(entryData[4:], protocol.Crc32Table) != storedCrc {
		return nil, ErrChecksumMismatch
	}
	return entryData[vlogHeaderSize+int64(keySize):], nil
}

func (db *StoneDB) readVLogEntry(fileID int, offset int64, valSize int64) ([]byte, error) {
	file, err := db.getFileHandle(fileID)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, protocol.ErrKeyNotFound
		}
		return nil, err
	}
	headerBuf := make([]byte, vlogHeaderSize)
	if _, err := file.ReadAt(headerBuf, offset); err != nil {
		return nil, err
	}

	meta := binary.BigEndian.Uint64(headerBuf[12:20])
	keySize, diskValSize, _ := unpackStoneMeta(meta)
	if int64(diskValSize) != valSize {
		return nil, ErrSizeMismatch
	}

	bodySize := int64(keySize + diskValSize)
	body := make([]byte, bodySize)
	if _, err := file.ReadAt(body, offset+vlogHeaderSize); err != nil {
		return nil, err
	}

	crc := crc32.Checksum(headerBuf[4:], protocol.Crc32Table)
	crc = crc32.Update(crc, protocol.Crc32Table, body)

	if crc != binary.BigEndian.Uint32(headerBuf[0:4]) {
		return nil, ErrChecksumMismatch
	}
	return body[keySize:], nil
}

func (db *StoneDB) getFileHandle(fileID int) (*os.File, error) {
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()
	if cf, ok := db.fileCache[fileID]; ok {
		db.lruList.MoveToFront(cf.element)
		return cf.file, nil
	}
	path := filepath.Join(db.dataDir, fmt.Sprintf("%d.vlog", fileID))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if db.lruList.Len() >= maxOpenFiles {
		oldest := db.lruList.Back()
		if oldest != nil {
			oldestID := oldest.Value.(int)
			if oldCF, ok := db.fileCache[oldestID]; ok {
				oldCF.file.Close()
				delete(db.fileCache, oldestID)
			}
			db.lruList.Remove(oldest)
		}
	}
	elem := db.lruList.PushFront(fileID)
	db.fileCache[fileID] = &cachedFile{file: f, element: elem}
	return f, nil
}

func (db *StoneDB) closeAllCachedFiles() {
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()
	for _, cf := range db.fileCache {
		cf.file.Close()
	}
	db.fileCache = make(map[int]*cachedFile)
	db.lruList = list.New()
}

func (db *StoneDB) setupActiveID() (int, error) {
	files, _ := os.ReadDir(db.dataDir)
	maxID := -1
	for _, f := range files {
		name := f.Name()
		if strings.HasSuffix(name, ".vlog") {
			base := strings.TrimSuffix(name, ".vlog")
			if val, err := strconv.Atoi(base); err == nil {
				if val > maxID {
					maxID = val
				}
			}
		}
	}
	db.activeFileID = maxID + 1
	return maxID, nil
}

func encodeKey(key string, txID uint64) []byte {
	buf := make([]byte, len(key)+9)
	copy(buf, key)
	buf[len(key)] = 0
	binary.BigEndian.PutUint64(buf[len(key)+1:], ^txID)
	return buf
}

func encodeKeyPrefix(key string) []byte {
	buf := make([]byte, len(key)+1)
	copy(buf, key)
	buf[len(key)] = 0
	return buf
}

func encodeValuePointer(fid uint32, off uint64, size uint32, txID uint64, isTombstone bool) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint32(buf[0:4], fid)
	binary.BigEndian.PutUint64(buf[4:12], off)
	if isTombstone {
		size |= (1 << 31)
	}
	binary.BigEndian.PutUint32(buf[12:16], size)
	binary.BigEndian.PutUint64(buf[16:24], txID)
	return buf
}

func decodeValuePointer(data []byte) ValuePointer {
	size := binary.BigEndian.Uint32(data[12:16])
	isTombstone := (size & (1 << 31)) != 0
	size &= ^(uint32(1 << 31))
	return ValuePointer{
		FileID:      binary.BigEndian.Uint32(data[0:4]),
		Offset:      binary.BigEndian.Uint64(data[4:12]),
		Size:        size,
		TxID:        binary.BigEndian.Uint64(data[16:24]),
		IsTombstone: isTombstone,
	}
}

func packStoneMeta(keyLen, valLen uint32, isDelete bool) uint64 {
	var meta uint64
	if isDelete {
		meta |= (1 << 63)
	}
	meta |= (uint64(keyLen) & 0xFFFF) << 32
	meta |= (uint64(valLen) & 0xFFFFFFFF)
	return meta
}

func unpackStoneMeta(meta uint64) (keyLen uint32, valLen uint32, isDelete bool) {
	isDelete = (meta & (1 << 63)) != 0
	keyLen = uint32((meta >> 32) & 0xFFFF)
	valLen = uint32(meta & 0xFFFFFFFF)
	return
}

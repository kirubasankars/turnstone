package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"turnstone/protocol"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	gcInterval  = 1 * time.Minute
	gcBatchSize = 1024 * 1024 // 1MB chunks
)

type gcOperation struct {
	key        string
	value      []byte
	txID       uint64
	origFileID uint32
	origOffset uint64
	isDelete   bool
}

func (db *StoneDB) rebuildIndex() error {
	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	var ids []int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".vlog") {
			base := strings.TrimSuffix(e.Name(), ".vlog")
			if id, err := strconv.Atoi(base); err == nil {
				ids = append(ids, id)
			}
		}
	}
	sort.Ints(ids)
	for _, id := range ids {
		if err := db.recoverVLogFile(id); err != nil {
			return err
		}
	}
	return nil
}

func (db *StoneDB) recoverVLogFile(fileID int) error {
	path := filepath.Join(db.dataDir, fmt.Sprintf("%d.vlog", fileID))
	f, err := os.OpenFile(path, os.O_RDWR, 0o666)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var offset int64 = 0
	var maxTxID uint64 = 0
	batch := new(leveldb.Batch)
	batchCount := 0

	for {
		headerBuf, err := reader.Peek(vlogHeaderSize)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		txID := binary.BigEndian.Uint64(headerBuf[4:12])
		if txID > maxTxID {
			maxTxID = txID
		}
		meta := binary.BigEndian.Uint64(headerBuf[12:20])
		keySize, valSize, isDelete := unpackStoneMeta(meta)
		totalSize := int64(vlogHeaderSize + keySize + valSize)

		data := make([]byte, totalSize)
		if _, err := io.ReadFull(reader, data); err != nil {
			f.Truncate(offset)
			f.Sync()
			break
		}

		if crc32.Checksum(data[4:], protocol.Crc32Table) != binary.BigEndian.Uint32(data[0:4]) {
			f.Truncate(offset)
			f.Sync()
			break
		}

		key := string(data[vlogHeaderSize : vlogHeaderSize+keySize])
		compositeKey := encodeKey(key, txID)
		ptrBytes := encodeValuePointer(uint32(fileID), uint64(offset), valSize, txID, isDelete)
		batch.Put(compositeKey, ptrBytes)
		batchCount++
		offset += totalSize
	}

	curr := atomic.LoadUint64(&db.nextTxID)
	if maxTxID > curr {
		atomic.StoreUint64(&db.nextTxID, maxTxID)
	}
	if batchCount > 0 {
		return db.ldb.Write(batch, &opt.WriteOptions{Sync: true})
	}
	return nil
}

func (db *StoneDB) runAutoGarbageCollector() {
	defer db.wg.Done()
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.quitCh:
			return
		case <-ticker.C:
			db.pickAndRunGC()
		}
	}
}

func (db *StoneDB) pickAndRunGC() {
	var candidates []int
	files, _ := os.ReadDir(db.dataDir)
	db.mu.RLock()
	activeID := db.activeFileID
	frozenID := db.frozenFileID
	hasFrozen := db.frozenMem != nil
	db.mu.RUnlock()

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".vlog") {
			base := strings.TrimSuffix(f.Name(), ".vlog")
			if id, err := strconv.Atoi(base); err == nil {
				isFrozen := hasFrozen && id == frozenID
				if id != activeID && !isFrozen {
					candidates = append(candidates, id)
				}
			}
		}
	}
	if len(candidates) > 0 {
		db.RunVLogGC(candidates[rand.Intn(len(candidates))])
	}
}

func (db *StoneDB) RunVLogGC(fileID int) error {
	vlogPath := filepath.Join(db.dataDir, fmt.Sprintf("%d.vlog", fileID))
	f, err := os.Open(vlogPath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var offset int64 = 0
	var batch []gcOperation
	var deletes [][]byte
	var batchSize int

	db.activeTxMu.Lock()
	minActive := uint64(math.MaxUint64)
	if len(db.activeTxns) > 0 {
		for id := range db.activeTxns {
			if id < minActive {
				minActive = id
			}
		}
	}
	db.activeTxMu.Unlock()

	for {
		headerBuf, err := reader.Peek(vlogHeaderSize)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		txID := binary.BigEndian.Uint64(headerBuf[4:12])
		meta := binary.BigEndian.Uint64(headerBuf[12:20])
		keySize, valSize, isDelete := unpackStoneMeta(meta)
		totalSize := int64(vlogHeaderSize + keySize + valSize)
		data := make([]byte, totalSize)
		if _, err := io.ReadFull(reader, data); err != nil {
			return err
		}

		key := string(data[vlogHeaderSize : vlogHeaderSize+keySize])
		value := data[vlogHeaderSize+keySize:]
		isLive := false

		iter := db.ldb.NewIterator(nil, nil)
		prefix := encodeKeyPrefix(key)
		if iter.Seek(prefix) && bytes.HasPrefix(iter.Key(), prefix) {
			ptr := decodeValuePointer(iter.Value())
			if int(ptr.FileID) == fileID && int64(ptr.Offset) == offset {
				isLive = true
			} else if txID >= minActive {
				isLive = true
			}
		}
		iter.Release()

		if isLive {
			batch = append(batch, gcOperation{key, value, txID, uint32(fileID), uint64(offset), isDelete})
			batchSize += len(value)
		} else {
			deletes = append(deletes, encodeKey(key, txID))
		}
		offset += totalSize

		if batchSize >= gcBatchSize {
			db.writeGCBatch(batch, deletes)
			batch = nil
			deletes = nil
			batchSize = 0
		}
	}
	if len(batch) > 0 || len(deletes) > 0 {
		db.writeGCBatch(batch, deletes)
	}

	for i := 0; i < 60; i++ {
		err := db.flushBufferToDisk(db.activeFileID, db.activeMem)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	f.Close()

	db.cacheMu.Lock()
	if cf, ok := db.fileCache[fileID]; ok {
		cf.file.Close()
		db.lruList.Remove(cf.element)
		delete(db.fileCache, fileID)
	}
	db.cacheMu.Unlock()

	return os.Remove(vlogPath)
}

func (db *StoneDB) writeGCBatch(ops []gcOperation, deletes [][]byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activeMem.Len() > vlogMaxFileSize {
		if db.frozenMem == nil {
			db.rotateMemTableLocked(nil)
		}
	}

	batch := new(leveldb.Batch)
	currentOffset := int64(db.activeMem.Len())

	for _, op := range ops {
		appendEntryToBuffer(db.activeMem, op.key, op.value, op.txID, op.isDelete)
		ptrBytes := encodeValuePointer(uint32(db.activeFileID), uint64(currentOffset), uint32(len(op.value)), op.txID, op.isDelete)
		batch.Put(encodeKey(op.key, op.txID), ptrBytes)
		currentOffset += int64(vlogHeaderSize + len(op.key) + len(op.value))
	}
	for _, key := range deletes {
		batch.Delete(key)
	}
	return db.ldb.Write(batch, &opt.WriteOptions{Sync: false})
}

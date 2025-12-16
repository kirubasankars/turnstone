package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

// --- Constants ---

const (
	JournalFile        = "data/data.db"
	ViewBuilderDB      = "data/views/index.db"
	CSVFile            = "data/views/data.csv"
	BucketIndices      = "indices"
	BucketMeta         = "meta"
	KeyLastOffset      = "last_offset"
	KeyLastGen         = "last_gen"
	FileHeaderSize     = 8 // Generation Header Size (Uint64)
	HeaderSize         = 12
	Tombstone          = ^uint32(0)
	ServerAddr         = "localhost:6379"
	OpCodeStat         = 0x20
	ResStatusOK        = 0x00
	CheckpointInterval = 5000
	RecordSize         = 12 // 8 bytes offset + 4 bytes length
	MaxPayloadSize     = 64 * 1024 * 1024
)

var CrcTable = crc32.MakeTable(crc32.Castagnoli)

// --- Structs ---

type Version struct {
	Offset int64
	Length int32
}

// --- State Management ---

type StateStore struct {
	db *bbolt.DB
}

func NewStateStore(path string) (*StateStore, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(BucketIndices)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(BucketMeta)); err != nil {
			return err
		}
		return nil
	})
	return &StateStore{db: db}, err
}

func (s *StateStore) Close() {
	s.db.Close()
}

func (s *StateStore) GetLastState() (uint64, int64) {
	var gen uint64
	var offset int64
	s.db.View(func(tx *bbolt.Tx) error {
		meta := tx.Bucket([]byte(BucketMeta))
		if v := meta.Get([]byte(KeyLastGen)); v != nil {
			gen = binary.BigEndian.Uint64(v)
		}
		if v := meta.Get([]byte(KeyLastOffset)); v != nil {
			offset = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return gen, offset
}

// GetLatestVersion returns the most recent {Offset, Length} for a key from the DB.
func (s *StateStore) GetLatestVersion(key string) (Version, bool) {
	var ver Version
	found := false
	s.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket([]byte(BucketIndices)).Get([]byte(key))
		if len(v) >= RecordSize {
			// Read the last 12 bytes
			start := len(v) - RecordSize
			record := v[start:]
			ver.Offset = int64(binary.BigEndian.Uint64(record[0:8]))
			ver.Length = int32(binary.BigEndian.Uint32(record[8:12]))
			found = true
		}
		return nil
	})
	return ver, found
}

// BatchUpdate appends new versions to the existing history in BoltDB.
func (s *StateStore) BatchUpdate(updates map[string][]Version, lastGen uint64, lastOffset int64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		indices := tx.Bucket([]byte(BucketIndices))
		meta := tx.Bucket([]byte(BucketMeta))

		for key, newVersions := range updates {
			// 1. Get existing data
			existing := indices.Get([]byte(key))

			// 2. Calculate new size
			needed := len(existing) + (len(newVersions) * RecordSize)

			// 3. Allocate
			newData := make([]byte, needed)
			copy(newData, existing)

			// 4. Append new versions
			ptr := len(existing)
			for _, v := range newVersions {
				binary.BigEndian.PutUint64(newData[ptr:], uint64(v.Offset))
				binary.BigEndian.PutUint32(newData[ptr+8:], uint32(v.Length))
				ptr += RecordSize
			}

			// 5. Store
			if err := indices.Put([]byte(key), newData); err != nil {
				return err
			}
		}

		// Update Checkpoint (Gen + Offset)
		// We use two separate buffers to ensure no aliasing issues inside the tx
		bufOff := make([]byte, 8)
		binary.BigEndian.PutUint64(bufOff, uint64(lastOffset))
		if err := meta.Put([]byte(KeyLastOffset), bufOff); err != nil {
			return err
		}

		bufGen := make([]byte, 8)
		binary.BigEndian.PutUint64(bufGen, lastGen)
		return meta.Put([]byte(KeyLastGen), bufGen)
	})
}

// --- Network ---

func getServerLimit() (uint64, int64, error) {
	conn, err := net.Dial("tcp", ServerAddr)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	req := make([]byte, 5)
	req[0] = OpCodeStat
	if _, err := conn.Write(req); err != nil {
		return 0, 0, err
	}

	respHeader := make([]byte, 5)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		return 0, 0, err
	}

	if respHeader[0] != ResStatusOK {
		return 0, 0, errors.New("server error status")
	}

	length := binary.BigEndian.Uint32(respHeader[1:])
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return 0, 0, err
	}

	// Response format: "Keys:%d Uptime:%s ... Offset:%d,%d" (Generation,Offset)
	for _, p := range strings.Split(string(payload), " ") {
		if strings.HasPrefix(p, "Offset:") {
			parts := strings.Split(strings.TrimPrefix(p, "Offset:"), ",")
			if len(parts) == 2 {
				gen, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					return 0, 0, fmt.Errorf("bad generation: %w", err)
				}
				off, err := strconv.ParseInt(parts[1], 10, 64)
				if err != nil {
					return 0, 0, fmt.Errorf("bad offset: %w", err)
				}
				return gen, off, nil
			}
			// Fallback if no generation provided (though store should provide it)
			off, err := strconv.ParseInt(parts[0], 10, 64)
			return 0, off, err
		}
	}
	return 0, 0, errors.New("offset not found")
}

// --- Main Logic ---

func main() {
	daemonMode := flag.Bool("daemon", true, "Run continuously")
	interval := flag.Duration("interval", 10*time.Second, "Poll interval")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("Starting View Builder | Daemon: %v", *daemonMode)

	if err := os.MkdirAll(filepath.Dir(ViewBuilderDB), 0755); err != nil {
		log.Fatal(err)
	}

	state, err := NewStateStore(ViewBuilderDB)
	if err != nil {
		log.Fatal(err)
	}
	defer state.Close()

	csvFile, err := os.OpenFile(CSVFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	// Init Check
	_, lastOff := state.GetLastState()
	if lastOff == 0 {
		if stat, _ := csvFile.Stat(); stat.Size() > 0 {
			log.Println("Truncating CSV for fresh index.")
			if err := csvFile.Truncate(0); err != nil {
				log.Fatalf("Failed to truncate CSV: %v", err)
			}
			if _, err := csvFile.Seek(0, 0); err != nil {
				log.Fatalf("Failed to seek CSV: %v", err)
			}
		}
	}
	// Initial seek to end (essential for appending)
	if _, err := csvFile.Seek(0, 2); err != nil {
		log.Fatalf("Failed to seek to end of CSV: %v", err)
	}

	for {
		if err := processBatch(state, csvFile); err != nil {
			log.Printf("Batch error: %v", err)
		}
		if !*daemonMode {
			break
		}
		time.Sleep(*interval)
	}
}

func processBatch(state *StateStore, csvFile *os.File) error {
	// 1. Get Server State (Truth Source)
	serverGen, serverOffsetLimit, err := getServerLimit()
	if err != nil {
		return err
	}

	// 2. Validate Journal File Identity
	// We read the first 8 bytes to ensure the file on disk matches the server's generation.
	journal, err := os.Open(JournalFile)
	if err != nil {
		return err
	}
	defer journal.Close()

	header := make([]byte, FileHeaderSize)
	if _, err := journal.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read journal header: %w", err)
	}
	fileGen := binary.BigEndian.Uint64(header)

	// Identity Verification
	if fileGen != serverGen {
		return fmt.Errorf("journal identity mismatch: file_gen=%d server_gen=%d (retry later)", fileGen, serverGen)
	}

	// 3. Get Local State
	localGen, localOffset := state.GetLastState()

	// 4. Handle Generation Changes / Initialization
	if localGen != fileGen {
		log.Printf("[Sync] Generation Change: Local=%d -> Server/File=%d. Resetting offset.", localGen, fileGen)
		localGen = fileGen
		localOffset = int64(FileHeaderSize)

		// Critical: Persist the new generation anchor immediately to avoid loops.
		if err := state.BatchUpdate(nil, localGen, localOffset); err != nil {
			return fmt.Errorf("failed to persist state reset: %w", err)
		}
	}

	// Safety: Ensure we don't read before the header
	if localOffset < int64(FileHeaderSize) {
		localOffset = int64(FileHeaderSize)
	}

	// Check if we are caught up
	if localOffset >= serverOffsetLimit {
		return nil
	}

	log.Printf("[Sync] Gen: %d | Processing bytes %d to %d (Lag: %d)", localGen, localOffset, serverOffsetLimit, serverOffsetLimit-localOffset)

	// 5. Processing Loop
	// Seek to where we left off
	if _, err := journal.Seek(localOffset, 0); err != nil {
		return err
	}
	reader := bufio.NewReader(journal)

	// Prepare CSV Writer
	bufWriter := bufio.NewWriterSize(csvFile, 64*1024)
	virtualOffset, err := csvFile.Seek(0, 2) // Append mode
	if err != nil {
		return err
	}

	updates := make(map[string][]Version, CheckpointInterval)
	count := 0

	// Flush closure
	flush := func(currentOffset int64) error {
		if err := bufWriter.Flush(); err != nil {
			return err
		}
		if err := state.BatchUpdate(updates, localGen, currentOffset); err != nil {
			return err
		}
		updates = make(map[string][]Version, CheckpointInterval)
		return nil
	}

	for localOffset < serverOffsetLimit {
		key, val, isDel, bytesRead, err := readJournalEntry(reader, localOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Handle Deletes / Updates (Same logic as before)
		var lastVer Version
		var hasLast bool

		if batchList, ok := updates[key]; ok && len(batchList) > 0 {
			lastVer = batchList[len(batchList)-1]
			hasLast = true
		} else {
			lastVer, hasLast = state.GetLatestVersion(key)
		}

		if hasLast {
			if err := bufWriter.Flush(); err != nil {
				return err
			}
			if _, err := csvFile.WriteAt([]byte("1"), lastVer.Offset); err != nil {
				return fmt.Errorf("failed to mark deleted at %d: %w", lastVer.Offset, err)
			}
			if _, err := csvFile.Seek(virtualOffset, 0); err != nil {
				return err
			}
		}

		if !isDel {
			n, err := appendRow(bufWriter, key, val)
			if err != nil {
				return err
			}
			v := Version{Offset: virtualOffset, Length: int32(n)}
			updates[key] = append(updates[key], v)
			virtualOffset += int64(n)
		}

		localOffset += bytesRead
		count++

		if count%CheckpointInterval == 0 {
			if err := flush(localOffset); err != nil {
				return err
			}
		}
	}

	return flush(localOffset)
}

// --- Helpers ---

func readJournalEntry(r io.Reader, offset int64) (key string, val []byte, isDel bool, n int64, err error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return "", nil, false, 0, err
	}

	keyLen := binary.BigEndian.Uint32(header[0:4])
	valLen := binary.BigEndian.Uint32(header[4:8])
	crc := binary.BigEndian.Uint32(header[8:12])

	payloadLen := int64(keyLen)
	if valLen != Tombstone {
		payloadLen += int64(valLen)
	}

	// FIX: Sanity check to prevent OOM on corrupt file
	if payloadLen > MaxPayloadSize {
		return "", nil, false, 0, fmt.Errorf("corruption: payload too large (%d) at %d", payloadLen, offset)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", nil, false, 0, err
	}

	if crc32.Checksum(payload, CrcTable) != crc {
		return "", nil, false, 0, fmt.Errorf("crc mismatch at %d", offset)
	}

	key = string(payload[:keyLen])
	isDel = (valLen == Tombstone)
	if !isDel {
		val = payload[keyLen:]
	}
	return key, val, isDel, int64(HeaderSize) + payloadLen, nil
}

// readValueAt uses the stored length to read efficiently using standard CSV logic
func readValueAt(f *os.File, ver Version) (string, error) {
	if ver.Length <= 0 {
		return "", nil
	}

	buf := make([]byte, ver.Length)
	if _, err := f.ReadAt(buf, ver.Offset); err != nil {
		return "", err
	}

	// Format is: 0,"Key","Value"\n
	r := csv.NewReader(bytes.NewReader(buf))
	r.FieldsPerRecord = -1 // Flexible

	record, err := r.Read()
	if err != nil {
		return "", err
	}

	// Expect 3 fields: Status, Key, Value
	if len(record) >= 3 {
		return record[2], nil
	}

	return "", nil
}

// appendRow writes data in Standard CSV format (RFC 4180).
// Format: status,"key","value"
// Quotes inside key/value are escaped as ""
func appendRow(w *bufio.Writer, key string, val []byte) (int, error) {
	var total int

	// 1. Status (0 = Active)
	n, err := w.WriteString("0,")
	total += n
	if err != nil {
		return total, err
	}

	// 2. Key (Quoted & Escaped)
	qKey := escapeCSV(key)
	n, err = w.WriteString(`"` + qKey + `",`)
	total += n
	if err != nil {
		return total, err
	}

	// 3. Value (Quoted & Escaped)
	// Convert bytes to string for CSV compatibility.
	// Note: This assumes binary data doesn't break basic string functions.
	// Standard CSV readers handle newlines/commas fine inside quotes.
	qVal := escapeCSV(string(val))
	n, err = w.WriteString(`"` + qVal + `"`)
	total += n
	if err != nil {
		return total, err
	}

	n, err = w.WriteRune('\n')
	total += n

	return total, err
}

func escapeCSV(s string) string {
	if !strings.Contains(s, `"`) {
		return s
	}
	return strings.ReplaceAll(s, `"`, `""`)
}


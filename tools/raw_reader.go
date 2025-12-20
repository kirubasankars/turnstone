package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

// --- Types for Index Decoding (Self-contained) ---

type IndexEntry uint64

const indexEntryDeletedMask uint64 = 1 << 63

func (e IndexEntry) Offset() int64 {
	return int64(uint64(e) &^ indexEntryDeletedMask)
}

func (e IndexEntry) Deleted() bool {
	return (uint64(e) & indexEntryDeletedMask) != 0
}

func decodeHistory(data []byte) []IndexEntry {
	if len(data) < 4 {
		return nil
	}
	r := bytes.NewReader(data)
	var count uint32
	_ = binary.Read(r, binary.LittleEndian, &count)
	hist := make([]IndexEntry, count)
	for i := 0; i < int(count); i++ {
		var packed uint64
		_ = binary.Read(r, binary.LittleEndian, &packed)
		hist[i] = IndexEntry(packed)
	}
	return hist
}

const HeaderSize = 20

// --- Main Tool Logic ---

func main() {
	// 1. Flags
	path := flag.String("path", "", "Path to file (.db or .idx)")
	mode := flag.String("mode", "", "Mode: journal, bolt-pair, bolt-index, bolt-meta, verify (default: auto-detect)")

	offset := flag.Int64("offset", -1, "Int64 Offset (for Journal seek or Pair lookup)")
	key := flag.String("key", "", "String Key (for Index/Meta lookup)")
	limit := flag.Int("limit", 0, "Read size (bytes) or Entry count (rows)")

	flag.Usage = printUsage
	flag.Parse()

	if *path == "" {
		printUsage()
		os.Exit(1)
	}

	// 2. Auto-detect mode if not provided
	selectedMode := *mode
	if selectedMode == "" {
		if strings.HasSuffix(*path, ".db") {
			selectedMode = "journal"
		} else if strings.HasSuffix(*path, ".idx") {
			selectedMode = "bolt-pair" // Default to pair inspection for indexes
		} else {
			fmt.Println("Error: Cannot infer mode from file extension. Please use -mode.")
			os.Exit(1)
		}
	}

	// 3. Dispatch
	switch selectedMode {
	case "journal":
		startOff := *offset
		if startOff < 0 {
			startOff = 0
		}
		debugJournal(*path, startOff, *limit)
	case "bolt-pair":
		debugBoltPair(*path, *offset, *limit)
	case "bolt-index":
		debugBoltIndex(*path, *key, *limit)
	case "bolt-meta":
		debugBoltMeta(*path, *key)
	case "verify":
		verifyIntegrity(*path)
	default:
		fmt.Printf("Error: Unknown mode '%s'\n", selectedMode)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("TurnstoneDB Debug Tool")
	fmt.Println("Usage: go run debug_reader.go -path <file> [flags]")
	fmt.Println("\nFlags:")
	flag.PrintDefaults()
	fmt.Println("\nExamples:")
	fmt.Println("  Journal Read:    go run debug_reader.go -path data/1.db -offset 100")
	fmt.Println("  Bolt Pair (1):   go run debug_reader.go -path data/1.idx -mode bolt-pair -offset 94")
	fmt.Println("  Bolt Pair (All): go run debug_reader.go -path data/1.idx -mode bolt-pair -limit 20")
	fmt.Println("  Bolt Index (1):  go run debug_reader.go -path data/1.idx -mode bolt-index -key myUser")
	fmt.Println("  Verify Match:    go run debug_reader.go -path data/1.db -mode verify")
}

// --- BoltDB Handlers ---

func openBolt(path string) (*bbolt.DB, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
}

func debugBoltPair(path string, lookupOffset int64, limit int) {
	db, err := openBolt(path)
	if err != nil {
		fmt.Printf("Error opening BoltDB: %v\n", err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("pair"))
		if b == nil {
			return fmt.Errorf("bucket 'pair' not found")
		}

		// Single Lookup
		if lookupOffset >= 0 {
			fmt.Printf("Looking up Offset: %d\n", lookupOffset)
			var kBuf [8]byte
			binary.BigEndian.PutUint64(kBuf[:], uint64(lookupOffset))
			v := b.Get(kBuf[:])
			if v == nil {
				fmt.Println("-> Not Found")
			} else {
				lenVal := binary.BigEndian.Uint64(v)
				fmt.Printf("-> Found: Length = %d\n", lenVal)
			}
			return nil
		}

		// Scan
		fmt.Println("Scanning 'pair' bucket...")
		c := b.Cursor()
		count := 0
		max := limit
		if max <= 0 {
			max = 50 // Default scan limit
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(k) != 8 {
				fmt.Printf("WARN: Invalid Key Len %d\n", len(k))
				continue
			}
			off := binary.BigEndian.Uint64(k)
			lenVal := binary.BigEndian.Uint64(v)
			fmt.Printf("  Offset %-10d : Length %d\n", off, lenVal)
			count++
			if count >= max {
				fmt.Println("... (limit reached)")
				break
			}
		}
		fmt.Printf("Total displayed: %d\n", count)
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func debugBoltIndex(path string, lookupKey string, limit int) {
	db, err := openBolt(path)
	if err != nil {
		fmt.Printf("Error opening BoltDB: %v\n", err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("index"))
		if b == nil {
			return fmt.Errorf("bucket 'index' not found")
		}

		// Single Lookup
		if lookupKey != "" {
			fmt.Printf("Looking up Key: %q\n", lookupKey)
			v := b.Get([]byte(lookupKey))
			if v == nil {
				fmt.Println("-> Not Found")
			} else {
				hist := decodeHistory(v)
				fmt.Printf("-> Found %d versions:\n", len(hist))
				for _, h := range hist {
					status := "SET"
					if h.Deleted() {
						status = "DEL"
					}
					fmt.Printf("   Offset: %-10d [%s]\n", h.Offset(), status)
				}
			}
			return nil
		}

		// Scan
		fmt.Println("Scanning 'index' bucket...")
		c := b.Cursor()
		count := 0
		max := limit
		if max <= 0 {
			max = 20
		}

		for k, v := c.First(); k != nil; k, v = c.Next() {
			hist := decodeHistory(v)
			latest := "N/A"
			if len(hist) > 0 {
				last := hist[len(hist)-1]
				if last.Deleted() {
					latest = fmt.Sprintf("DEL @ %d", last.Offset())
				} else {
					latest = fmt.Sprintf("SET @ %d", last.Offset())
				}
			}
			fmt.Printf("  Key: %-15q : %s (%d vers)\n", k, latest, len(hist))
			count++
			if count >= max {
				fmt.Println("... (limit reached)")
				break
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func debugBoltMeta(path string, lookupKey string) {
	db, err := openBolt(path)
	if err != nil {
		fmt.Printf("Error opening BoltDB: %v\n", err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		if b == nil {
			return fmt.Errorf("bucket 'meta' not found")
		}

		if lookupKey != "" {
			v := b.Get([]byte(lookupKey))
			fmt.Printf("Key: %s\nValue: %v\n", lookupKey, v)
			if len(v) == 8 {
				fmt.Printf("Value (uint64): %d\n", binary.BigEndian.Uint64(v))
			} else {
				fmt.Printf("Value (string): %s\n", string(v))
			}
			return nil
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("%-15s : ", k)
			if len(v) == 8 {
				fmt.Printf("%d (int64)\n", binary.BigEndian.Uint64(v))
			} else {
				fmt.Printf("%s (string)\n", string(v))
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

// --- Journal Handler ---

func debugJournal(filePath string, offset int64, length int) {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	info, _ := f.Stat()
	fileSize := info.Size()

	if offset >= fileSize {
		fmt.Printf("Error: Offset %d is beyond file size %d\n", offset, fileSize)
		os.Exit(1)
	}

	fmt.Printf("File: %s (Size: %d)\n", filePath, fileSize)
	fmt.Printf("Seeking to offset: %d\n", offset)

	_, err = f.Seek(offset, 0)
	if err != nil {
		fmt.Printf("Error seeking: %v\n", err)
		os.Exit(1)
	}

	readSize := length
	if readSize <= 0 {
		readSize = int(fileSize - offset)
		fmt.Printf("Auto-detected length: reading remaining %d bytes\n", readSize)
	}

	buf := make([]byte, readSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		fmt.Printf("Error reading: %v\n", err)
		os.Exit(1)
	}

	data := buf[:n]

	fmt.Printf("Successfully read %d bytes.\n", n)
	fmt.Println("\n--- Hex Dump ---")
	fmt.Print(hex.Dump(data))

	if n >= HeaderSize {
		fmt.Println("--- TurnstoneDB Header Interpretation (First 20 bytes) ---")
		keyLen := binary.BigEndian.Uint32(data[0:4])
		valLen := binary.BigEndian.Uint32(data[4:8])
		minReadVer := binary.BigEndian.Uint64(data[8:16])
		crc := binary.BigEndian.Uint32(data[16:20])

		fmt.Printf("Key Length:       %d\n", keyLen)
		fmt.Printf("Value Length:     %d\n", valLen)
		fmt.Printf("Min Read Version: %d\n", minReadVer)
		fmt.Printf("Stored CRC:       %x\n", crc)

		var payloadLen int64
		isTombstone := valLen == ^uint32(0)

		if isTombstone {
			fmt.Println("Type:             DELETE (Tombstone)")
			payloadLen = int64(keyLen)
		} else {
			fmt.Println("Type:             SET")
			payloadLen = int64(keyLen) + int64(valLen)
		}

		totalEntrySize := int64(HeaderSize) + payloadLen
		fmt.Printf("Total Entry Size: %d bytes (Header + Payload)\n", totalEntrySize)

		fmt.Println("\n--- Decoded Data ---")

		if int64(n) >= int64(HeaderSize)+int64(keyLen) {
			key := data[HeaderSize : HeaderSize+int(keyLen)]
			fmt.Printf("Key:              %q\n", key)
		} else {
			fmt.Printf("Key:              [Partial/Missing] (Need %d bytes total)\n", int64(HeaderSize)+int64(keyLen))
		}

		if !isTombstone {
			valStart := int64(HeaderSize) + int64(keyLen)
			valEnd := valStart + int64(valLen)

			if int64(n) >= valEnd {
				val := data[valStart:valEnd]
				fmt.Printf("Value:            %q\n", val)
			} else {
				fmt.Printf("Value:            [Partial/Missing] (Need %d bytes total)\n", valEnd)
			}
		}

		if int64(n) < totalEntrySize {
			fmt.Printf("\n[WARNING] Read length (%d) is smaller than required entry size (%d).\n", n, totalEntrySize)
		}
	} else {
		fmt.Println("\n[WARNING] Not enough data read to decode a full header (need 20 bytes).")
	}
}

// --- Verification Handler ---

func verifyIntegrity(journalPath string) {
	idxPath := strings.TrimSuffix(journalPath, ".db") + ".idx"
	if journalPath == idxPath {
		// User passed .idx, try to find .db
		idxPath = journalPath
		journalPath = strings.TrimSuffix(idxPath, ".idx") + ".db"
	}

	fmt.Printf("Verifying Journal: %s\n", journalPath)
	fmt.Printf("Against Index:   %s\n", idxPath)

	f, err := os.Open(journalPath)
	if err != nil {
		fmt.Printf("Error opening journal: %v\n", err)
		return
	}
	defer f.Close()

	db, err := openBolt(idxPath)
	if err != nil {
		fmt.Printf("Error opening BoltDB: %v\n", err)
		return
	}
	defer db.Close()

	var offset int64 = 0
	header := make([]byte, HeaderSize)
	mismatchCount := 0

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("pair"))
		if b == nil {
			return fmt.Errorf("bucket 'pair' not found")
		}

		for {
			// Read Journal Header
			_, err := f.Seek(offset, 0)
			if err != nil {
				return err
			}
			_, err = io.ReadFull(f, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("journal read error at %d: %v", offset, err)
			}

			keyLen := binary.BigEndian.Uint32(header[0:4])
			valLen := binary.BigEndian.Uint32(header[4:8])

			var payloadLen int64
			if valLen == ^uint32(0) {
				payloadLen = int64(keyLen)
			} else {
				payloadLen = int64(keyLen) + int64(valLen)
			}

			actualSize := int64(HeaderSize) + payloadLen

			// Check Bolt
			var kBuf [8]byte
			binary.BigEndian.PutUint64(kBuf[:], uint64(offset))
			v := b.Get(kBuf[:])

			if v == nil {
				fmt.Printf("[MISSING] Offset %d exists in Journal (Size %d) but not in Index\n", offset, actualSize)
				mismatchCount++
			} else {
				storedSize := int64(binary.BigEndian.Uint64(v))
				if storedSize != actualSize {
					fmt.Printf("[MISMATCH] Offset %d: Journal=%d, Index=%d (Diff: %d)\n", offset, actualSize, storedSize, actualSize-storedSize)
					mismatchCount++
				}
			}

			offset += actualSize
		}
		return nil
	})

	if err != nil {
		fmt.Printf("Verification failed: %v\n", err)
	} else {
		fmt.Printf("Verification complete. Found %d mismatches.\n", mismatchCount)
	}
}

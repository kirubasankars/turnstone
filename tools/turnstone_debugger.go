package main

import (
	"bytes"
	"encoding/base64"
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

// --- Colors & Styling ---
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorCyan   = "\033[36m"
	ColorGray   = "\033[90m"
)

func printHeader(title string) {
	fmt.Printf("\n%s=== %s ===%s\n", ColorCyan, title, ColorReset)
}

func printError(format string, args ...interface{}) {
	fmt.Printf("%s[ERROR] %s%s\n", ColorRed, fmt.Sprintf(format, args...), ColorReset)
}

func printSuccess(format string, args ...interface{}) {
	fmt.Printf("%s[OK] %s%s\n", ColorGreen, fmt.Sprintf(format, args...), ColorReset)
}

func printInfo(key string, val interface{}) {
	fmt.Printf("%s%-20s%s : %v\n", ColorBlue, key, ColorReset, val)
}

func formatSize(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// --- Types for Index Decoding ---

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
	mode := flag.String("mode", "", "Mode: summary, journal, bolt-pair, bolt-index, bolt-meta, verify (default: auto-detect)")

	offset := flag.Int64("offset", -1, "Int64 Offset (for Journal seek or Pair lookup)")
	key := flag.String("key", "", "String Key (for Index/Meta lookup)")
	limit := flag.Int("limit", 0, "Read size (bytes) or Entry count (rows)")
	asText := flag.Bool("text", false, "Print values as text instead of Base64 (Journal mode)")

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
			if *offset >= 0 {
				selectedMode = "journal"
			} else {
				selectedMode = "verify" // Default to verify/summary for .db if no offset given
			}
		} else if strings.HasSuffix(*path, ".idx") {
			if *key != "" {
				selectedMode = "bolt-index"
			} else {
				selectedMode = "summary"
			}
		} else {
			printError("Cannot infer mode from file extension. Please use -mode.")
			os.Exit(1)
		}
	}

	// 3. Dispatch
	switch selectedMode {
	case "summary":
		showSummary(*path)
	case "journal":
		startOff := *offset
		if startOff < 0 {
			startOff = 0
		}
		debugJournal(*path, startOff, *limit, *asText)
	case "bolt-pair":
		debugBoltPair(*path, *offset, *limit)
	case "bolt-index":
		debugBoltIndex(*path, *key, *limit)
	case "bolt-meta":
		debugBoltMeta(*path, *key)
	case "verify":
		verifyIntegrity(*path)
	default:
		printError("Unknown mode '%s'", selectedMode)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(ColorCyan + "TurnstoneDB Debugger & Inspector" + ColorReset)
	fmt.Println("Usage: go run debugger.go -path <file> [flags]")
	fmt.Println("\n" + ColorYellow + "Flags:" + ColorReset)
	flag.PrintDefaults()
	fmt.Println("\n" + ColorYellow + "Modes:" + ColorReset)
	fmt.Println("  summary     : Scan index/journal and show stats (Counts, Size, Fragmentation)")
	fmt.Println("  journal     : Read raw entry from journal at -offset")
	fmt.Println("  bolt-index  : Inspect Key History in Index (use -key)")
	fmt.Println("  bolt-pair   : Inspect Offset->Size map in Index (use -offset)")
	fmt.Println("  verify      : Full integrity check between Journal and Index")
	fmt.Println("\n" + ColorYellow + "Examples:" + ColorReset)
	fmt.Println("  Quick Stats:    go run debugger.go -path data/1.idx")
	fmt.Println("  Read Entry:     go run debugger.go -path data/1.db -offset 12345")
	fmt.Println("  Find Key:       go run debugger.go -path data/1.idx -mode bolt-index -key myUser")
	fmt.Println("  Verify Integrity: go run debugger.go -path data/1.db -mode verify")
}

// --- High Level Summaries ---

func showSummary(path string) {
	// If pointing to DB, map to IDX
	idxPath := path
	if strings.HasSuffix(path, ".db") {
		idxPath = strings.TrimSuffix(path, ".db") + ".idx"
	}

	printHeader("Database Summary")
	printInfo("Target File", idxPath)

	db, err := openBolt(idxPath)
	if err != nil {
		printError("Could not open index: %v", err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		// 1. Meta
		meta := tx.Bucket([]byte("meta"))
		if meta != nil {
			fmt.Println("\n" + ColorYellow + "--- Metadata ---" + ColorReset)
			meta.ForEach(func(k, v []byte) error {
				valDisplay := ""
				if len(v) == 8 {
					valDisplay = fmt.Sprintf("%d", binary.BigEndian.Uint64(v))
				} else {
					valDisplay = string(v)
				}
				fmt.Printf("  %-15s : %s\n", string(k), valDisplay)
				return nil
			})
		}

		// 2. Index Stats
		idx := tx.Bucket([]byte("index"))
		if idx != nil {
			fmt.Println("\n" + ColorYellow + "--- Index Statistics ---" + ColorReset)
			stats := idx.Stats()
			printInfo("Total Keys", stats.KeyN)
			printInfo("B-Tree Depth", stats.Depth)
			printInfo("Branch Pages", stats.BranchPageN)
			printInfo("Leaf Pages", stats.LeafPageN)

			// Sample count of deleted keys
			deletedCount := 0
			activeCount := 0
			c := idx.Cursor()
			limit := 1000 // Sample size
			i := 0
			for k, v := c.First(); k != nil && i < limit; k, v = c.Next() {
				hist := decodeHistory(v)
				if len(hist) > 0 && hist[len(hist)-1].Deleted() {
					deletedCount++
				} else {
					activeCount++
				}
				i++
			}
			if i > 0 {
				delRate := (float64(deletedCount) / float64(i)) * 100
				printInfo("Est. Delete Rate", fmt.Sprintf("%.1f%% (based on first %d keys)", delRate, i))
			}
		}

		// 3. Physical Size
		fi, err := os.Stat(strings.TrimSuffix(idxPath, ".idx") + ".db")
		if err == nil {
			printInfo("Journal Size", formatSize(fi.Size()))
		}

		return nil
	})
	if err != nil {
		printError("%v", err)
	}
}

// --- BoltDB Handlers ---

func openBolt(path string) (*bbolt.DB, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	// Use a timeout so we don't hang if the DB is locked by the server
	return bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
}

func debugBoltPair(path string, lookupOffset int64, limit int) {
	printHeader("BoltDB: Offset -> Size Map")
	db, err := openBolt(path)
	if err != nil {
		printError("Opening BoltDB: %v", err)
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
			printInfo("Lookup Offset", lookupOffset)
			var kBuf [8]byte
			binary.BigEndian.PutUint64(kBuf[:], uint64(lookupOffset))
			v := b.Get(kBuf[:])
			if v == nil {
				fmt.Printf("%s-> Not Found%s\n", ColorRed, ColorReset)
			} else {
				lenVal := binary.BigEndian.Uint64(v)
				fmt.Printf("%s-> Found: Length = %d bytes%s\n", ColorGreen, lenVal, ColorReset)
			}
			return nil
		}

		// Scan
		fmt.Println(ColorGray + "Scanning 'pair' bucket..." + ColorReset)
		c := b.Cursor()
		count := 0
		max := limit
		if max <= 0 {
			max = 50
		}

		fmt.Printf("%-15s | %-15s\n", "Offset", "Length")
		fmt.Println(strings.Repeat("-", 35))

		for k, v := c.First(); k != nil; k, v = c.Next() {
			off := binary.BigEndian.Uint64(k)
			lenVal := binary.BigEndian.Uint64(v)
			fmt.Printf("%-15d | %d\n", off, lenVal)
			count++
			if count >= max {
				fmt.Println(ColorYellow + "... (limit reached)" + ColorReset)
				break
			}
		}
		return nil
	})
	if err != nil {
		printError("%v", err)
	}
}

func debugBoltIndex(path string, lookupKey string, limit int) {
	printHeader("BoltDB: Key Index")
	db, err := openBolt(path)
	if err != nil {
		printError("Opening BoltDB: %v", err)
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
			printInfo("Lookup Key", lookupKey)
			v := b.Get([]byte(lookupKey))
			if v == nil {
				fmt.Printf("%s-> Not Found%s\n", ColorRed, ColorReset)
			} else {
				hist := decodeHistory(v)
				fmt.Printf("%s-> Found %d versions:%s\n", ColorGreen, len(hist), ColorReset)
				for i, h := range hist {
					status := ColorGreen + "SET" + ColorReset
					if h.Deleted() {
						status = ColorRed + "DEL" + ColorReset
					}
					prefix := "  "
					if i == len(hist)-1 {
						prefix = "->" // Arrow pointing to latest
					}
					fmt.Printf("%s %s @ Offset %d\n", prefix, status, h.Offset())
				}
			}
			return nil
		}

		// Scan
		fmt.Println(ColorGray + "Scanning 'index' bucket..." + ColorReset)
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
					latest = fmt.Sprintf("%sDEL%s @ %d", ColorRed, ColorReset, last.Offset())
				} else {
					latest = fmt.Sprintf("%sSET%s @ %d", ColorGreen, ColorReset, last.Offset())
				}
			}
			fmt.Printf("%-20q : %s (v%d)\n", k, latest, len(hist))
			count++
			if count >= max {
				fmt.Println(ColorYellow + "... (limit reached)" + ColorReset)
				break
			}
		}
		return nil
	})
	if err != nil {
		printError("%v", err)
	}
}

func debugBoltMeta(path string, lookupKey string) {
	printHeader("BoltDB: Metadata")
	db, err := openBolt(path)
	if err != nil {
		printError("Opening BoltDB: %v", err)
		return
	}
	defer db.Close()

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		if b == nil {
			return fmt.Errorf("bucket 'meta' not found")
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Filter if specific key requested
			if lookupKey != "" && string(k) != lookupKey {
				continue
			}
			valDisplay := ""
			if len(v) == 8 {
				valDisplay = fmt.Sprintf("%d (int64)", binary.BigEndian.Uint64(v))
			} else {
				valDisplay = string(v)
			}
			fmt.Printf("%-15s : %s\n", k, valDisplay)
		}
		return nil
	})
	if err != nil {
		printError("%v", err)
	}
}

// --- Journal Handler ---

func isPrintable(data []byte) bool {
	for _, b := range data {
		if b < 32 || b > 126 {
			// Allow common whitespace
			if b != '\n' && b != '\r' && b != '\t' {
				return false
			}
		}
	}
	return true
}

func debugJournal(filePath string, offset int64, length int, asText bool) {
	printHeader("Journal Inspection")

	f, err := os.Open(filePath)
	if err != nil {
		printError("Opening file: %v", err)
		os.Exit(1)
	}
	defer f.Close()

	info, _ := f.Stat()
	fileSize := info.Size()

	if offset >= fileSize {
		printError("Offset %d is beyond file size %s", offset, formatSize(fileSize))
		os.Exit(1)
	}

	printInfo("File", filePath)
	printInfo("Total Size", formatSize(fileSize))
	printInfo("Seek Offset", offset)

	_, err = f.Seek(offset, 0)
	if err != nil {
		printError("Seek failed: %v", err)
		os.Exit(1)
	}

	// Default to reading a reasonable chunk to find a header + payload
	readSize := length
	if readSize <= 0 {
		readSize = 1024 // Default peek size
	}
	if int64(readSize) > fileSize-offset {
		readSize = int(fileSize - offset)
	}

	buf := make([]byte, readSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		printError("Read failed: %v", err)
		os.Exit(1)
	}

	data := buf[:n]

	if n >= HeaderSize {
		fmt.Println("\n" + ColorYellow + "--- Entry Header ---" + ColorReset)
		keyLen := binary.BigEndian.Uint32(data[0:4])
		valLen := binary.BigEndian.Uint32(data[4:8])
		minReadVer := binary.BigEndian.Uint64(data[8:16])
		crc := binary.BigEndian.Uint32(data[16:20])

		printInfo("Key Length", keyLen)
		if valLen == ^uint32(0) {
			printInfo("Type", ColorRed+"DELETE (Tombstone)"+ColorReset)
		} else {
			printInfo("Type", ColorGreen+"SET"+ColorReset)
			printInfo("Value Length", valLen)
		}
		printInfo("Min Read Ver", minReadVer)
		printInfo("CRC32", fmt.Sprintf("0x%x", crc))

		// Try to display Key
		if int64(n) >= int64(HeaderSize)+int64(keyLen) {
			key := data[HeaderSize : HeaderSize+int(keyLen)]
			fmt.Printf("%sKey:%s %q\n", ColorBlue, ColorReset, key)

			// Try to display Value
			if valLen != ^uint32(0) {
				valStart := int64(HeaderSize) + int64(keyLen)
				valEnd := valStart + int64(valLen)
				if int64(n) >= valEnd {
					val := data[valStart:valEnd]
					fmt.Printf("%sValue:%s ", ColorBlue, ColorReset)

					if asText {
						if isPrintable(val) {
							fmt.Printf("%s\n", string(val))
						} else {
							fmt.Println("(Binary Data - see hex dump)")
							fmt.Println(hex.Dump(val))
						}
					} else {
						fmt.Printf("%s (Base64)\n", base64.StdEncoding.EncodeToString(val))
					}
				} else {
					fmt.Printf("%sValue:%s [Truncated in view, use -limit %d to see full]\n", ColorBlue, ColorReset, valEnd)
				}
			}
		} else {
			fmt.Println(ColorYellow + "[Incomplete Data for Payload]" + ColorReset)
		}

	} else {
		printError("Not enough data for header (need 20 bytes, got %d)", n)
	}

	fmt.Println("\n" + ColorYellow + "--- Raw Hex Dump ---" + ColorReset)
	fmt.Print(hex.Dump(data))
}

// --- Verification Handler ---

func verifyIntegrity(journalPath string) {
	idxPath := strings.TrimSuffix(journalPath, ".db") + ".idx"
	if journalPath == idxPath {
		idxPath = journalPath
		journalPath = strings.TrimSuffix(idxPath, ".idx") + ".db"
	}

	printHeader("Integrity Verification")
	printInfo("Journal", journalPath)
	printInfo("Index", idxPath)

	f, err := os.Open(journalPath)
	if err != nil {
		printError("Opening journal: %v", err)
		return
	}
	defer f.Close()

	db, err := openBolt(idxPath)
	if err != nil {
		printError("Opening BoltDB: %v", err)
		return
	}
	defer db.Close()

	var offset int64 = 0
	header := make([]byte, HeaderSize)
	mismatchCount := 0
	entryCount := 0
	lastReport := time.Now()

	fmt.Println("\n" + ColorGray + "Starting scan..." + ColorReset)

	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("pair"))
		if b == nil {
			return fmt.Errorf("bucket 'pair' not found")
		}

		for {
			// Progress Report every 2 seconds
			if time.Since(lastReport) > 2*time.Second {
				fmt.Printf("\rScanned %d entries | Offset: %s...", entryCount, formatSize(offset))
				lastReport = time.Now()
			}

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
				fmt.Printf("\n%s[MISSING]%s Offset %d exists in Journal (Size %d) but not in Index\n", ColorRed, ColorReset, offset, actualSize)
				mismatchCount++
			} else {
				storedSize := int64(binary.BigEndian.Uint64(v))
				if storedSize != actualSize {
					fmt.Printf("\n%s[MISMATCH]%s Offset %d: Journal=%d, Index=%d (Diff: %d)\n", ColorRed, ColorReset, offset, actualSize, storedSize, actualSize-storedSize)
					mismatchCount++
				}
			}

			entryCount++
			offset += actualSize
		}
		return nil
	})

	fmt.Println() // Clear progress line
	if err != nil {
		printError("Verification failed: %v", err)
	} else {
		if mismatchCount == 0 {
			printSuccess("Verification complete. Scanned %d entries. No errors found.", entryCount)
		} else {
			printError("Verification complete. Found %d mismatches.", mismatchCount)
		}
	}
}

package main

import (
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	HeaderSize = 24
)

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	// Shift args for subcommands
	os.Args = append(os.Args[:1], os.Args[2:]...)

	switch cmd {
	case "get":
		runGet()
	case "scan":
		runScan()
	case "scan-log":
		runScanLog()
	case "inspect":
		runInspect()
	case "dump-csv":
		runDumpCSV()
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: turnstone_debugger <command> [arguments]")
	fmt.Println("Commands:")
	fmt.Println("  get       Lookup a key by offset from LevelDB, or scan values log if missing")
	fmt.Println("            Usage: get -db <index_dir> -log <values.log> -key <key>")
	fmt.Println("  scan      Iterate over keys in the LevelDB index")
	fmt.Println("            Usage: scan -db <index_dir> [-start <key>] [-prefix <prefix>] [-limit <n>]")
	fmt.Println("  scan-log  Iterate over all entries in the values log sequentially")
	fmt.Println("            Usage: scan-log -log <values.log> [-verify] [-verbose]")
	fmt.Println("  inspect   Read a raw entry from the log at a specific offset")
	fmt.Println("            Usage: inspect -log <values.log> -offset <int>")
	fmt.Println("  dump-csv  Dump the index and log to CSV")
	fmt.Println("            Usage: dump-csv -db <index_dir> -log <values.log>")
}

// UnpackMeta unpacks the 32-bit metadata field.
// Layout assumption: [1 bit IsDel] [15 bits KeyLen] [16 bits ValLen]
func UnpackMeta(packed uint32) (uint16, uint32, bool) {
	isDel := (packed & (1 << 31)) != 0
	keyLen := uint16((packed >> 16) & 0x7FFF)
	valLen := packed & 0xFFFF
	return keyLen, uint32(valLen), isDel
}

// runGet implements the "get" command logic
func runGet() {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	idxPath := fs.String("db", "", "Path to LevelDB index directory")
	journalPath := fs.String("log", "", "Path to values log (journal) file")
	key := fs.String("key", "", "Key to lookup")
	fs.Parse(os.Args[1:])

	if *journalPath == "" || *key == "" {
		fmt.Println("Error: -log and -key are required")
		fs.Usage()
		return
	}

	foundInIndex := false
	var offset int64

	// 1. Try LevelDB Lookup
	if *idxPath != "" {
		db, err := leveldb.OpenFile(*idxPath, nil)
		if err == nil {
			defer db.Close()
			val, err := db.Get([]byte(*key), nil)
			if err == nil && len(val) >= 8 {
				offset = int64(binary.BigEndian.Uint64(val))
				fmt.Printf("[Index] Found key '%s' at offset %d. Fetching...\n", *key, offset)
				foundInIndex = true
			} else {
				fmt.Printf("[Index] Key '%s' not found in LevelDB.\n", *key)
			}
		} else {
			fmt.Printf("[Index] Could not open LevelDB at '%s': %v\n", *idxPath, err)
		}
	} else {
		fmt.Println("[Index] No LevelDB path provided (-db). Skipping index lookup.")
	}

	// 2. If found in index, read directly
	if foundInIndex {
		readJournalEntry(*journalPath, offset)
		return
	}

	// 3. Fallback: Scan Journal for latest value
	fmt.Println("[Fallback] Scanning entire journal for latest value...")
	scanOffset, err := scanJournalForKey(*journalPath, *key)
	if err != nil {
		fmt.Printf("[Fallback] Scan failed: %v\n", err)
		return
	}

	if scanOffset != -1 {
		fmt.Printf("[Fallback] Found latest entry at offset %d\n", scanOffset)
		readJournalEntry(*journalPath, scanOffset)
	} else {
		fmt.Println("[Fallback] Key not found in journal.")
	}
}

// runScan implements the "scan" command logic (Index Scan)
func runScan() {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	idxPath := fs.String("db", "", "Path to LevelDB index directory")
	startKey := fs.String("start", "", "Start key for iteration")
	prefix := fs.String("prefix", "", "Prefix filter for keys")
	limit := fs.Int("limit", 100, "Maximum number of keys to list")
	fs.Parse(os.Args[1:])

	if *idxPath == "" {
		fmt.Println("Error: -db is required")
		fs.Usage()
		return
	}

	db, err := leveldb.OpenFile(*idxPath, nil)
	if err != nil {
		fmt.Printf("Error opening LevelDB: %v\n", err)
		return
	}
	defer db.Close()

	var iterType util.Range
	if *prefix != "" {
		iterType = util.Range{Start: []byte(*prefix), Limit: util.BytesPrefix([]byte(*prefix)).Limit}
	}

	iter := db.NewIterator(&iterType, nil)
	defer iter.Release()

	if *startKey != "" && *prefix == "" {
		if !iter.Seek([]byte(*startKey)) {
			fmt.Println("Start key not found or passed end of DB.")
			return
		}
	} else if *prefix == "" {
		iter.First()
	} else {
		// prefix logic handled by NewIterator slice range, ensure we start at beginning of that range
		if !iter.First() && *prefix != "" {
			fmt.Printf("No keys found with prefix '%s'\n", *prefix)
			return
		}
	}

	count := 0

	// Use tabwriter for cleaner output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "KEY\tLOG OFFSET")
	fmt.Fprintln(w, "--------------------\t----------")

	for iter.Valid() {
		if count >= *limit {
			break
		}

		key := string(iter.Key())
		val := iter.Value()
		var offsetDisplay string

		if len(val) >= 8 {
			offset := int64(binary.BigEndian.Uint64(val))
			offsetDisplay = fmt.Sprintf("%d", offset)
		} else {
			offsetDisplay = fmt.Sprintf("Invalid (%d bytes)", len(val))
		}

		// Use %q for keys to handle special chars safely
		fmt.Fprintf(w, "%q\t%s\n", key, offsetDisplay)

		iter.Next()
		count++
	}
	w.Flush()

	if err := iter.Error(); err != nil {
		fmt.Printf("Iterator error: %v\n", err)
	}
}

// runScanLog implements the "scan-log" command logic (Journal Scan)
func runScanLog() {
	fs := flag.NewFlagSet("scan-log", flag.ExitOnError)
	journalPath := fs.String("log", "", "Path to values log (journal) file")
	verify := fs.Bool("verify", false, "Verify CRC of every entry (reads full payload)")
	verbose := fs.Bool("verbose", false, "Show value previews")
	fs.Parse(os.Args[1:])

	if *journalPath == "" {
		fmt.Println("Error: -log is required")
		fs.Usage()
		return
	}

	f, err := os.Open(*journalPath)
	if err != nil {
		fmt.Printf("Error opening journal: %v\n", err)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		fmt.Printf("Error stating journal: %v\n", err)
		return
	}
	fileSize := info.Size()
	var offset int64 = 0

	fmt.Printf("Scanning Journal: %s (Size: %d bytes)\n", *journalPath, fileSize)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)

	headerCols := []string{"OFFSET", "TYPE", "KEY", "KEY LEN", "VAL LEN", "CRC", "STATUS"}
	if *verbose {
		headerCols = append(headerCols, "VAL PREVIEW")
	}

	// Create a separator line matching header widths or min content widths
	var separators []string
	for _, col := range headerCols {
		width := len(col)
		// Adjust separator width for columns with potentially long/fixed content
		switch col {
		case "KEY":
			width = 20
		case "CRC":
			width = 10
		case "STATUS":
			width = 9
		}
		separators = append(separators, strings.Repeat("-", width))
	}

	fmt.Fprintln(w, strings.Join(headerCols, "\t"))
	fmt.Fprintln(w, strings.Join(separators, "\t"))

	header := make([]byte, HeaderSize)

	for offset < fileSize {
		// Read Header
		if _, err := f.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading header at offset %d: %v\n", offset, err)
			return
		}

		packed := binary.BigEndian.Uint32(header[0:4])
		keyLen, valLen, isDel := UnpackMeta(packed)
		storedCrc := binary.BigEndian.Uint32(header[20:24])

		typeStr := "PUT"
		if isDel {
			typeStr = "DEL"
		}

		// Calculate total entry length
		var payloadLen int64
		if isDel {
			payloadLen = int64(keyLen)
		} else {
			payloadLen = int64(keyLen) + int64(valLen)
		}

		totalLen := int64(HeaderSize) + payloadLen

		// Check for truncation immediately
		isTruncated := false
		if offset+totalLen > fileSize {
			isTruncated = true
		}

		// Attempt to read key for display
		var keyStr string

		// Safe bounds for key reading
		keyReadSize := int64(keyLen)
		if offset+int64(HeaderSize)+keyReadSize > fileSize {
			keyReadSize = fileSize - (offset + int64(HeaderSize))
		}

		if keyReadSize <= 0 {
			keyStr = ""
		} else {
			keyBuf := make([]byte, keyReadSize)
			if _, err := f.ReadAt(keyBuf, offset+int64(HeaderSize)); err != nil {
				keyStr = "<READ ERR>"
			} else {
				// Use quoted string to handle binary/special chars safely
				keyStr = fmt.Sprintf("%q", string(keyBuf))
				if len(keyStr) > 20 {
					keyStr = keyStr[:17] + "..."
				}
			}
		}

		status := "SKIP"
		if isTruncated {
			status = "TRUNCATED"
		}

		var valPreview string = ""

		if !isTruncated && (*verify || *verbose) {
			payload := make([]byte, int(payloadLen))
			if _, err := f.ReadAt(payload, offset+HeaderSize); err != nil {
				status = "READ_ERR"
			} else {
				if *verify {
					digest := crc32.New(crc32Table)
					digest.Write(header[0:20])
					digest.Write(payload)
					if digest.Sum32() == storedCrc {
						status = "OK"
					} else {
						status = "CRC_FAIL"
					}
				}

				if *verbose && !isDel && int64(len(payload)) >= int64(keyLen) {
					valBytes := payload[keyLen:]
					if len(valBytes) > 20 {
						valPreview = hex.EncodeToString(valBytes[:8]) + "..."
					} else {
						valPreview = hex.EncodeToString(valBytes)
					}
				}
			}
		}

		line := fmt.Sprintf("%d\t%s\t%s\t%d\t%d\t0x%08x\t%s",
			offset, typeStr, keyStr, keyLen, valLen, storedCrc, status)

		if *verbose {
			line += fmt.Sprintf("\t%s", valPreview)
		}

		fmt.Fprintln(w, line)

		if isTruncated {
			w.Flush()
			fmt.Printf("... Log truncated at offset %d (Expected end: %d, File size: %d)\n", offset, offset+totalLen, fileSize)
			break
		}

		offset += totalLen
	}
	w.Flush()
}

// scanJournalForKey reads the log sequentially to find the last occurrence of key
func scanJournalForKey(journalPath string, targetKey string) (int64, error) {
	f, err := os.Open(journalPath)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return -1, err
	}
	fileSize := info.Size()
	var offset int64 = 0
	var lastFoundOffset int64 = -1

	header := make([]byte, HeaderSize)

	for offset < fileSize {
		// Read Header
		if _, err := f.ReadAt(header, offset); err != nil {
			if err == io.EOF {
				break
			}
			return -1, fmt.Errorf("read header at %d: %v", offset, err)
		}

		packed := binary.BigEndian.Uint32(header[0:4])
		keyLen, valLen, _ := UnpackMeta(packed)

		totalLen := int64(HeaderSize) + int64(keyLen) + int64(valLen)

		// Check bounds
		if offset+totalLen > fileSize {
			break // Incomplete entry at end of file
		}

		// Read Key
		keyBuf := make([]byte, keyLen)
		if _, err := f.ReadAt(keyBuf, offset+int64(HeaderSize)); err != nil {
			return -1, fmt.Errorf("read key at %d: %v", offset, err)
		}

		if string(keyBuf) == targetKey {
			lastFoundOffset = offset
			// We don't break here because we want the *latest* (last) entry in the log
		}

		offset += totalLen
	}

	return lastFoundOffset, nil
}

func runInspect() {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	journalPath := fs.String("log", "", "Path to values log (journal) file")
	offset := fs.Int64("offset", -1, "Offset to inspect")
	fs.Parse(os.Args[1:])

	if *journalPath == "" || *offset < 0 {
		fmt.Println("Error: -log and valid -offset are required")
		fs.Usage()
		return
	}

	readJournalEntry(*journalPath, *offset)
}

func runDumpCSV() {
	fs := flag.NewFlagSet("dump-csv", flag.ExitOnError)
	idxPath := fs.String("db", "", "Path to LevelDB index directory")
	journalPath := fs.String("log", "", "Path to values log (journal) file")
	fs.Parse(os.Args[1:])

	if *idxPath == "" || *journalPath == "" {
		fmt.Println("Error: -db and -log are required")
		fs.Usage()
		return
	}

	dumpCSV(*idxPath, *journalPath)
}

func readJournalEntry(journalPath string, offset int64) {
	f, err := os.Open(journalPath)
	if err != nil {
		fmt.Printf("Error opening journal: %v\n", err)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err == nil && offset >= info.Size() {
		fmt.Printf("Error: Offset %d is beyond file size %d\n", offset, info.Size())
		return
	}

	header := make([]byte, HeaderSize)
	if _, err := f.ReadAt(header, offset); err != nil {
		fmt.Printf("Error reading header: %v\n", err)
		return
	}

	packed := binary.BigEndian.Uint32(header[0:4])
	keyLen, valLen, isDel := UnpackMeta(packed)
	lsn := binary.BigEndian.Uint64(header[4:12])
	logID := binary.BigEndian.Uint64(header[12:20])
	storedCrc := binary.BigEndian.Uint32(header[20:24])

	var payloadLen int64
	if isDel {
		payloadLen = int64(keyLen)
	} else {
		payloadLen = int64(keyLen) + int64(valLen)
	}

	fmt.Printf("RAW WAL ENTRY @ %d\n", offset)
	fmt.Printf("  LSN: %d  LogID: %d  Deleted: %v\n", lsn, logID, isDel)
	fmt.Printf("  KeyLen: %d  ValLen: %d  TotalLen: %d\n", keyLen, valLen, int64(HeaderSize)+payloadLen)
	fmt.Printf("  HeaderCRC: 0x%08x\n", storedCrc)

	payload := make([]byte, int(payloadLen))
	if _, err := f.ReadAt(payload, offset+HeaderSize); err != nil {
		fmt.Printf("  Error reading payload: %v\n", err)
		return
	}

	// Verify CRC
	digest := crc32.New(crc32Table)
	digest.Write(header[0:20])
	digest.Write(payload)
	if digest.Sum32() == storedCrc {
		fmt.Println("  CRC Check: VALID")
	} else {
		fmt.Printf("  CRC Check: INVALID (Calc 0x%08x)\n", digest.Sum32())
	}

	if int64(len(payload)) >= int64(keyLen) {
		fmt.Printf("  Key: %q\n", string(payload[:int(keyLen)]))
	}

	if !isDel && int64(len(payload)) >= int64(keyLen)+int64(valLen) {
		val := payload[int(keyLen):]
		fmt.Printf("  Value: %s\n", hex.EncodeToString(val))
		if len(val) < 100 {
			fmt.Printf("  Value (Text): %q\n", string(val))
		}
	}
}

func dumpCSV(idxPath, journalPath string) {
	db, err := leveldb.OpenFile(idxPath, nil)
	if err != nil {
		fmt.Printf("Error opening leveldb: %v\n", err)
		return
	}
	defer db.Close()

	fmt.Println("Key,Offset,ValLen")
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		val := iter.Value()

		if len(val) < 8 {
			continue
		}

		offset := int64(binary.BigEndian.Uint64(val))

		// Optional: We could peek at the journal to get the value length,
		// but for CSV dump we might just dump the index data.
		// To be safe, we just print what we have in the index.
		fmt.Printf("%s,%d,?\n", key, offset)
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		fmt.Printf("Iterator error: %v\n", err)
	}
}

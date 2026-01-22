package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"turnstone/protocol"
)

func FuzzKV(f *testing.F) {
	// 1. Add seed corpus (valid scenarios to start mutating from)
	f.Add([]byte("key1val1"))
	f.Add([]byte{0x00, 0x04, 'u', 's', 'e', 'r', 0x00, 0x05, 'a', 'l', 'i', 'c', 'e'})

	f.Fuzz(func(t *testing.T, data []byte) {
		// 2. Setup DB (Fresh for each iteration)
		db, _ := setupDB(t)
		defer db.Close()

		// 3. Setup Verification Model (Source of Truth)
		model := make(map[string][]byte)

		// 4. Interpret 'data' as a script of operations
		reader := bytes.NewReader(data)
		for reader.Len() > 0 {
			opByte, err := reader.ReadByte()
			if err != nil {
				break
			}

			// Modulo 3 to pick operation: 0=Put, 1=Get, 2=Delete
			op := opByte % 3

			switch op {
			case 0: // PUT
				key, val, err := readKeyVal(reader)
				if err != nil {
					return // Not enough data, stop this run
				}

				// Execute on DB
				tx := db.BeginTx()
				tx.Put(key, val)
				if err := tx.Commit(); err != nil {
					t.Fatalf("Put commit failed: %v", err)
				}

				// Update Model
				model[key] = val

			case 1: // GET
				key, err := readKey(reader)
				if err != nil {
					return
				}

				// Execute on DB
				// Read at CurrentTxID to ensure we see the latest committed writes
				gotVal, err := db.Get(key, db.CurrentTxID())

				// Verify against Model
				wantVal, exists := model[key]

				if exists {
					if err != nil {
						t.Fatalf("Get('%s') failed: %v. Expected value exists in model.", key, err)
					}
					if !bytes.Equal(gotVal, wantVal) {
						t.Fatalf("Get('%s') mismatch.\nWant: %x\nGot : %x", key, wantVal, gotVal)
					}
				} else {
					if err != protocol.ErrKeyNotFound {
						t.Fatalf("Get('%s') unexpected success. Expected ErrKeyNotFound, got err=%v val=%x", key, err, gotVal)
					}
				}

			case 2: // DELETE
				key, err := readKey(reader)
				if err != nil {
					return
				}

				// Execute on DB
				tx := db.BeginTx()
				tx.Delete(key)
				if err := tx.Commit(); err != nil {
					t.Fatalf("Delete commit failed: %v", err)
				}

				// Update Model
				delete(model, key)
			}
		}
	})
}

// --- Helpers to parse random bytes into Keys/Values ---

func readKey(r *bytes.Reader) (string, error) {
	// Read 1 byte for length (Keys 0-255 bytes)
	lenByte, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	kLen := int(lenByte)

	if r.Len() < kLen {
		return "", fmt.Errorf("EOF")
	}

	buf := make([]byte, kLen)
	r.Read(buf)
	return string(buf), nil
}

func readKeyVal(r *bytes.Reader) (string, []byte, error) {
	key, err := readKey(r)
	if err != nil {
		return "", nil, err
	}

	// Value length: Read 2 bytes (Values 0-65535 bytes)
	if r.Len() < 2 {
		return "", nil, fmt.Errorf("EOF")
	}
	var vLen uint16
	binary.Read(r, binary.BigEndian, &vLen)

	if r.Len() < int(vLen) {
		return "", nil, fmt.Errorf("EOF")
	}

	val := make([]byte, vLen)
	r.Read(val)

	return key, val, nil
}

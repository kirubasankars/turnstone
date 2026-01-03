package engine

import (
	"testing"
)

// setupDB creates a fresh DB in a temporary directory.
// It returns the DB instance and the directory path.
// The directory is automatically cleaned up by the testing framework.
func setupDB(t testing.TB) (*StoneDB, string) {
	t.Helper()
	dir := t.TempDir()
	// Use 0 as default initialTxID for tests
	db, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	return db, dir
}

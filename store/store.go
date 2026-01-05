package store

import (
	"log/slog"
	"time"

	"turnstone/stonedb"
)

// StoreStats holds basic metrics.
type StoreStats struct {
	KeyCount int
	Uptime   string
}

// Store wraps stonedb.DB to provide a compatibility layer and stats tracking.
type Store struct {
	*stonedb.DB
	logger    *slog.Logger
	startTime time.Time
}

// NewStore initializes a persistent StoneDB store in the specified directory.
func NewStore(dir string, logger *slog.Logger) (*Store, error) {
	opts := stonedb.Options{
		// Default options
		MaxWALSize:           10 * 1024 * 1024, // 10MB
		CompactionMinGarbage: 4 * 1024 * 1024,  // 4MB
	}

	db, err := stonedb.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &Store{
		DB:        db,
		logger:    logger,
		startTime: time.Now(),
	}, nil
}

// Close closes the underlying StoneDB instance.
func (s *Store) Close() error {
	s.logger.Info("Closing StoneDB store")
	return s.DB.Close()
}

// Stats returns usage statistics.
// Note: Since StoneDB doesn't expose live key counts publicly, we return a placeholder.
func (s *Store) Stats() StoreStats {
	count, _ := s.DB.KeyCount()
	return StoreStats{
		KeyCount: int(count),
		Uptime:   time.Since(s.startTime).Round(time.Second).String(),
	}
}

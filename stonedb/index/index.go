package index

import (
	"os"

	"turnstone/stonedb/btree"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = btree.ErrNotFound

// WriteOptions controls write durability.
type WriteOptions struct {
	Sync bool
}

// Range restricts iterator traversal.
type Range struct {
	Start []byte
	Limit []byte
}

// BytesPrefix returns a range covering all keys with the given prefix.
func BytesPrefix(prefix []byte) *Range {
	if len(prefix) == 0 {
		return nil
	}
	limit := make([]byte, len(prefix))
	copy(limit, prefix)
	for i := len(limit) - 1; i >= 0; i-- {
		limit[i]++
		if limit[i] != 0 {
			return &Range{Start: prefix, Limit: limit}
		}
	}
	return &Range{Start: prefix, Limit: nil}
}

// DB is a memory-mapped B+ tree index.
type DB struct {
	tree *btree.Tree
}

// Open opens or creates an index at path (directory).
func Open(path string, _ int) (*DB, error) {
	tree, err := btree.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{tree: tree}, nil
}

// Close closes the index.
func (db *DB) Close() error {
	return db.tree.Close()
}

// Get returns the value for key.
func (db *DB) Get(key []byte, _ *WriteOptions) ([]byte, error) {
	return db.tree.Get(key)
}

// GetLocked looks up key while the caller holds an active iterator read lock.
func (db *DB) GetLocked(key []byte) ([]byte, error) {
	return db.tree.GetLocked(key)
}

// Put stores key/value.
func (db *DB) Put(key, value []byte, opts *WriteOptions) error {
	if err := db.tree.Put(key, value); err != nil {
		return err
	}
	if opts != nil && opts.Sync {
		return db.tree.Sync()
	}
	return nil
}

// Batch accumulates writes.
type Batch struct {
	puts    [][2][]byte
	deletes [][]byte
}

// Put adds a put to the batch.
func (b *Batch) Put(key, value []byte) {
	b.puts = append(b.puts, [2][]byte{clone(key), clone(value)})
}

// Delete adds a delete to the batch.
func (b *Batch) Delete(key []byte) {
	b.deletes = append(b.deletes, clone(key))
}

// Len returns the number of operations in the batch.
func (b *Batch) Len() int {
	return len(b.puts) + len(b.deletes)
}

// Reset clears the batch.
func (b *Batch) Reset() {
	b.puts = nil
	b.deletes = nil
}

// Write applies the batch.
func (db *DB) Write(batch *Batch, opts *WriteOptions) error {
	sync := opts != nil && opts.Sync
	return db.tree.ApplyBatch(batch.puts, batch.deletes, sync)
}

// Iterator walks the keyspace.
type Iterator interface {
	Valid() bool
	Next() bool
	Prev() bool
	Seek(key []byte) bool
	Last() bool
	Key() []byte
	Value() []byte
	Release()
	Error() error
}

type iterator struct {
	inner *btree.Iterator
}

// NewIterator creates an iterator over the optional range.
func (db *DB) NewIterator(slice *Range, _ *WriteOptions) Iterator {
	var r *btree.Range
	if slice != nil {
		r = &btree.Range{Start: slice.Start, Limit: slice.Limit}
	}
	return &iterator{inner: db.tree.NewIterator(r)}
}

func (it *iterator) Valid() bool        { return it.inner.Valid() }
func (it *iterator) Next() bool           { return it.inner.Next() }
func (it *iterator) Prev() bool           { return it.inner.Prev() }
func (it *iterator) Seek(key []byte) bool { return it.inner.Seek(key) }
func (it *iterator) Last() bool           { return it.inner.Last() }
func (it *iterator) Key() []byte          { return it.inner.Key() }
func (it *iterator) Value() []byte       { return it.inner.Value() }
func (it *iterator) Release()            { it.inner.Release() }
func (it *iterator) Error() error        { return it.inner.Error() }

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// RemoveAll deletes the index directory contents.
func RemoveAll(path string) error {
	return os.RemoveAll(path)
}

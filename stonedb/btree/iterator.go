package btree

// Range restricts iterator traversal to [Start, Limit).
type Range struct {
	Start []byte
	Limit []byte
}

// Iterator walks keys in sorted order. It holds a read lock on the tree from
// creation until Release(), providing a stable snapshot for the duration.
type Iterator struct {
	tree   *Tree
	range_ *Range
	leaf   uint64
	idx    int
	valid  bool
	err    error
}

// NewIterator creates an iterator in an invalid state. Call Seek, First, or
// Next to position it (matching LevelDB iterator semantics).
func (t *Tree) NewIterator(r *Range) *Iterator {
	t.mu.RLock()
	it := &Iterator{tree: t, range_: r}
	if t.mf == nil {
		it.err = ErrClosed
	}
	return it
}

func (it *Iterator) inRange(key []byte) bool {
	if it.range_ == nil {
		return true
	}
	if it.range_.Start != nil && compareKeys(key, it.range_.Start) < 0 {
		return false
	}
	if it.range_.Limit != nil && compareKeys(key, it.range_.Limit) >= 0 {
		return false
	}
	return true
}

func (it *Iterator) First() bool {
	if it.tree.mf == nil {
		it.valid = false
		it.err = ErrClosed
		return false
	}
	root := it.tree.rootPage()
	if root == 0 {
		it.valid = false
		return false
	}
	leaf := it.tree.leftmostLeaf()
	it.leaf = leaf
	it.idx = 0
	return it.advanceToValid()
}

func (it *Iterator) Last() bool {
	if it.tree.mf == nil {
		it.valid = false
		it.err = ErrClosed
		return false
	}
	leaf := it.tree.rightmostLeaf()
	if leaf == 0 {
		it.valid = false
		return false
	}
	it.leaf = leaf
	it.idx = it.tree.leafCount(leaf) - 1
	for it.idx >= 0 {
		k, _, err := it.tree.leafEntry(leaf, it.idx)
		if err != nil {
			it.err = err
			it.valid = false
			return false
		}
		if it.inRange(k) {
			it.valid = true
			return true
		}
		it.idx--
	}
	for {
		prev := it.tree.leafPrev(it.leaf)
		if prev == 0 {
			it.valid = false
			return false
		}
		it.leaf = prev
		it.idx = it.tree.leafCount(prev) - 1
		for it.idx >= 0 {
			k, _, err := it.tree.leafEntry(prev, it.idx)
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			if it.inRange(k) {
				it.valid = true
				return true
			}
			it.idx--
		}
	}
}

func (it *Iterator) Seek(key []byte) bool {
	if it.tree.mf == nil {
		it.valid = false
		it.err = ErrClosed
		return false
	}
	root := it.tree.rootPage()
	if root == 0 {
		it.valid = false
		return false
	}
	leaf, idx, _ := it.tree.findLeaf(root, key)
	it.leaf = leaf
	it.idx = idx
	return it.advanceToValid()
}

func (it *Iterator) advanceToValid() bool {
	for {
		if it.leaf == 0 {
			it.valid = false
			return false
		}
		n := it.tree.leafCount(it.leaf)
		for it.idx < n {
			k, _, err := it.tree.leafEntry(it.leaf, it.idx)
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			if it.inRange(k) {
				it.valid = true
				return true
			}
			it.idx++
		}
		next := it.tree.leafNext(it.leaf)
		if next == 0 {
			it.valid = false
			return false
		}
		it.leaf = next
		it.idx = 0
	}
}

func (it *Iterator) Valid() bool {
	return it.valid && it.err == nil
}

func (it *Iterator) Next() bool {
	if !it.valid {
		return it.First()
	}
	it.idx++
	return it.advanceToValid()
}

func (it *Iterator) Prev() bool {
	if !it.valid {
		return false
	}
	it.idx--
	for {
		if it.idx >= 0 {
			k, _, err := it.tree.leafEntry(it.leaf, it.idx)
			if err != nil {
				it.err = err
				it.valid = false
				return false
			}
			if it.inRange(k) {
				it.valid = true
				return true
			}
			it.idx--
			continue
		}
		prev := it.tree.leafPrev(it.leaf)
		if prev == 0 {
			it.valid = false
			return false
		}
		it.leaf = prev
		it.idx = it.tree.leafCount(prev) - 1
	}
}

func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	k, _, err := it.tree.leafEntry(it.leaf, it.idx)
	if err != nil {
		it.err = err
		it.valid = false
		return nil
	}
	return k
}

func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	_, v, err := it.tree.leafEntry(it.leaf, it.idx)
	if err != nil {
		it.err = err
		it.valid = false
		return nil
	}
	return v
}

func (it *Iterator) Error() error {
	return it.err
}

func (it *Iterator) Release() {
	it.tree.mu.RUnlock()
	it.valid = false
}

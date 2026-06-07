package btree

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrNotFound = errors.New("not found")
	ErrClosed   = errors.New("btree closed")
)

type pathEntry struct {
	page  uint64
	child int
}

// Tree is a memory-mapped B+ tree with byte-ordered keys.
type Tree struct {
	mu   sync.RWMutex
	mf   *mmapFile
	path string
}

// Open opens or creates a B+ tree at dir.
func Open(dir string) (*Tree, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "data.bt")
	mf, err := openMmap(path, 16)
	if err != nil {
		return nil, err
	}
	t := &Tree{mf: mf, path: path}
	if readU64(mf.page(metaPageID), metaMagicOff) != magic {
		t.initMeta()
	}
	return t, nil
}

func (t *Tree) initMeta() {
	p := t.mf.page(metaPageID)
	writeU64(p, metaMagicOff, magic)
	writeU32(p, metaVersionOff, version)
	writeU64(p, metaRootOff, 0)
	writeU64(p, metaNumPagesOff, 1)
	writeU64(p, metaFreeHeadOff, 0)
	writeU64(p, metaLeftLeafOff, 0)
}

func (t *Tree) rootPage() uint64  { return readU64(t.mf.page(metaPageID), metaRootOff) }
func (t *Tree) setRoot(p uint64)  { writeU64(t.mf.page(metaPageID), metaRootOff, p) }
func (t *Tree) leftLeaf() uint64  { return readU64(t.mf.page(metaPageID), metaLeftLeafOff) }
func (t *Tree) setLeftLeaf(p uint64) { writeU64(t.mf.page(metaPageID), metaLeftLeafOff, p) }
func (t *Tree) numPages() uint64  { return readU64(t.mf.page(metaPageID), metaNumPagesOff) }
func (t *Tree) setNumPages(n uint64) { writeU64(t.mf.page(metaPageID), metaNumPagesOff, n) }
func (t *Tree) freeHead() uint64  { return readU64(t.mf.page(metaPageID), metaFreeHeadOff) }
func (t *Tree) setFreeHead(p uint64) { writeU64(t.mf.page(metaPageID), metaFreeHeadOff, p) }

func (t *Tree) allocPage() (uint64, error) {
	head := t.freeHead()
	if head != 0 {
		p := t.mf.page(head)
		next := readU64(p, 0)
		t.setFreeHead(next)
		for i := range p {
			p[i] = 0
		}
		return head, nil
	}
	id := t.numPages()
	if err := t.mf.grow(id + 1); err != nil {
		return 0, err
	}
	t.setNumPages(id + 1)
	p := t.mf.page(id)
	for i := range p {
		p[i] = 0
	}
	return id, nil
}

func (t *Tree) freePage(id uint64) {
	p := t.mf.page(id)
	writeU64(p, 0, t.freeHead())
	t.setFreeHead(id)
}

func (t *Tree) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mf == nil {
		return nil
	}
	err := t.mf.close()
	t.mf = nil
	return err
}

func (t *Tree) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mf == nil {
		return ErrClosed
	}
	return t.mf.sync()
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.getLocked(key)
}

// GetLocked looks up key. The caller must already hold the tree read lock.
func (t *Tree) GetLocked(key []byte) ([]byte, error) {
	return t.getLocked(key)
}

func (t *Tree) getLocked(key []byte) ([]byte, error) {
	if t.mf == nil {
		return nil, ErrClosed
	}
	root := t.rootPage()
	if root == 0 {
		return nil, ErrNotFound
	}
	leaf, idx, found := t.findLeaf(root, key)
	if !found {
		return nil, ErrNotFound
	}
	_, val, err := t.leafEntry(leaf, idx)
	return val, err
}

func (t *Tree) Put(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mf == nil {
		return ErrClosed
	}
	return t.putLocked(key, value)
}

func (t *Tree) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mf == nil {
		return ErrClosed
	}
	return t.deleteLocked(key)
}

func (t *Tree) ApplyBatch(puts [][2][]byte, deletes [][]byte, sync bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mf == nil {
		return ErrClosed
	}
	for _, kv := range puts {
		if err := t.putLocked(kv[0], kv[1]); err != nil {
			return err
		}
	}
	for _, k := range deletes {
		if err := t.deleteLocked(k); err != nil {
			return err
		}
	}
	if sync {
		return t.mf.sync()
	}
	return nil
}

func (t *Tree) putLocked(key, value []byte) error {
	root := t.rootPage()
	if root == 0 {
		leaf, err := t.allocPage()
		if err != nil {
			return err
		}
		t.initLeaf(leaf, 0, 0)
		t.leafInsert(leaf, 0, key, value)
		t.setRoot(leaf)
		t.setLeftLeaf(leaf)
		return nil
	}
	path, leaf, idx, found := t.findLeafPath(root, key)
	if found {
		return t.leafUpdate(leaf, idx, value)
	}
	return t.insertIntoLeaf(path, leaf, idx, key, value)
}

func (t *Tree) deleteLocked(key []byte) error {
	root := t.rootPage()
	if root == 0 {
		return nil
	}
	_, leaf, idx, found := t.findLeafPath(root, key)
	if !found {
		return nil
	}
	return t.leafDelete(leaf, idx)
}

func (t *Tree) pageType(page uint64) byte { return t.mf.page(page)[0] }

func (t *Tree) initLeaf(page, prev, next uint64) {
	p := t.mf.page(page)
	p[0] = pageTypeLeaf
	writeU16(p, 1, 0)
	writeU64(p, 3, prev)
	writeU64(p, 11, next)
}

func (t *Tree) leafCount(page uint64) int { return int(readU16(t.mf.page(page), 1)) }
func (t *Tree) setLeafCount(page uint64, n int) { writeU16(t.mf.page(page), 1, uint16(n)) }
func (t *Tree) leafPrev(page uint64) uint64 { return readU64(t.mf.page(page), 3) }
func (t *Tree) leafNext(page uint64) uint64 { return readU64(t.mf.page(page), 11) }

func (t *Tree) setLeafLinks(page, prev, next uint64) {
	p := t.mf.page(page)
	writeU64(p, 3, prev)
	writeU64(p, 11, next)
}

func (t *Tree) leafEntrySize(page uint64, idx int) int {
	off := leafHeaderSize
	for i := 0; i < idx; i++ {
		kl := int(readU32(t.mf.page(page), off))
		vl := int(readU32(t.mf.page(page), off+4))
		off += 8 + kl + vl
	}
	return off
}

func (t *Tree) leafEntry(page uint64, idx int) (key, val []byte, err error) {
	if idx < 0 || idx >= t.leafCount(page) {
		return nil, nil, errors.New("leaf index out of range")
	}
	off := t.leafEntrySize(page, idx)
	p := t.mf.page(page)
	kl := int(readU32(p, off))
	vl := int(readU32(p, off+4))
	off += 8
	key = cloneBytes(p[off : off+kl])
	val = cloneBytes(p[off+kl : off+kl+vl])
	return key, val, nil
}

func (t *Tree) leafKeyAt(page uint64, idx int) []byte {
	off := t.leafEntrySize(page, idx)
	p := t.mf.page(page)
	kl := int(readU32(p, off))
	off += 8
	return cloneBytes(p[off : off+kl])
}

func (t *Tree) leafUsedBytes(page uint64) int {
	n := t.leafCount(page)
	if n == 0 {
		return leafHeaderSize
	}
	off := t.leafEntrySize(page, n)
	p := t.mf.page(page)
	kl := int(readU32(p, off))
	vl := int(readU32(p, off+4))
	return off + 8 + kl + vl
}

func (t *Tree) rewriteLeaf(page uint64, entries [][2][]byte) {
	p := t.mf.page(page)
	prev, next := t.leafPrev(page), t.leafNext(page)
	for i := range p {
		p[i] = 0
	}
	t.initLeaf(page, prev, next)
	off := leafHeaderSize
	for _, e := range entries {
		writeU32(p, off, uint32(len(e[0])))
		writeU32(p, off+4, uint32(len(e[1])))
		off += 8
		copy(p[off:], e[0])
		off += len(e[0])
		copy(p[off:], e[1])
		off += len(e[1])
	}
	t.setLeafCount(page, len(entries))
}

func (t *Tree) leafInsert(page uint64, idx int, key, val []byte) {
	n := t.leafCount(page)
	entries := make([][2][]byte, 0, n+1)
	for i := 0; i < n; i++ {
		k, v, _ := t.leafEntry(page, i)
		entries = append(entries, [2][]byte{k, v})
	}
	entries = append(entries[:idx], append([][2][]byte{{key, val}}, entries[idx:]...)...)
	t.rewriteLeaf(page, entries)
}

func (t *Tree) leafUpdate(page uint64, idx int, val []byte) error {
	n := t.leafCount(page)
	entries := make([][2][]byte, n)
	for i := 0; i < n; i++ {
		k, v, _ := t.leafEntry(page, i)
		if i == idx {
			entries[i] = [2][]byte{k, val}
		} else {
			entries[i] = [2][]byte{k, v}
		}
	}
	t.rewriteLeaf(page, entries)
	return nil
}

func (t *Tree) findLeaf(root uint64, key []byte) (leaf uint64, idx int, found bool) {
	_, leaf, idx, found = t.findLeafPath(root, key)
	return leaf, idx, found
}

func (t *Tree) findLeafPath(root uint64, key []byte) (path []pathEntry, leaf uint64, idx int, found bool) {
	cur := root
	for t.pageType(cur) != pageTypeLeaf {
		n := t.internalCount(cur)
		p := t.mf.page(cur)
		off := internalHdrSize
		child := readU64(p, off)
		off += 8
		childIdx := 0
		for i := 0; i < n; i++ {
			kl := int(readU32(p, off))
			off += 4
			sep := p[off : off+kl]
			off += kl
			if compareKeys(key, sep) < 0 {
				path = append(path, pathEntry{page: cur, child: childIdx})
				cur = child
				goto descend
			}
			child = readU64(p, off)
			off += 8
			childIdx = i + 1
		}
		path = append(path, pathEntry{page: cur, child: childIdx})
		cur = child
	descend:
	}
	n := t.leafCount(cur)
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		cmp := compareKeys(t.leafKeyAt(cur, mid), key)
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < n && compareKeys(t.leafKeyAt(cur, lo), key) == 0 {
		return path, cur, lo, true
	}
	return path, cur, lo, false
}

func (t *Tree) leafFitsAfterInsert(page uint64, key, val []byte) bool {
	ins := 8 + len(key) + len(val)
	return t.leafUsedBytes(page)+ins <= pageSize && t.leafCount(page) < 128
}

func (t *Tree) insertIntoLeaf(path []pathEntry, leaf uint64, idx int, key, val []byte) error {
	if t.leafFitsAfterInsert(leaf, key, val) {
		t.leafInsert(leaf, idx, key, val)
		return nil
	}
	return t.splitLeaf(path, leaf, idx, key, val)
}

func (t *Tree) splitLeaf(path []pathEntry, leaf uint64, idx int, key, val []byte) error {
	n := t.leafCount(leaf)
	entries := make([][2][]byte, 0, n+1)
	for i := 0; i < n; i++ {
		k, v, _ := t.leafEntry(leaf, i)
		entries = append(entries, [2][]byte{k, v})
	}
	entries = append(entries[:idx], append([][2][]byte{{key, val}}, entries[idx:]...)...)

	mid := (len(entries) + 1) / 2
	left := entries[:mid]
	right := entries[mid:]

	t.rewriteLeaf(leaf, left)
	newLeaf, err := t.allocPage()
	if err != nil {
		return err
	}
	oldNext := t.leafNext(leaf)
	t.initLeaf(newLeaf, leaf, oldNext)
	t.rewriteLeaf(newLeaf, right)
	t.setLeafLinks(leaf, t.leafPrev(leaf), newLeaf)
	if oldNext != 0 {
		t.setLeafLinks(oldNext, newLeaf, t.leafNext(oldNext))
	}
	sep := right[0][0]
	return t.insertInParent(path, leaf, newLeaf, sep)
}

func (t *Tree) insertInParent(path []pathEntry, leftChild, rightChild uint64, sepKey []byte) error {
	if len(path) == 0 {
		newRoot, err := t.allocPage()
		if err != nil {
			return err
		}
		t.initInternal(newRoot)
		t.internalSet(newRoot, []internalEntry{{child: leftChild}, {key: sepKey, child: rightChild}})
		t.setRoot(newRoot)
		return nil
	}
	parent := path[len(path)-1].page
	entries := t.internalEntries(parent)
	newEntries := make([]internalEntry, 0, len(entries)+1)
	for _, e := range entries {
		newEntries = append(newEntries, e)
		if e.child == leftChild {
			newEntries = append(newEntries, internalEntry{key: sepKey, child: rightChild})
		}
	}
	if t.internalFits(newEntries) {
		t.internalSet(parent, newEntries)
		return nil
	}
	return t.splitInternal(path[:len(path)-1], parent, newEntries)
}

type internalEntry struct {
	key   []byte
	child uint64
}

func (t *Tree) internalCount(page uint64) int { return int(readU16(t.mf.page(page), 1)) }
func (t *Tree) setInternalCount(page uint64, n int) { writeU16(t.mf.page(page), 1, uint16(n)) }

func (t *Tree) initInternal(page uint64) {
	p := t.mf.page(page)
	p[0] = pageTypeInternal
	writeU16(p, 1, 0)
}

func (t *Tree) internalEntries(page uint64) []internalEntry {
	n := t.internalCount(page)
	p := t.mf.page(page)
	off := internalHdrSize
	child := readU64(p, off)
	off += 8
	entries := []internalEntry{{child: child}}
	for i := 0; i < n; i++ {
		kl := int(readU32(p, off))
		off += 4
		key := cloneBytes(p[off : off+kl])
		off += kl
		child = readU64(p, off)
		off += 8
		entries = append(entries, internalEntry{key: key, child: child})
	}
	return entries
}

func (t *Tree) internalSet(page uint64, entries []internalEntry) {
	p := t.mf.page(page)
	for i := range p {
		p[i] = 0
	}
	t.initInternal(page)
	off := internalHdrSize
	writeU64(p, off, entries[0].child)
	off += 8
	for i := 1; i < len(entries); i++ {
		writeU32(p, off, uint32(len(entries[i].key)))
		off += 4
		copy(p[off:], entries[i].key)
		off += len(entries[i].key)
		writeU64(p, off, entries[i].child)
		off += 8
	}
	t.setInternalCount(page, len(entries)-1)
}

func (t *Tree) internalFits(entries []internalEntry) bool {
	size := internalHdrSize + 8
	for i := 1; i < len(entries); i++ {
		size += 4 + len(entries[i].key) + 8
	}
	return size <= pageSize
}

func (t *Tree) splitInternal(path []pathEntry, page uint64, entries []internalEntry) error {
	mid := len(entries) / 2
	promote := entries[mid].key
	right := entries[mid:]
	right[0].key = nil
	left := entries[:mid]
	t.internalSet(page, left)

	newPage, err := t.allocPage()
	if err != nil {
		return err
	}
	t.internalSet(newPage, right)

	if len(path) == 0 {
		newRoot, err := t.allocPage()
		if err != nil {
			return err
		}
		t.initInternal(newRoot)
		t.internalSet(newRoot, []internalEntry{{child: page}, {key: promote, child: newPage}})
		t.setRoot(newRoot)
		return nil
	}
	parent := path[len(path)-1].page
	parentEntries := t.internalEntries(parent)
	newParent := make([]internalEntry, 0, len(parentEntries)+1)
	inserted := false
	for _, e := range parentEntries {
		newParent = append(newParent, e)
		if e.child == page && !inserted {
			newParent = append(newParent, internalEntry{key: promote, child: newPage})
			inserted = true
		}
	}
	if !inserted {
		newParent = append(newParent, internalEntry{key: promote, child: newPage})
	}
	if t.internalFits(newParent) {
		t.internalSet(parent, newParent)
		return nil
	}
	return t.splitInternal(path[:len(path)-1], parent, newParent)
}

func (t *Tree) leafDelete(page uint64, idx int) error {
	n := t.leafCount(page)
	entries := make([][2][]byte, 0, n-1)
	for i := 0; i < n; i++ {
		if i == idx {
			continue
		}
		k, v, _ := t.leafEntry(page, i)
		entries = append(entries, [2][]byte{k, v})
	}
	if len(entries) == 0 {
		return t.removeLeaf(page)
	}
	t.rewriteLeaf(page, entries)
	return nil
}

func (t *Tree) removeLeaf(page uint64) error {
	prev, next := t.leafPrev(page), t.leafNext(page)
	if prev != 0 {
		t.setLeafLinks(prev, t.leafPrev(prev), next)
	}
	if next != 0 {
		t.setLeafLinks(next, prev, t.leafNext(next))
	}
	if t.leftLeaf() == page {
		if next != 0 {
			t.setLeftLeaf(next)
		} else if prev != 0 {
			t.setLeftLeaf(prev)
		} else {
			t.setLeftLeaf(0)
		}
	}
	if page == t.rootPage() {
		t.setRoot(0)
	}
	t.freePage(page)
	return nil
}

func (t *Tree) rightmostLeaf() uint64 {
	root := t.rootPage()
	if root == 0 {
		return 0
	}
	cur := root
	for t.pageType(cur) != pageTypeLeaf {
		entries := t.internalEntries(cur)
		cur = entries[len(entries)-1].child
	}
	return cur
}

func (t *Tree) leftmostLeaf() uint64 {
	if ll := t.leftLeaf(); ll != 0 {
		return ll
	}
	root := t.rootPage()
	if root == 0 {
		return 0
	}
	cur := root
	for t.pageType(cur) != pageTypeLeaf {
		entries := t.internalEntries(cur)
		cur = entries[0].child
	}
	return cur
}

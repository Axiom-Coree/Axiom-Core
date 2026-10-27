package state

import (
	"sync/atomic"

	"Axiom-Core/pkg/model"
)

// Store is the snapshot root holder. Writers replace the root with a
// COW tree via CAS; readers atomically load the root once and traverse
// the immutable tree with no locks.
//
// Concurrency:
//   - Any number of goroutines may call Snapshot, Get, or Range.
//   - Exactly one goroutine may call Put (or PutCAS for multi-writer).
//   - Snapshot returns a snapshot that remains valid forever. There is
//     no release protocol in Day 5; snapshots are cheap (just the
//     root pointer), and the nodes they reference are never freed.
//     Reclamation is Day 6+.
type Store struct {
	root atomic.Pointer[node]
}

// NewStore returns an empty Store.
func NewStore() *Store { return &Store{} }

// Snapshot returns an immutable view of the tree as of the moment of
// the call. Traversals of the returned Snapshot are safe concurrently
// with writers and with other readers.
func (s *Store) Snapshot() Snapshot {
	return Snapshot{root: s.root.Load()}
}

// Put inserts or updates an account. The tree is updated via COW:
// only the path from root to leaf is cloned, every other subtree is
// shared. Concurrency: Put is single-writer. Concurrent Puts race
// (one wins the CAS, the other loses and must retry).
func (s *Store) Put(a model.Account) {
	for {
		old := s.root.Load()
		newRoot := put(old, a.ID, a)
		if s.root.CompareAndSwap(old, newRoot) {
			return
		}
		// Lost the CAS race; reload and retry.
	}
}

// Len is not tracked; the tree doesn't maintain a size counter (that
// would require the size to be part of the root, which complicates
// COW). For a size, iterate a Snapshot and count.

// Snapshot is an immutable view of a Store.
type Snapshot struct {
	root *node
}

// Get returns the account at id, or (zero, false).
func (s Snapshot) Get(id model.AccountID) (model.Account, bool) {
	return get(s.root, id)
}

// Range calls fn for every account in ascending AccountID order. It
// stops if fn returns false.
func (s Snapshot) Range(fn func(model.Account) bool) {
	rangeWalk(s.root, fn)
}

func rangeWalk(n *node, fn func(model.Account) bool) bool {
	if n == nil {
		return true
	}
	if n.isLeaf() {
		for _, a := range n.values {
			if !fn(a) {
				return false
			}
		}
		return true
	}
	for i := range n.keys {
		if !rangeWalk(n.children[i], fn) {
			return false
		}
		if !fn(n.values[i]) {
			return false
		}
	}
	return rangeWalk(n.children[len(n.keys)], fn)
}

// Len returns the number of accounts in the snapshot. O(n).
func (s Snapshot) Len() int {
	n := 0
	s.Range(func(model.Account) bool {
		n++
		return true
	})
	return n
}

// All returns every account in the snapshot, sorted by ID. Allocates
// one slice; do not call in a hot loop.
func (s Snapshot) All() []model.Account {
	return collect(s.root, nil)
}

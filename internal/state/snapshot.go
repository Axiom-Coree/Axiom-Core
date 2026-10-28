package state

import (
	"sync"
	"sync/atomic"

	"Axiom-Core/pkg/model"
)

// Store is the snapshot root holder.
//
// Concurrency:
//   - Any number of goroutines may call Snapshot, Get, Range.
//   - Exactly one goroutine may call Put.
//   - A background reclaimer goroutine runs from NewStore; stop it
//     with Close.
//
// Lifecycle: a SnapshotHandle pins nothing in Day 6 (reclamation is
// Day 7+), but the API has Release so callers can develop the habit
// now and so that adding reclamation later is a non-breaking change.
//
// Design note (Day 7 handoff):
//
// Reclamation in a COW tree is harder than "drop the old root" because
// the tree shares subtrees across epochs. A node displaced at epoch D
// may still be reachable from a newer root via structural sharing. The
// two correct designs are:
//
//  1. Refcount per node — simple, but the atomic increment on the hot
//     path (SnapshotRead) would cost 20-30% on this CPU.
//  2. Mark-and-sweep — correct, off the hot path, but O(live tree
//     size) per reclaim cycle.
//
// Day 7 chooses (2) and adds the per-node pinned flag to support it.
// Day 6's API is designed so this is a non-breaking change.
type Store struct {
	root  atomic.Pointer[node]
	epoch atomic.Uint64

	// mu guards liveEpochs. reads of the tree are lock-free; only
	// epoch registration (a map write) needs the lock.
	mu sync.Mutex

	// liveEpochs counts live SnapshotHandles per epoch. An epoch
	// with count 0 is not in the map.
	liveEpochs map[uint64]int

	closed   chan struct{}
	closedMu sync.Mutex
}

// NewStore returns a Store with the reclaimer goroutine running.
// Day 6's reclaimer is a no-op stub; Day 7 will fill it in. The
// goroutine exists so the API is stable across the two days.
func NewStore() *Store {
	s := &Store{
		liveEpochs: make(map[uint64]int),
		closed:     make(chan struct{}),
	}
	go s.reclaimer()
	return s
}

// Close stops the background reclaimer. Snapshots taken before Close
// remain valid (they still traverse their immutable trees), but
// Release after Close is a no-op and no reclamation happens.
//
// Idempotent.
func (s *Store) Close() {
	s.closedMu.Lock()
	defer s.closedMu.Unlock()
	select {
	case <-s.closed:
		return
	default:
	}
	close(s.closed)
}

// SnapshotHandle is an immutable view of a Store at the moment
// Snapshot() was called.
//
// A handle must be passed by pointer. It is not safe to copy after
// Release has been called.
type SnapshotHandle struct {
	root  *node
	epoch uint64
	store *Store
}

// Snapshot returns a handle pinned against reclamation (once Day 7's
// reclaimer lands; Day 6's is a no-op). Callers MUST call Release (or
// defer it) when done.
//
// Snapshot is lock-free on the tree traversal side; the epoch
// registration is a single map insert under a small mutex, which
// happens ~once per snapshot, not once per Get.
func (s *Store) Snapshot() *SnapshotHandle {
	root := s.root.Load()
	epoch := s.epoch.Load()

	s.mu.Lock()
	s.liveEpochs[epoch]++
	s.mu.Unlock()

	return &SnapshotHandle{root: root, epoch: epoch, store: s}
}

// Release unpins the snapshot. After Release the handle MUST NOT be
// used. Idempotent.
func (h *SnapshotHandle) Release() {
	if h == nil || h.store == nil {
		return
	}
	s := h.store
	h.store = nil

	s.mu.Lock()
	if n := s.liveEpochs[h.epoch]; n <= 1 {
		delete(s.liveEpochs, h.epoch)
	} else {
		s.liveEpochs[h.epoch] = n - 1
	}
	s.mu.Unlock()
}

// Get returns the account at id, or (zero, false).
func (h *SnapshotHandle) Get(id model.AccountID) (model.Account, bool) {
	if h == nil {
		return model.Account{}, false
	}
	return get(h.root, id)
}

// Range calls fn for every account in ascending ID order, stopping if
// fn returns false.
func (h *SnapshotHandle) Range(fn func(model.Account) bool) {
	if h == nil {
		return
	}
	rangeWalk(h.root, fn)
}

// Len returns the number of accounts. O(n).
func (h *SnapshotHandle) Len() int {
	if h == nil {
		return 0
	}
	n := 0
	rangeWalk(h.root, func(model.Account) bool { n++; return true })
	return n
}

// All returns every account sorted by ID.
func (h *SnapshotHandle) All() []model.Account {
	if h == nil {
		return nil
	}
	return collect(h.root, nil)
}

// Put inserts or updates an account. Single-writer. The CAS is
// defensive against a second writer; the design assumes one.
func (s *Store) Put(a model.Account) {
	old := s.root.Load()
	newRoot := put(old, a.ID, a)
	if s.root.CompareAndSwap(old, newRoot) {
		s.epoch.Add(1)
	}
}

// reclaimer is the background goroutine. Day 6: a no-op stub that
// just waits for Close. Day 7 will replace the body with a
// mark-and-sweep pass that walks liveEpochs, computes minEpoch, and
// frees subtrees older than minEpoch that are not reachable from any
// live root.
func (s *Store) reclaimer() {
	<-s.closed
}

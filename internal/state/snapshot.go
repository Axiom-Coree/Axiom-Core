package state

import (
	"sync"
	"sync/atomic"
	"time"

	"Axiom-Core/pkg/model"
)

// Default bounds. Overridable per-Store for tests and for callers with
// tighter memory budgets.
const (
	defaultMaxLease        = 30 * time.Second
	defaultReclaimInterval = 1 * time.Second
)

// Store is the snapshot root holder.
//
// Concurrency:
//   - Any number of goroutines may call Snapshot, Get, Range.
//   - Exactly one goroutine may call Put.
//   - A background reclaimer goroutine runs from NewStore; stop it with
//     Close.
//
// Memory bound: every SnapshotHandle is registered with a lease. Handles
// that are not Released within maxLease are invalidated by the
// reclaimer — their root is set to nil and the Go GC frees their
// nodes. The peak live heap is therefore bounded by
// maxLease × putRate × (bytes retained per put), regardless of caller
// behavior. This is the Day-7 guarantee; Day 6's Release() gives the
// caller fast-path control, and Day 7's reclaimer catches the ones
// who forget.
type Store struct {
	root  atomic.Pointer[node]
	epoch atomic.Uint64

	// mu guards handles. Snapshot, Release, and the reclaimer each
	// take it briefly. The tree traversal is lock-free.
	mu sync.Mutex

	// handles is the live-snapshot registry. The reclaimer iterates
	// it to find expired handles.
	handles map[*SnapshotHandle]struct{}

	maxLease        time.Duration
	reclaimInterval time.Duration
	now             func() time.Time

	closed   chan struct{}
	closedMu sync.Mutex
}

// NewStore returns a Store with the default lease and interval.
func NewStore() *Store {
	return newStore(defaultMaxLease, defaultReclaimInterval, time.Now)
}

// NewStoreWithLease returns a Store with custom lease and interval.
// Passing 0 for either uses the default. Tests use this to run a
// reclaimer with a sub-millisecond lease.
func NewStoreWithLease(maxLease, interval time.Duration) *Store {
	if maxLease == 0 {
		maxLease = defaultMaxLease
	}
	if interval == 0 {
		interval = defaultReclaimInterval
	}
	return newStore(maxLease, interval, time.Now)
}

func newStore(maxLease, interval time.Duration, now func() time.Time) *Store {
	s := &Store{
		handles:         make(map[*SnapshotHandle]struct{}),
		maxLease:        maxLease,
		reclaimInterval: interval,
		now:             now,
		closed:          make(chan struct{}),
	}
	go s.reclaimer()
	return s
}

// Close stops the background reclaimer. Snapshots taken before Close
// remain usable (they're not invalidated), but no further expired
// handles are reclaimed.
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

// SnapshotHandle is an immutable view of a Store.
//
// Lifetime: a handle pins its tree until either Release() is called or
// the reclaimer invalidates it after maxLease. After either, Get/Range/
// Len/All return zero/nil — the handle is "dead" but safe to call.
//
// Callers MUST call Release() when done, ideally via defer, to release
// the tree promptly. Forgetting is not a memory leak in Day 7 (the
// reclaimer will invalidate), but it delays reclamation until the
// lease expires.
type SnapshotHandle struct {
	mu      sync.Mutex // guards root and store fields (Release vs reclaimer)
	root    *node
	takenAt time.Time
	store   *Store
}

// Snapshot returns a handle pinned against reclamation for up to
// maxLease. The handle is fully usable until Release or lease expiry.
func (s *Store) Snapshot() *SnapshotHandle {
	h := &SnapshotHandle{
		root:    s.root.Load(),
		takenAt: s.now(),
		store:   s,
	}
	s.mu.Lock()
	s.handles[h] = struct{}{}
	s.mu.Unlock()
	return h
}

// Release detaches the handle from the Store. After Release, the
// handle's Get/Range/Len/All return zero/nil, and the tree it pinned
// is a candidate for GC. Idempotent.
func (h *SnapshotHandle) Release() {
	if h == nil {
		return
	}
	h.mu.Lock()
	s := h.store
	h.store = nil
	h.root = nil
	h.mu.Unlock()
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.handles, h)
	s.mu.Unlock()
}

// liveRoot returns the handle's root, or nil if the handle is dead.
// Read by Get/Range/Len/All; synchronized against Release and the
// reclaimer via h.mu.
func (h *SnapshotHandle) liveRoot() *node {
	h.mu.Lock()
	r := h.root
	h.mu.Unlock()
	return r
}

// Get returns the account at id, or (zero, false). Safe on a dead
// handle (returns false).
func (h *SnapshotHandle) Get(id model.AccountID) (model.Account, bool) {
	if h == nil {
		return model.Account{}, false
	}
	return get(h.liveRoot(), id)
}

// Range calls fn for every account in ascending ID order. No-op on a
// dead handle.
func (h *SnapshotHandle) Range(fn func(model.Account) bool) {
	if h == nil {
		return
	}
	rangeWalk(h.liveRoot(), fn)
}

// Len returns the number of accounts. 0 on a dead handle.
func (h *SnapshotHandle) Len() int {
	if h == nil {
		return 0
	}
	n := 0
	rangeWalk(h.liveRoot(), func(model.Account) bool { n++; return true })
	return n
}

// All returns every account sorted by ID. nil on a dead handle.
func (h *SnapshotHandle) All() []model.Account {
	if h == nil {
		return nil
	}
	return collect(h.liveRoot(), nil)
}

// Put inserts or updates an account. Single-writer.
func (s *Store) Put(a model.Account) {
	old := s.root.Load()
	newRoot := put(old, a.ID, a)
	if s.root.CompareAndSwap(old, newRoot) {
		s.epoch.Add(1)
	}
}

// reclaimer invalidates expired handles every reclaimInterval. When a
// handle is invalidated, its root is set to nil and it is removed from
// the registry. Its tree becomes unreachable (unless shared with the
// current root or another live handle), and Go's GC frees it at the
// next cycle.
//
// This is the Day-7 design: the reclaimer does NOT walk the tree.
// Go's GC is the mark-and-sweep. The reclaimer's job is to bound the
// peak heap by deciding when a root becomes unreachable. The bound is
// maxLease × putRate × bytesPerPut.
func (s *Store) reclaimer() {
	ticker := time.NewTicker(s.reclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
			s.reclaimExpired()
		}
	}
}

func (s *Store) reclaimExpired() {
	now := s.now()
	var expired []*SnapshotHandle
	s.mu.Lock()
	for h := range s.handles {
		if now.Sub(h.takenAt) > s.maxLease {
			expired = append(expired, h)
		}
	}
	s.mu.Unlock()

	for _, h := range expired {
		h.mu.Lock()
		h.root = nil
		h.store = nil
		h.mu.Unlock()
	}
	if len(expired) > 0 {
		s.mu.Lock()
		for _, h := range expired {
			delete(s.handles, h)
		}
		s.mu.Unlock()
	}
}

// NumLiveHandles returns the number of registered handles. For tests
// and monitoring.
func (s *Store) NumLiveHandles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.handles)
}

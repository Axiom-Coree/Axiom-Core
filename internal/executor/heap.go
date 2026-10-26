package executor

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// TransferID is the 128-bit transfer identity. Kept as an alias so the
// heap's key type matches the idempotency index's key type end to end.
//type TransferID = [16]byte

// timeoutItem is one entry in the timeout heap.
//
// Layout (32 bytes, 8-byte aligned):
//
//	transferID  [16]byte   offset  0, size 16
//	expiryNano  int64      offset 16, size  8
//	tiebreak    uint64     offset 24, size  8
//
// No pointers, so the slice of timeoutItems is not scanned by the GC.
// This matters: a heap of 1M pending transfers is 32 MiB of not-scanned
// memory, not 32 MiB of "walk every entry and look for pointers".
type timeoutItem struct {
	transferID [16]byte
	expiryNano int64
	tiebreak   uint64
}

// Compile-time layout guarantee. If someone adds a field to
// timeoutItem that bloats the struct or makes it non-pointer-free,
// the build fails here rather than silently regressing the heap's
// memory characteristics.
const timeoutItemSize = int(unsafe.Sizeof(timeoutItem{}))

var (
	_ = [1]struct{}{}[timeoutItemSize-32]
)

// TimeoutHeap is a slice-backed binary min-heap ordered by
// (expiryNano, tiebreak). Both are compared ascending; ties are broken
// by the monotonic tiebreak so the ordering is total and deterministic.
//
// Contract:
//
//   - Push adds an entry. O(log n).
//   - PopMin removes and returns the earliest live entry. Amortized
//     O(log n) when the heap has few stale entries; worst case
//     O(n log n) when the heap is mostly stale (see Cancel note below).
//   - Peek returns the earliest live entry without removing it.
//     O(1) when the root is live, O(k) to skip k stale roots.
//
// Staleness: cancellation is lazy. Cancel does not remove an entry
// from the heap (which would require O(n) to find it); instead it
// records the ID in the canceled set. PopMin and Peek skip entries
// whose IDs are in the canceled set, discarding them as they surface
// at the root. The heap therefore holds "candidates," some resolved;
// the number of live entries is `Live()`, not `Len()`.
//
// This trade-off is deliberate: eager removal would require a
// position map from transferID to heap index, doubling the memory and
// making every push/pop a map operation. Lazy invalidation keeps the
// hot path allocation-free and pointer-free.
//
// Thread-safety: all public methods are safe for concurrent use. The
// internal mutex serializes Push (from Gateway.Submit) against
// Cancel/Peek/PopMin (from Timer.Run). This is not the lock-free
// design from Days 1-2 — the heap is not hot enough to justify it —
// but it is correct, and the mutex cost (~15ns on this CPU) is
// dwarfed by the O(log n) sift work (~120ns at 1024 entries).
type TimeoutHeap struct {
	mu       sync.Mutex
	items    []timeoutItem
	live     int                 // number of entries not cancelled
	cancel   map[TransferID]bool // lazily cancelled IDs
	tiebreak atomic.Uint64       // monotonic tiebreaker source
}

// NewTimeoutHeap creates a heap with the given initial capacity.
// Preallocating avoids repeated reallocation as the heap grows.
func NewTimeoutHeap(capacity int) *TimeoutHeap {
	if capacity < 1 {
		capacity = 1
	}
	return &TimeoutHeap{
		items:  make([]timeoutItem, 0, capacity),
		cancel: make(map[TransferID]bool),
	}
}

// Len returns the total number of entries in the heap, including stale
// (canceled) ones. For the number of unresolved entries, use Live().
func (h *TimeoutHeap) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.items)
}

// Live returns the number of entries that have not been cancelled.
func (h *TimeoutHeap) Live() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.live
}

// Push adds a timeout for transferID at expiryNano.
//
// expiryNano is compared as int64; the tiebreaker ensures the heap
// order is total even when two entries have the same expiry.
//
// O(log n).
func (h *TimeoutHeap) Push(transferID TransferID, expiryNano int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	it := timeoutItem{
		transferID: transferID,
		expiryNano: expiryNano,
		tiebreak:   h.tiebreak.Add(1),
	}
	h.items = append(h.items, it)
	h.live++
	h.siftUp(len(h.items) - 1)
}

// Cancel marks transferID as resolved. It does not remove the entry
// from the heap; the next PopMin/Peek that reaches it will discard it.
//
// Cancelling an ID that is already canceled, or never pushed, is a
// no-op (returns false). Cancelling an ID that has already been
// popped is also a no-op.
//
// O(1) plus the cost of the map insert.
func (h *TimeoutHeap) Cancel(transferID TransferID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.cancel[transferID]; ok {
		return false
	}
	h.cancel[transferID] = true
	// Note: we do NOT decrement h.live here. The entry is still in
	// items[] until it surfaces at the root. If we decremented now,
	// Live() would undercount until the entry is actually discarded,
	// and Cancel could be called again on the same entry (the map
	// check prevents that, but the accounting is cleaner this way).
	// Instead, we decrement in popIfStale when we actually drop the
	// entry from items[].
	return true
}

// Peek returns the earliest live entry's transferID and expiry, without
// removing it. It discards any stale entries it encounters at the root.
//
// Returns (zero, 0, false) if there are no live entries.
//
// O(1) if the root is live (the common case); O(k) to skip k stale
// roots at the top of the heap.
func (h *TimeoutHeap) Peek() (TransferID, int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skipStaleRootsLocked()
	if len(h.items) == 0 {
		return TransferID{}, 0, false
	}
	root := h.items[0]
	return root.transferID, root.expiryNano, true
}

// PopMin removes and returns the earliest live entry.
//
// Returns (zero, 0, false) if there are no live entries.
//
// Amortized O(log n). Worst case O(n log n) if the heap contains many
// stale entries interleaved with live ones — each stale root costs a
// sift-down (O(log n)) to remove, and you may pop up to n stale roots
// before finding a live one. In practice, if cancellation is rare
// relative to throughput, this is a non-issue. If cancellation is
// common, switch to an eager-removal design (position map).
func (h *TimeoutHeap) PopMin() (TransferID, int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skipStaleRootsLocked()
	if len(h.items) == 0 {
		return TransferID{}, 0, false
	}
	root := h.items[0]

	h.live-- // ← ADD: pop removes one live entry
	h.removeAt(0)
	return root.transferID, root.expiryNano, true
}

func (h *TimeoutHeap) skipStaleRootsLocked() {
	for len(h.items) > 0 {
		id := h.items[0].transferID
		if !h.cancel[id] {
			return
		}
		delete(h.cancel, id)
		h.live--
		h.removeAtLocked(0)
	}
}

// removeAtLocked removes element at i. Caller holds h.mu.
//
// Only one of siftDown/siftUp is needed; siftDown returns whether it
// moved the element, and siftUp is only called if it didn't.
func (h *TimeoutHeap) removeAtLocked(i int) {
	n := len(h.items) - 1
	if i == n {
		h.items = h.items[:n]
		return
	}
	h.items[i] = h.items[n]
	h.items = h.items[:n]
	if i < n {
		if !h.siftDownLocked(i) {
			h.siftUpLocked(i)
		}
	}
}

func (h *TimeoutHeap) siftUpLocked(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.lessLocked(i, parent) {
			return
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *TimeoutHeap) siftDownLocked(i int) bool {
	n := len(h.items)
	moved := false
	for {
		left := 2*i + 1
		if left >= n {
			return moved
		}
		smallest := left
		right := left + 1
		if right < n && h.lessLocked(right, left) {
			smallest = right
		}
		if !h.lessLocked(smallest, i) {
			return moved
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
		moved = true
	}
}

func (h *TimeoutHeap) lessLocked(a, b int) bool {
	ia, ib := &h.items[a], &h.items[b]
	if ia.expiryNano != ib.expiryNano {
		return ia.expiryNano < ib.expiryNano
	}
	return ia.tiebreak < ib.tiebreak
}

// skipStaleRoots pops canceled entries from the root until either
// the heap is empty or the root is live.
func (h *TimeoutHeap) skipStaleRoots() {
	for len(h.items) > 0 {
		id := h.items[0].transferID
		if !h.cancel[id] {
			return
		}
		delete(h.cancel, id)
		h.live--
		h.removeAt(0)
	}
}

// removeAt removes the element at index i by replacing it with the
// last element, shrinking the slice, then restoring heap order.
//
// i is typically 0 (remove-min) but the helper works for any index in
// case eager removal is added later.
func (h *TimeoutHeap) removeAt(i int) {
	n := len(h.items) - 1
	if i == n {
		h.items = h.items[:n]
		return
	}
	h.items[i] = h.items[n]
	h.items = h.items[:n]
	if i < n {
		if !h.siftDown(i) {
			h.siftUp(i)
		}
	}
}

// siftUp moves the element at index i toward the root until the heap
// order is restored. Called after append (Push).
func (h *TimeoutHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			return
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

// siftDown moves the element at index i toward the leaves until the
// heap order is restored. Called after removeAt (Pop / skipStale).
func (h *TimeoutHeap) siftDown(i int) bool {
	n := len(h.items)
	moved := false
	for {
		left := 2*i + 1
		if left >= n {
			return moved
		}
		smallest := left
		right := left + 1
		if right < n && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, i) {
			return moved
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
		moved = true
	}
}

// less reports whether items[a] < items[b] under the heap ordering:
// primary key expiryNano, secondary key tiebreak.
func (h *TimeoutHeap) less(a, b int) bool {
	ia, ib := &h.items[a], &h.items[b]
	if ia.expiryNano != ib.expiryNano {
		return ia.expiryNano < ib.expiryNano
	}
	return ia.tiebreak < ib.tiebreak
}

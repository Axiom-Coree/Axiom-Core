// Lock-free MPSC ring buffer

package ingress

import (
	"runtime"
	"sync/atomic"

	"Axiom-Core/pkg/model"
)

// Ring is a bounded, lock-free MPSC ring buffer of model.Transfer values.
//
// Concurrency contract:
//   - Any number of goroutines may call Push concurrently.
//   - Exactly one goroutine may call Pop at a time.
//   - Capacity is a power of two; NewRing rounds up.
//
// Two Push implementations are provided:
//   - Push: general MPSC. Safe from any number of producers.
//   - PushSP: single-producer specialization. Must be called by exactly
//     one goroutine. Skips the head CAS and the published scan; both are
//     pure overhead when there is no contention.
//
// Callers who know they have one producer should use PushSP; it is
// measurably faster (see ring_bench_test.go).
//
// Slot seq encoding (pos is the absolute sequence number):
//
//	free at pos       : seq == pos
//	published at pos  : seq == pos + 1        (written by producer)
//	released after pos: seq == pos + cap      (written by consumer)
type Ring struct {
	slots []slot

	head atomic.Uint64
	_    [56]byte

	published atomic.Uint64
	_         [56]byte

	tail atomic.Uint64
	_    [56]byte

	mask uint64

	// SP-mode flag. Set only by NewRingSP; read in Pop to choose the
	// fast path. Never mutated after construction, so no atomics needed.
	sp bool
}

type slot struct {
	seq      atomic.Uint64
	transfer model.Transfer
}

func NewRing(capacity int) *Ring {
	return newRing(capacity, false)
}

// NewRingSP constructs a ring whose single producer will use PushSP.
// The returned ring must be pushed to by exactly one goroutine.
func NewRingSP(capacity int) *Ring {
	return newRing(capacity, true)
}

func newRing(capacity int, sp bool) *Ring {
	if capacity < 2 {
		capacity = 2
	}
	capPow2 := 1
	for capPow2 < capacity {
		capPow2 <<= 1
	}
	r := &Ring{
		slots: make([]slot, capPow2),
		mask:  uint64(capPow2 - 1),
		sp:    sp,
	}
	for i := range r.slots {
		r.slots[i].seq.Store(uint64(i))
	}
	return r
}

// spin backoff
//
// The previous version called runtime.Gosched() on every failed claim.
// On a 15 W mobile CPU that is a ~100 ns syscall-ish operation, which is
// worse than the wait it's supposed to be papering over. We spin tight
// for a short window, then back off with a real yield. The tight window
// is deliberately small long tight spins on a laptop burn thermal
// budget and clock down the whole chip.

const spinTight = 64

//go:noinline
func spinHint() {
	// On amd64 Go doesn't expose PAUSE directly, but Gosched on a very
	// short window is cheap enough. For portability we use Gosched
	// only after the tight window; the tight window itself is a naked
	// loop that the compiler can't optimize away (CompareAndSwap in
	// the caller retries on the next iteration).
}

// general MPSC Push

// Push attempts to enqueue t. Returns false after maxSpins unsuccessful
// attempts when the ring is full. maxSpins <= 0 spins until success.
//
// Push is safe for concurrent use by any number of goroutines.
func (r *Ring) Push(t model.Transfer, maxSpins int) bool {
	spins := 0
	for {
		pos := r.head.Load()
		s := &r.slots[pos&r.mask]

		if s.seq.Load() != pos {
			if maxSpins > 0 && spins >= maxSpins {
				return false
			}
			spins++
			if spins > spinTight {
				runtime.Gosched()
			}
			continue
		}

		if !r.head.CompareAndSwap(pos, pos+1) {
			continue
		}

		s.transfer = t
		s.seq.Store(pos + 1)

		r.advancePublished(pos + 1)
		return true
	}
}

// advancePublished moves published forward over contiguous published slots.
// O(k) where k is the number of consecutive published slots after cur;
// k == 1 in the uncontended case, k > 1 only under contention.
func (r *Ring) advancePublished(target uint64) {
	for {
		cur := r.published.Load()
		if cur >= target {
			return
		}
		next := cur
		for next < target {
			if r.slots[next&r.mask].seq.Load() != next+1 {
				break
			}
			next++
		}
		if next == cur {
			return
		}
		if r.published.CompareAndSwap(cur, next) {
			if next >= target {
				return
			}
		}
	}
}

// single-producer PushSP

// PushSP is the single-producer fast path. Must be called by exactly one
// goroutine; must not be mixed with Push on the same ring.
//
// Compared to Push:
//   - head is a plain Store, not a CAS. With one producer the CAS can
//     never fail, so it's pure waste (24% of the profile in the SP case).
//   - published is advanced by a plain Store, not a CAS-loop scan. With
//     one producer, publication order == claim order, so no scan is
//     needed (advancePublished was 25% cum in the profile).
//   - the only remaining atomics are the release store on slot.seq
//     (publication) and the acquire load the consumer does on the same
//     word. That pair is the minimum for a correct handoff.
func (r *Ring) PushSP(t model.Transfer, maxSpins int) bool {
	pos := r.head.Load()
	s := &r.slots[pos&r.mask]

	// Wait for the consumer to free this slot if we've lapped.
	spins := 0
	for s.seq.Load() != pos {
		if maxSpins > 0 && spins >= maxSpins {
			return false
		}
		spins++
		if spins > spinTight {
			runtime.Gosched()
		}
	}

	s.transfer = t
	s.seq.Store(pos + 1) // release: publish the payload

	r.head.Store(pos + 1) // release: advance the claim cursor

	// In SP mode, publish in lockstep. No scan, no CAS.
	r.published.Store(pos + 1)
	return true
}

//  Pop

// Pop dequeues one Transfer. Returns false when empty.
// Must be called by exactly one goroutine at a time.
func (r *Ring) Pop() (model.Transfer, bool) {
	pos := r.tail.Load()

	if r.sp {
		// SP fast path: the producer maintains published in lockstep,
		// so published is authoritative and no scan is needed.
		if pos >= r.published.Load() {
			return model.Transfer{}, false
		}
	} else {
		// MPSC path: published can lag because producers publish out
		// of order. Fall back to checking the slot's own seq word,
		// which is the authoritative acquire.
		if pos >= r.published.Load() && r.slots[pos&r.mask].seq.Load() != pos+1 {
			return model.Transfer{}, false
		}
	}

	s := &r.slots[pos&r.mask]

	// Acquire load: pairs with the producer's release store on seq.
	if s.seq.Load() != pos+1 {
		return model.Transfer{}, false
	}

	t := s.transfer

	// Release the slot for the next lap.
	s.seq.Store(pos + uint64(len(r.slots)))
	r.tail.Store(pos + 1)
	return t, true
}

// Len returns an approximate number of unread elements.
func (r *Ring) Len() int {
	p := r.published.Load()
	t := r.tail.Load()
	if p <= t {
		return 0
	}
	return int(p - t)
}

// Cap returns the ring capacity.
func (r *Ring) Cap() int { return len(r.slots) }

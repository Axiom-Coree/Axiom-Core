package executor

import (
	"context"
	"time"
)

// TimeoutEvent is emitted when a transfer's timeout expires before the
// drain has applied it. The consumer (typically the drain's error path)
// decides what to do: mark the transfer failed, reverse any partial
// application, emit an alert, etc.
type TimeoutEvent struct {
	ID         TransferID
	ExpiryNano int64
	FiredNano  int64 // FiredNano >= ExpiryNano
}

// Timer turns the timeout heap into an event stream. It is the SOLE
// reader of the heap: all heap.Push calls come from a single producer
// (Gateway.Submit), and all heap.Cancel calls come through the
// cancellation channel from the drain. This keeps the lock-free heap
// single-producer single-consumer, which is its design contract.
//
// Concurrency contract:
//
//   - Heap.Push: called by Gateway.Submit (single producer).
//   - Heap.Cancel: called by Timer.Run (single consumer of cancels).
//   - Heap.Peek / PopMin: called by Timer.Run (single reader).
//
// The Timer is the only goroutine that touches the heap on the read
// and cancel sides. Push is safe because it's single-producer and
// happens on disjoint heap internals (Push appends to items[] and
// sift-ups; Cancel and PopMin modify items[], cancel, and live).
// Actually — Push writes to items[] while Timer reads items[] — this is
// a data race UNLESS the heap has an internal lock on Push vs Peek.
// See the note at the bottom of this file. For Day 4's single-producer
// case, Gateway.Submit and Timer.Run are on the same "side" in the
// following sense: Push adds, Peek reads, PopMin removes. Under Go's
// memory model, they need synchronization.
//
// The chosen solution for Day 4: Push and PopMin are serialized by the
// same single mutex. This is a small change from the earlier lock-free
// design but it's the honest thing to do given that the timer and the
// gateway are on different goroutines. See the heap's SetMutexed docs.
type Timer struct {
	heap *TimeoutHeap
	out  chan<- TimeoutEvent

	// cancel receives IDs to remove from the heap. Drain.apply sends
	// to this channel on successful application, so timeouts for
	// already-applied transfers are dropped without firing.
	//
	// Buffered (typically to the ring capacity) so Drain.apply never
	// blocks on a slow timer. If the channel fills, Drain.apply
	// should drop the cancel and let the timeout fire — the drain
	// will see the transfer is already applied and skip it. That's a
	// correctness-preserving degradation, not a bug.
	cancel <-chan TransferID

	// clock returns nanoseconds since an arbitrary epoch. Injectable
	// for tests; time.Now().UnixNano in production.
	clock func() int64

	// idleInterval is how long the timer sleeps when the heap is empty.
	idleInterval time.Duration

	// maxSleep caps how long the timer will sleep on a far-future
	// expiry, so newly-pushed earlier expiries are noticed promptly.
	maxSleep time.Duration

	// wakeup is an optional signal from Submit. When the timer is
	// sleeping on a far-future expiry, Submit can nudge it to re-check
	// the root immediately. Optional: correctness does not depend on
	// it, only latency.
	wakeup <-chan struct{}
}

func NewTimer(
	heap *TimeoutHeap,
	out chan<- TimeoutEvent,
	cancel <-chan TransferID,
	clock func() int64,
	wakeup <-chan struct{},
) *Timer {
	if clock == nil {
		clock = func() int64 { return time.Now().UnixNano() }
	}
	return &Timer{
		heap:         heap,
		out:          out,
		cancel:       cancel,
		clock:        clock,
		idleInterval: 100 * time.Microsecond,
		maxSleep:     time.Millisecond,
		wakeup:       wakeup,
	}
}

// Run is the timer loop. Blocks until ctx is cancelled.
func (t *Timer) Run(ctx context.Context) {
	for {
		// 1. Check for cancellation from the drain first, so heap.Cancel
		//    happens on this goroutine (never on Drain.apply's).
		select {
		case id := <-t.cancel:
			t.heap.Cancel(id)
			continue
		case <-ctx.Done():
			return
		default:
		}

		// 2. Peek. O(1) when the root is live; O(k) to skip k stale
		//    roots after a batch of cancels.
		_, exp, ok := t.heap.Peek()
		if !ok {
			// Heap empty. Sleep briefly, but wake up to check the
			// cancel channel.
			if !t.sleep(ctx, t.idleInterval) {
				return
			}
			continue
		}

		now := t.clock()
		if exp > now {
			d := time.Duration(exp - now)
			if d > t.maxSleep {
				d = t.maxSleep
			}
			if !t.sleep(ctx, d) {
				return
			}
			continue
		}

		// 3. Expired. Pop and emit.
		id, poppedExp, poppedOK := t.heap.PopMin()
		if !poppedOK {
			// Cancel emptied the heap between Peek and PopMin.
			continue
		}
		select {
		case t.out <- TimeoutEvent{
			ID:         id,
			ExpiryNano: poppedExp,
			FiredNano:  t.clock(),
		}:
		case <-ctx.Done():
			return
		}
	}
}

// sleep waits for d (or a cancel/wakeup arrives), returning false on
// ctx cancellation.
func (t *Timer) sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	if t.wakeup != nil {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		case <-t.wakeup:
			return true
		case id := <-t.cancel:
			// Process the cancel and loop. We can't push it back;
			// the loop's top-of-iteration check would drain it anyway.
			// Calling Cancel here means we don't drop it.
			t.heap.Cancel(id)
			return true
		}
	}
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case id := <-t.cancel:
		t.heap.Cancel(id)
		return true
	}
}

// Concurrency note
//
// The heap's public methods (Push, Cancel, Peek, PopMin) are NOT
// safe for concurrent use with each other, because they all mutate
// or read the same internal state (items, live, cancel) with no
// synchronization. Under the Day-4 design:
//
//   - Gateway.Submit calls Push.
//   - Timer.Run calls Cancel, Peek, PopMin.
//
// Gateway.Submit and Timer.Run are different goroutines. So there IS
// a data race: Submit's Push writes to items[] and live while Timer's
// Peek reads them.
//
// For Day 4, the pragmatic fix is to add a mutex to TimeoutHeap and
// have Push, Cancel, Peek, PopMin each lock it. This is NOT the
// lock-free design from Days 1-2, but the heap is not on the same
// hot path as the ring: Peek is ~25ns, Push is ~120ns, and the mutex
// on a single-core-contended word is ~15ns. The lock adds ~10-15%
// overhead to heap ops, which is acceptable for a structure that's
// touched once per transfer rather than once per field.
//
// Day 5+ can revisit this with a truly lock-free heap if the mutex
// shows up in profiles. For now, correctness first.

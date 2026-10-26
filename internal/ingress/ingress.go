package ingress

import (
	"errors"
	"runtime"
	"sync/atomic"
	"time"

	"Axiom-Core/internal/executor"
	"Axiom-Core/pkg/model"
)

// Errors returned by Submit. The HTTP/RPC layer maps these to status
// codes; they are not for internal logging (log the wrapped error).
var (
	// ErrUnbalanced means the transfer violated the double-entry
	// invariant (DebitAmount != CreditAmount, or either negative).
	// The transfer is rejected before reaching the ring — the
	// invariant from Day 1 is enforced HERE, not in the drain.
	ErrUnbalanced = errors.New("transfer violates double-entry invariant")

	// ErrBackpressure means the ring was full for the configured
	// number of spin attempts. The caller should retry with backoff,
	// return 503, or drop depending on policy.
	ErrBackpressure = errors.New("ring is full; try again later")
)

// Gateway is the ingress boundary. It owns the idempotency index and
// the ring, and serializes submissions through the single-producer
// fast path (PushSP).
//
// The Gateway is NOT safe for concurrent use. It must be called from a
// single goroutine (one HTTP handler's request loop, one RPC stream,
// etc.). If you need concurrent ingress, run N Gateways each with their
// own ring, or wrap the Gateway in a mutex — but the latter defeats
// the point of the SP ring.
type Gateway struct {
	index       *executor.IdempotencyIndex
	ring        *Ring
	seq         atomic.Uint64 // logical transfer sequence, monotonic
	heap        *executor.TimeoutHeap
	now         func() int64 // injectable clock for tests
	timeoutNano int64        // deadline offset from submit time

	// maxSpins bounds how long Submit will spin when the ring is
	// full before returning ErrBackpressure. 64 matches the ring's
	// internal backoff threshold; going beyond that means the ring
	// really is saturated and the caller should back off.
	maxSpins int
	wakeup   chan<- struct{} // optional; nudges the Timer
}

// GatewayConfig bundles the wiring for NewGatewayWithTimeout.
type GatewayConfig struct {
	Index       *executor.IdempotencyIndex
	Ring        *Ring
	Heap        *executor.TimeoutHeap
	TimeoutNano int64
	Now         func() int64 // nil = time.Now().UnixNano
	Wakeup      chan<- struct{}
}

// NewGateway wires an index and a ring into a boundary. The ring must
// be constructed with NewRingSP, since Submit calls PushSP.
func NewGateway(index *executor.IdempotencyIndex, ring *Ring) *Gateway {
	return &Gateway{index: index, ring: ring, maxSpins: 64}
}

// NewGatewayWithTimeout is the Day-4 constructor: Submit pushes to the
// heap after a successful ring push, so the transfer's timeout starts
// ticking when it's enqueued.
func NewGatewayWithTimeout(cfg GatewayConfig) *Gateway {
	now := cfg.Now
	if now == nil {
		now = func() int64 { return time.Now().UnixNano() }
	}
	return &Gateway{
		index:       cfg.Index,
		ring:        cfg.Ring,
		heap:        cfg.Heap,
		now:         now,
		timeoutNano: cfg.TimeoutNano,
		maxSpins:    64,
		wakeup:      cfg.Wakeup,
	}
}

// Submit validates a transfer and, if valid and not already seen,
// pushes it into the ring. It returns the logical sequence assigned to
// the transfer (or the sequence assigned when it was first seen, on
// replay) and a nil error on success.
//
// Idempotency semantics: at-most-once.
//
// The transfer ID is recorded in the index BEFORE the ring push. If
// the process crashes between Record and PushSP, the ID is marked but
// the transfer was never enqueued — subsequent retries of the same ID
// return the recorded sequence and are not re-enqueued. This is the
// correct failure mode for a ledger: a dropped transfer is recoverable
// (the client can re-submit with a new ID after reconciliation), but a
// double-applied transfer is not.
//
// Concurrency: Submit is single-producer. Callers must not invoke it
// concurrently. This matches the ring's PushSP contract.
func (g *Gateway) Submit(t model.Transfer) (uint64, error) {
	// 1. Validate the double-entry invariant BEFORE doing anything
	//    observable. An invalid transfer never reaches the ring and
	//    never occupies a slot in the index. This is the place where
	//    Day 1's layout-level invariant becomes a runtime guarantee.
	if !t.Balanced() {
		return 0, ErrUnbalanced
	}

	// 2. Reserve a logical sequence for this submission. Two
	//    submissions of the same ID get different seqs; the first one
	//    wins (Record returns the first seq on replay). That's fine —
	//    the seq is a tie-breaker for ordering, not an identity.
	seq := g.seq.Add(1)

	// 3. Try to record the ID. If it's already present, this is a
	//    replay: return the ORIGINAL seq (the one recorded when the
	//    transfer was first accepted), not the one we just generated.
	//    That's what makes retries idempotent — the client sees the
	//    same seq on every replay of the same transfer.
	if existing, inserted := g.index.Record(t.ID, seq); !inserted {
		return existing, nil
	}

	// 4. From here on, the ID is recorded. If anything fails, the
	//    transfer is effectively "lost" from the caller's perspective
	//    (at-most-once). We do NOT undo the record — undoing it would
	//    reintroduce the double-apply window.
	//
	// Push with a bounded spin. If the ring is genuinely full, return
	// ErrBackpressure and let the caller decide. Do not spin forever:
	// that's the difference between a slow request and a hung service.
	for !g.ring.PushSP(t, g.maxSpins) {
		// PushSP returns false only when maxSpins is exceeded. On
		// failure, we're in a bad state: the ID is recorded but the
		// transfer wasn't enqueued. The caller sees ErrBackpressure
		// and should not retry the same ID (it's recorded) — it
		// should surface the failure. This is documented at-most-once.

		return seq, ErrBackpressure
	}

	// register a timeout. Only after the ring accepted the
	// transfer, so a backpressure-rejected transfer has no timeout.
	if g.heap != nil {
		g.heap.Push(t.ID, g.now()+g.timeoutNano)
		if g.wakeup != nil {
			select {
			case g.wakeup <- struct{}{}:
			default:
			}
		}
	}

	return seq, nil
}

// Len returns the index size. Useful for monitoring.
func (g *Gateway) Len() int { return g.index.Len() }

// Cap returns the ring capacity.
func (g *Gateway) Cap() int { return g.ring.Cap() }

// SpinHint is exported so tests can force the backpressure path
// without relying on timing. Do not use in production.
func (g *Gateway) setMaxSpinsForTest(n int) { g.maxSpins = n }

// runtime reference: PushSP uses runtime.Gosched internally on the
// spin path. This import is kept so future changes to Submit that
// need explicit yielding don't have to re-add it.
var _ = runtime.Gosched

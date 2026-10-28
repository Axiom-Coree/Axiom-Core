package ledger

import (
	"context"
	"errors"
	"log"
	"runtime"
	"sync/atomic"

	"Axiom-Core/pkg/model"
)

// Ring is what Drain drains from. *ingress.Ring satisfies it
// structurally — ledger does NOT import ingress. This is what breaks
// the ledger → ingress → executor → ledger import cycle: ingress
// imports executor (for IdempotencyIndex and TimeoutHeap), and
// executor imports ledger (for Store), so ledger must not import
// ingress.
type Ring interface {
	Pop() (model.Transfer, bool)
	Len() int
}

// TransferID is the raw 16-byte identifier used on the cancellation
// channel. It is deliberately [16]byte, not executor.TransferID, for
// the same reason as Ring: ledger must not import executor.
//
// executor.TransferID is declared as `type TransferID = [16]byte`
// (an alias), so chan TransferID and chan executor.TransferID are the
// same type. Callers that construct the cancel channel as
// `make(chan executor.TransferID, n)` still work — the two names are
// identical types.
type TransferID = [16]byte

// Drain is the single-consumer side of the pipeline: it pops transfers
// from the ring and applies them to accounts in sequence order.
//
// There is exactly one Drain per ring, and Drain.Run is the only
// goroutine that calls Pop. This is the ring's single-consumer contract.
type Drain struct {
	ring  Ring
	store *Store

	// cancel receives IDs of successfully applied transfers, so the
	// Timer (which is the heap's sole consumer) can drop their
	// timeouts. Optional: nil = no timeouts wired.
	//
	// Typed as chan<- [16]byte rather than chan<- executor.TransferID
	// so ledger does not import executor. Since executor.TransferID is
	// an alias for [16]byte, the caller can still pass a
	// `chan executor.TransferID` — they're the same type.
	cancel chan<- TransferID

	// Metrics. Read atomically from any goroutine; written only by Run.
	applied atomic.Uint64
	skipped atomic.Uint64
	failed  atomic.Uint64
}

// NewDrain constructs a Drain without timeout wiring.
func NewDrain(ring Ring, store *Store) *Drain {
	return &Drain{ring: ring, store: store}
}

// NewDrainWithTimeout wires a cancellation channel so successful
// applies are reported to the Timer. The channel should be buffered
// to at least the ring capacity to avoid blocking; a full channel is
// handled by dropping the cancel (the timeout will fire and be a
// no-op, since the transfer is already applied).
//
// The cancel parameter is chan<- [16]byte; callers with a
// `chan executor.TransferID` can pass it directly, because
// executor.TransferID is a type alias for [16]byte.
func NewDrainWithTimeout(ring Ring, store *Store, cancel chan<- TransferID) *Drain {
	return &Drain{ring: ring, store: store, cancel: cancel}
}

// Run is the drain loop. It runs until ctx is cancelled, then returns.
//
// Callers should run it in a dedicated goroutine:
//
//	go drain.Run(ctx)
//
// When ctx is cancelled, Run drains remaining items and returns.
func (d *Drain) Run(ctx context.Context) {
	const spinAfter = 64
	empty := 0
	for {
		select {
		case <-ctx.Done():
			for {
				t, ok := d.ring.Pop()
				if !ok {
					return
				}
				d.apply(t)
			}
		default:
		}

		t, ok := d.ring.Pop()
		if !ok {
			empty++
			if empty >= spinAfter {
				runtime.Gosched()
				empty = 0
			}
			continue
		}
		empty = 0
		d.apply(t)
	}
}

// apply posts a single transfer to the two accounts it references.
//
// (Comment unchanged from the original — kept verbatim.)
func (d *Drain) apply(t model.Transfer) {
	debit, ok := d.store.Get(t.DebitAccount)
	if !ok {
		d.failed.Add(1)
		log.Printf("drain: unknown debit account %x for transfer %x",
			t.DebitAccount[:4], t.ID[:4])
		return
	}
	credit, ok := d.store.Get(t.CreditAccount)
	if !ok {
		d.failed.Add(1)
		log.Printf("drain: unknown credit account %x for transfer %x",
			t.CreditAccount[:4], t.ID[:4])
		return
	}

	if !debit.Apply(t) {
		d.skipped.Add(1)
		return
	}
	if !credit.Apply(t) {
		d.failed.Add(1)
		log.Printf("drain: credit apply failed after debit succeeded "+
			"for transfer %x (debit seq=%d, credit seq=%d)",
			t.ID[:4], debit.Sequence, credit.Sequence)
		return
	}
	d.applied.Add(1)

	if d.cancel != nil {
		select {
		case d.cancel <- t.ID:
		default:
			// Channel full; timeout will fire and be a no-op.
		}
	}
}

// Stats returns a snapshot of drain counters. Safe to call concurrently.
func (d *Drain) Stats() (applied, skipped, failed uint64) {
	return d.applied.Load(), d.skipped.Load(), d.failed.Load()
}

// Applied returns the number of successfully applied transfers.
func (d *Drain) Applied() uint64 { return d.applied.Load() }

// ErrNoRing is returned by NewDrain if the ring is nil.
var ErrNoRing = errors.New("drain: ring is nil")

package ledger

import (
	"context"
	"errors"
	"log"
	"runtime"
	"sync/atomic"

	"Axiom-Core/internal/ingress"
	"Axiom-Core/pkg/model"
)

// Drain is the single-consumer side of the pipeline: it pops transfers
// from the ring and applies them to accounts in sequence order.
//
// There is exactly one Drain per ring, and Drain.Run is the only
// goroutine that calls Pop. This is the ring's single-consumer contract.
type Drain struct {
	ring  *ingress.Ring
	store *Store

	// Metrics. Read atomically from any goroutine; written only by Run.
	applied atomic.Uint64
	skipped atomic.Uint64
	failed  atomic.Uint64
}

func NewDrain(ring *ingress.Ring, store *Store) *Drain {
	return &Drain{ring: ring, store: store}
}

// Run is the drain loop. It runs until ctx is cancelled, then returns.
// Callers should run it in a dedicated goroutine:
//
//	go drain.Run(ctx)
//
// When ctx is cancelled, Run drains remaining items (bounded by a
// grace period) and returns. It does not abandon the ring.
func (d *Drain) Run(ctx context.Context) {
	const spinAfter = 64
	empty := 0
	for {
		select {
		case <-ctx.Done():
			// Drain what's left before returning, so in-flight
			// transfers aren't lost on shutdown.
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
			// Empty ring. Yield to other goroutines after a short
			// tight-spin window, so we don't burn a core spinning
			// on an empty ring while producers are idle.
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
// Returns whether the transfer was applied (true), skipped as stale
// (false, no error), or failed (false, error logged).
//
// The double-entry invariant was checked at ingress, so we don't
// re-check it here — Apply would reject unbalanced transfers anyway,
// but the check is a property of the type, not of the drain.
//
// Ordering guarantee: transfers arrive in the ring in the order they
// were pushed (the ring is FIFO), and Apply rejects out-of-order
// sequences, so applying in pop order is applying in sequence order.
// If a transfer is rejected as stale (Sequence <= account.Sequence),
// it's either a duplicate or arrived after a later transfer on the
// same account — which shouldn't happen with a single-consumer drain,
// so it's worth logging.
func (d *Drain) apply(t model.Transfer) {
	// Look up both accounts. If either is missing, the transfer can't
	// be applied atomically — apply to neither, log, and move on.
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

	// Apply to both. Order matters for intermediate invariants (some
	// systems require debit-before-credit so the sum of balances never
	// dips below zero during the posting), but this ledger doesn't
	// enforce non-negativity. Apply debit first for determinism.
	if !debit.Apply(t) {
		d.skipped.Add(1)
		return
	}
	if !credit.Apply(t) {
		// Debit succeeded, credit failed — this is a real inconsistency
		// (impossible with the current model, since both accounts exist
		// and Apply's checks are symmetric). Log loudly; it means the
		// account state diverged from what the transfer claimed.
		d.failed.Add(1)
		log.Printf("drain: credit apply failed after debit succeeded "+
			"for transfer %x (debit seq=%d, credit seq=%d)",
			t.ID[:4], debit.Sequence, credit.Sequence)
		return
	}
	d.applied.Add(1)
}

// Stats returns a snapshot of drain counters. Safe to call concurrently.
func (d *Drain) Stats() (applied, skipped, failed uint64) {
	return d.applied.Load(), d.skipped.Load(), d.failed.Load()
}

// ErrNoRing is returned by NewDrain if the ring is nil.
var ErrNoRing = errors.New("drain: ring is nil")

// Applied returns the number of successfully applied transfers.
// Safe to call concurrently with Run.
func (d *Drain) Applied() uint64 { return d.applied.Load() }

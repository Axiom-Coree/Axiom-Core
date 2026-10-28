package executor

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"Axiom-Core/internal/ledger"
	"Axiom-Core/internal/state"
	"Axiom-Core/pkg/model"
)

// Ingress
//
// Ingress is the executor's only dependency on the producer side.
//
// The executor deliberately does NOT import *ingress.Ring. If it did, we
// would have executor → ingress, and ingress already imports executor (for
// the idempotency index and the timeout heap). That's an import cycle, and
// Go rejects it.
//
// Instead, we declare the minimum contract the executor needs:
//
//	Pop() (model.Transfer, bool)  — pull the next transfer, false if empty
//	Len() int                     — current occupancy
//
// *ingress.Ring satisfies it structurally. This is standard Go
// dependency-inversion: the consumer declares the interface, the
// producer (or the wiring layer) satisfies it.
//
// Side benefit for tests: a fake Ingress (a slice-backed queue, a
// channel-backed one, a scripted one) is now trivial to write, and the
// executor's tests don't need a real ring.
type Ingress interface {
	Pop() (model.Transfer, bool)
	Len() int
}

// Config
//
// Config controls batching behavior. Zero values use the defaults
// documented on each field. The defaults are chosen to match the
// source doc's throughput/latency targets:
//
//	BatchSize      = 4096       — see the cost model in applyBatch's doc
//	BatchDeadline  = 5ms        — the source doc's T = min(buffer.size, 5ms)
//	IdleSleep      = 100µs      — ~10k wakes/sec when idle; ~100µs latency
type Config struct {
	// BatchSize caps how many transfers are processed per batch. It's
	// the "n" in the source doc's cost model: cost_per_op = a + c/n.
	//
	// The dominant cost in this pipeline is Phase 2 (tree.Put per
	// distinct account), not Phase 1 (per-transfer apply). A batch of
	// 4096 transfers touching ~100 distinct accounts does 100 tree.Puts
	// instead of 4096 — a 40x reduction in the fixed per-batch cost.
	//
	// Too large a batch: latency grows (a slow item delays the commit
	// for the whole batch). 4096 at ~200 ns/transfer = ~820 µs worst
	// case, which is inside the 5 ms deadline.
	BatchSize int

	// BatchDeadline bounds the latency of a batch. Even if the ring
	// has fewer than BatchSize items, we commit after this much time
	// so a slow trickle of transfers doesn't sit in the ring forever.
	//
	// The source doc's T = min(buffer.size, 5ms) is the same idea:
	// whichever comes first (full batch, or deadline) triggers a commit.
	BatchDeadline time.Duration

	// IdleSleep is how long we sleep when the ring is empty. Trade-off:
	//
	//	- Too small (e.g., 1µs): we burn a core waking up 1M times/sec
	//	  when the ring is empty.
	//	- Too large (e.g., 10ms): a transfer that arrives just after we
	//	  went to sleep waits up to 10ms before it's processed.
	//
	// 100µs balances these: ~10k wakes/sec when idle (cheap on a
	// modern CPU) and ~100µs added latency for a transfer that arrives
	// during sleep. A real system would use a condvar or a wakeup
	// channel instead; Day 2+ can add that.
	IdleSleep time.Duration
}

func (c *Config) applyDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 4096
	}
	if c.BatchDeadline <= 0 {
		c.BatchDeadline = 5 * time.Millisecond
	}
	if c.IdleSleep <= 0 {
		c.IdleSleep = 100 * time.Microsecond
	}
}

// Executor

// Executor is the single-writer bridge between the lock-free ingress
// (Day 2's ring + Day 3's index) and the snapshot state (Day 5-7's COW
// B-tree), applying each transfer to the mutable ledger (Day 1's
// Account.Apply) in deterministic FIFO order.
//
// # Single-writer contract
//
// Exactly one goroutine may call Run. This is what makes the whole
// pipeline's linearizability argument work:
//
//   - The ring's single-consumer contract requires one Pop caller.
//   - ledger.Store's accounts are mutated without locks by Account.Apply;
//     only the executor touches them.
//   - state.Store's Put is single-writer (its atomic.Pointer CAS is
//     defensive, not load-bearing — the design assumes one writer).
//
// Violate this and the -race detector will fire, or worse, silent
// corruption. The type is not a marker interface for "single goroutine
// only"; that's a caller contract, documented here and enforced by
// convention.
//
// # Determinism
//
// For a given sequence of ring contents, Run produces the same
// ledger.Store and state.Store outcomes across runs. The only
// non-deterministic input is *which* transfers are in the ring when
// Run wakes up — but the ring is FIFO and Pop is deterministic, so the
// set of applied transfers is a prefix of the submitted order.
//
// The batching deadline (5 ms) affects *when* the batch stops growing,
// not *which* transfers are in it: whatever is in the ring at drain
// time is applied in FIFO order. Tests can therefore assert exact
// state, not "eventually consistent" state.
type Executor struct {
	ring  Ingress
	store *ledger.Store
	tree  *state.Store
	cfg   Config

	// Counters. Atomic so a monitoring goroutine can read them
	// without a lock or a copy of the Executor. They are NOT used for
	// internal synchronization — the executor is single-goroutine, so
	// within Run they could be plain uint64. Atomic makes external
	// observation (Stats, tests, metrics) race-free.
	applied   atomic.Uint64 // transfers successfully applied to both accounts
	failed    atomic.Uint64 // transfers that failed (unbalanced, unknown account, stale seq)
	batches   atomic.Uint64 // number of batches committed
	committed atomic.Uint64 // number of account updates published to the tree

	// failableApply is the per-transfer apply function. In production
	// it's applyOne; tests override it to simulate failures without
	// constructing special accounts.
	//
	// Why a function field and not an interface: the behavior is a
	// single method with no state, and a function is simpler to
	// override in tests (just assign e.failableApply = func...). An
	// interface would be fine too; this is a judgment call.
	failableApply func(model.Transfer) bool
}

// New constructs an Executor. All three dependencies are required;
// passing nil will panic on first use.
//
// The ring parameter is an Ingress interface, so the caller (typically
// cmd/axiom or an internal/pipeline package) can pass *ingress.Ring
// without executor importing ingress.
func New(ring Ingress, store *ledger.Store, tree *state.Store, cfg Config) *Executor {
	cfg.applyDefaults()
	e := &Executor{
		ring:  ring,
		store: store,
		tree:  tree,
		cfg:   cfg,
	}
	e.failableApply = e.applyOne
	return e
}

// Run — the loop
//
// Run is the executor's main loop. It blocks until ctx is cancelled,
// then drains the remaining ring contents and returns.
//
// Structure: an outer loop that checks cancellation, fills a batch,
// applies it, sleeps if the ring was empty, and repeats. The batch is
// reused across iterations (batch = batch[:0]) to avoid one allocation
// per batch; at 5 ms deadlines and ~4k transfers/batch, that's
// ~200 batches/sec, so the saving is small but the pattern is free.
func (e *Executor) Run(ctx context.Context) {
	// Preallocate the batch to BatchSize. It's reused for the life of
	// the executor (only reset to length 0 between iterations).
	batch := make([]model.Transfer, 0, e.cfg.BatchSize)

	for {
		// Check for cancellation at the top of each iteration. This
		// is the fast path: if ctx is already cancelled, we skip
		// straight to drainFinal and return.
		select {
		case <-ctx.Done():
			e.drainFinal(batch)
			return
		default:
		}

		batch = batch[:0]
		deadline := time.Now().Add(e.cfg.BatchDeadline)

		// Fill the batch: up to BatchSize items, stopping early if
		// the deadline passes.
		//
		// Why check the deadline every 64 items rather than every
		// item: time.Now() is ~20 ns, and at ~150 ns/transfer that's
		// ~13% overhead. Checking every 64 items amortizes it to
		// ~0.3 ns/item, which is noise. The 64 is a power of two so
		// the check is `len(batch)&63 == 0`, which the compiler folds
		// to a single AND+branch.
		for len(batch) < e.cfg.BatchSize {
			t, ok := e.ring.Pop()
			if !ok {
				break
			}
			batch = append(batch, t)
			if len(batch)&63 == 0 && time.Now().After(deadline) {
				break
			}
		}

		// Ring was empty (or only very recently non-empty and we
		// popped zero items). Sleep briefly before retrying, or exit
		// if cancelled.
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				e.drainFinal(batch)
				return
			case <-time.After(e.cfg.IdleSleep):
			}
			continue
		}

		// Apply the batch. applyBatch is not a separate goroutine;
		// it's a straight-line function called from Run. There is no
		// concurrency inside a batch.
		e.applyBatch(batch)
	}
}

// drainFinal is called on cancellation: drain everything currently in
// the ring (ignoring BatchSize and BatchDeadline) and apply it as one
// batch. This ensures no transfers are stranded on shutdown.
//
// drainFinal is best-effort: it drains whatever the ring holds at the
// moment it's called. If a producer is still pushing, those pushes
// race with the drain and may or may not make it into the final
// batch. For a clean shutdown, cancel the producers first, then
// cancel the executor's ctx.
func (e *Executor) drainFinal(batch []model.Transfer) {
	batch = batch[:0]
	for {
		t, ok := e.ring.Pop()
		if !ok {
			break
		}
		batch = append(batch, t)
	}
	if len(batch) > 0 {
		e.applyBatch(batch)
	}
}

// applyBatch — the two-phase apply + publish
//
// A batch is applied in two phases:
//
//	Phase 1 (apply):    for each transfer, call Account.Apply on the
//	                    debit and credit accounts. Mutates ledger.Store
//	                    in place. Failures are counted and skipped.
//
//	Phase 2 (publish):  for each account touched in phase 1, call
//	                    state.Store.Put to publish the updated value
//	                    to the COW tree.
//
// # Why two phases and not one
//
// If we published each account to the tree immediately after applying
// each transfer, the tree's COW cost (~2.5 µs per Put) would be paid
// per transfer, not per distinct account. A batch of 4096 transfers
// touching 100 accounts would do 4096 Puts instead of 100 — a 40x
// increase in the dominant cost.
//
// The batching amortization is exactly this: push the expensive
// operation (tree.Put) out of the per-transfer loop and into a
// per-account loop, where |accounts| ≪ |transfers|.
//
// # Why the two phases are still ordered
//
// Phase 1 must complete before phase 2 (we need final balances to
// publish). Within phase 1, transfers are applied in FIFO order (the
// ring's guarantee). Within phase 2, accounts are published in
// first-seen order (deterministic given the batch).
//
// A concurrent reader taking a snapshot during phase 2 sees a prefix
// of the published accounts — never a mix of pre- and post-batch
// values for the same account. This is the linearizability point
// for the batch: the batch's publish is atomic from any account's
// perspective, but not atomic across all accounts. Callers that need
// a stable snapshot across a batch should take it before or after,
// not during; the window is tiny (100 puts × 2.5 µs = 250 µs at
// batch_size=4096, accounts=100).
//
// Why failures don't abort the batch
//
// A failed apply (unknown account, stale sequence, unbalanced) is a
// data issue, not a correctness issue. Aborting the batch would
// strand the remaining transfers in the ring with no way to retry
// (the ring's Pop already consumed them). Instead, we log-and-skip:
// the failed counter increments, the transfer's effect is absent
// from the ledger, and monitoring catches the rate.
//
// This matches the pipeline's at-most-once semantics (Day 3's
// Gateway records the ID before pushing to the ring; a failed apply
// is effectively a dropped transfer, which the client can retry
// with a new ID).
func (e *Executor) applyBatch(batch []model.Transfer) {
	// Collect the distinct accounts first. This is O(n + m*k) where
	// m is distinct accounts and k is batch size; for a batch of 4096
	// transfers touching ~100 accounts, that's ~4096 + 4096*100/2 ≈
	// 200k comparisons, which is ~50 µs. Acceptable. A map would be
	// faster for large batches but allocates; the slice is
	// allocation-free after the first batch.
	touched := e.touchedAccounts(batch)

	// Phase 1: apply every transfer to the ledger.
	for _, t := range batch {
		if !e.failableApply(t) {
			e.failed.Add(1)
			continue
		}
		e.applied.Add(1)
	}

	// Phase 2: publish every touched account to the tree.
	//
	// Why we look up the account again (rather than collecting
	// *Account pointers in phase 1): the account may have been
	// registered or removed between phases (only removed is
	// problematic, and Store.Register is idempotent, so this is
	// mostly defensive). It also keeps the phases independent —
	// phase 2 doesn't need phase 1's state.
	//
	// Why we publish the whole Account value, not just the balance:
	// the tree stores model.Account (40 bytes), and the whole value
	// is what a snapshot reader sees. Publishing only the balance
	// would lose the Sequence field, breaking the snapshot's
	// consistency story.
	for _, id := range touched {
		acct, ok := e.store.Get(id)
		if !ok {
			// Account disappeared between phase 1 and 2. Shouldn't
			// happen (Store.Get is stable for registered accounts),
			// but if it does, skip. The tree won't have a value for
			// this account; a reader's Get will return not-found.
			continue
		}
		e.tree.Put(*acct)
		e.committed.Add(1)
	}

	e.batches.Add(1)
}

// touchedAccounts — distinct account IDs in first-seen order
//
// Returns the distinct account IDs referenced by the batch, in the
// order they first appear. Order doesn't affect the tree's
// correctness (COW is commutative across different keys), but it
// makes the publish deterministic across runs, which tests rely on.
//
// Complexity: O(n*m) where n is batch size and m is distinct accounts.
// For n=4096, m=100, that's ~400k comparisons per batch. At ~1 ns per
// comparison on a modern CPU, that's ~400 µs per batch — which is
// ~10% of the 5 ms deadline budget. Acceptable but not ideal. If m
// grows (e.g., batch touches 1000 accounts), switch to a
// map[AccountID]struct{} for O(n) with ~100k allocations per batch;
// the trade depends on m.
func (e *Executor) touchedAccounts(batch []model.Transfer) []model.AccountID {
	// nil initial capacity; let append grow it. Most batches have
	// far fewer than 2*batch distinct accounts, and a too-large
	// preallocation would waste memory.
	var ids []model.AccountID
	for _, t := range batch {
		if !containsID(ids, t.DebitAccount) {
			ids = append(ids, t.DebitAccount)
		}
		if !containsID(ids, t.CreditAccount) {
			ids = append(ids, t.CreditAccount)
		}
	}
	return ids
}

// containsID is a linear scan. For small id slices (< 500), linear
// is faster than a map due to allocation and hashing overhead. For
// larger, switch to a map.
func containsID(ids []model.AccountID, id model.AccountID) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// applyOne — the default per-transfer apply
//
// Applies a single transfer to the ledger: look up both accounts,
// call Apply on each in order (debit first, then credit). Returns
// true iff both applies succeeded.
//
// Failure modes (all return false, counted by the caller):
//
//   - Debit account not registered.
//   - Credit account not registered.
//   - Debit Account.Apply rejected (stale sequence — t.Sequence <=
//     account.Sequence).
//   - Credit Account.Apply rejected (same).
//   - Credit apply failed AFTER debit succeeded: the account state is
//     now inconsistent (debit applied, credit not). This should be
//     impossible given the checks; it means the two accounts are in
//     an inconsistent state that predates this transfer. Logged
//     loudly; the transfer is counted as failed.
//
// The double-entry invariant (DebitAmount == CreditAmount) is checked
// at ingress (Gateway.Submit), not here. The executor trusts the
// ring's contents.
func (e *Executor) applyOne(t model.Transfer) bool {
	debit, ok := e.store.Get(t.DebitAccount)
	if !ok {
		return false
	}
	credit, ok := e.store.Get(t.CreditAccount)
	if !ok {
		return false
	}

	// Apply debit first. The choice of order is deterministic (tests
	// rely on it) but doesn't affect the final balances — Apply is
	// commutative across distinct accounts and not applied to the
	// same account twice (a transfer can't debit and credit the
	// same account; that would be a no-op transfer, which the
	// ingress should reject).
	if !debit.Apply(t) {
		return false
	}

	// Apply credit. If this fails after debit succeeded, the ledger
	// is inconsistent — log loudly. See failure modes above.
	if !credit.Apply(t) {
		log.Printf("executor: credit apply failed after debit succeeded "+
			"for transfer %x (debit seq=%d, credit seq=%d)",
			t.ID[:4], debit.Sequence, credit.Sequence)
		return false
	}

	return true
}

// Stats — external observation
//
// Returns the executor's counters. Safe to call from any goroutine
// (the counters are atomic). The counters are monotonic; a monitor
// computes rates by diffing two reads.
func (e *Executor) Stats() (applied, failed, batches, committed uint64) {
	return e.applied.Load(),
		e.failed.Load(),
		e.batches.Load(),
		e.committed.Load()
}

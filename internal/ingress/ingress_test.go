package ingress_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"Axiom-Core/internal/executor"
	"Axiom-Core/internal/ingress"
	"Axiom-Core/internal/ledger"
	"Axiom-Core/pkg/model"
)

func mkTransfer(id byte, debit, credit model.AccountID, amount int64, seq uint64) model.Transfer {
	var t model.Transfer
	t.ID[0] = id
	t.DebitAccount = debit
	t.CreditAccount = credit
	t.DebitAmount = model.Money(amount)
	t.CreditAmount = model.Money(amount)
	t.Sequence = seq
	return t
}

// Property: unbalanced transfers are rejected at the boundary.
func TestSubmitRejectsUnbalanced(t *testing.T) {
	idx := executor.NewIdempotencyIndex(1024)
	ring := ingress.NewRingSP(1024)
	gw := ingress.NewGateway(idx, ring)

	var a, b model.AccountID
	a[0], b[0] = 1, 2

	tf := mkTransfer(1, a, b, 100, 1)
	tf.CreditAmount = 99 // unbalanced

	if _, err := gw.Submit(tf); err != ingress.ErrUnbalanced {
		t.Fatalf("got %v, want ErrUnbalanced", err)
	}
	if idx.Len() != 0 {
		t.Fatalf("unbalanced transfer leaked into index (len=%d)", idx.Len())
	}
}

// Property: submitting the same ID twice returns the same seq, only
// one copy reaches the ring.
func TestSubmitIdempotent(t *testing.T) {
	idx := executor.NewIdempotencyIndex(1024)
	ring := ingress.NewRingSP(1024)
	gw := ingress.NewGateway(idx, ring)

	var a, b model.AccountID
	a[0], b[0] = 1, 2
	tf := mkTransfer(1, a, b, 100, 1)

	seq1, err := gw.Submit(tf)
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := gw.Submit(tf)
	if err != nil {
		t.Fatal(err)
	}
	if seq1 != seq2 {
		t.Fatalf("replay returned different seq: %d vs %d", seq1, seq2)
	}

	// Only one item in the ring.
	if got := ring.Len(); got != 1 {
		t.Fatalf("ring has %d items after replay, want 1", got)
	}
}

// Property: end-to-end — Submit -> ring -> Drain -> Account.Apply,
// balances match double-entry expectations.
func TestEndToEnd(t *testing.T) {
	idx := executor.NewIdempotencyIndex(1024)
	ring := ingress.NewRingSP(1024)
	gw := ingress.NewGateway(idx, ring)
	store := ledger.NewStore()

	var alice, bob model.AccountID
	alice[0], bob[0] = 1, 2
	store.Register(alice)
	store.Register(bob)

	drain := ledger.NewDrain(ring, store)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go drain.Run(ctx)

	// Submit 100 transfers, 10 from alice to bob.
	for i := 0; i < 100; i++ {
		tf := mkTransfer(byte(i), alice, bob, 10, uint64(i+1))
		if _, err := gw.Submit(tf); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for the drain to process everything.
	deadline := time.Now().Add(5 * time.Second)
	for ring.Len() > 0 || drain.Applied() < 100 {
		if time.Now().After(deadline) {
			t.Fatalf("drain didn't finish: ring=%d applied=%d",
				ring.Len(), drain.Applied())
		}
		runtime.Gosched()
		time.Sleep(time.Millisecond)
	}

	// Verify balances: alice debited 1000, bob credited 1000.
	// Note: Account.Apply ADDS DebitAmount to debit account's balance
	// and SUBTRACTS CreditAmount from credit account (from Day 1).
	// Alice (debit) balance should be +1000, Bob (credit) should be -1000.
	a, _ := store.Get(alice)
	b, _ := store.Get(bob)
	if a.Balance != 1000 {
		t.Fatalf("alice balance = %d, want 1000", a.Balance)
	}
	if b.Balance != -1000 {
		t.Fatalf("bob balance = %d, want -1000", b.Balance)
	}
}

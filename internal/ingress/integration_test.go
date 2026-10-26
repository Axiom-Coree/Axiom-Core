package ingress_test

import (
	"context"
	"testing"
	"time"

	"Axiom-Core/internal/executor"
	"Axiom-Core/internal/ingress"
	"Axiom-Core/internal/ledger"
	"Axiom-Core/pkg/model"
)

// mkTransferBoundary builds a Transfer for boundary tests.
func mkTransferBoundary(id byte, debit, credit model.AccountID, amount int64, seq uint64) model.Transfer {
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
//func TestSubmitRejectsUnbalanced(t *testing.T) {
//	idx := executor.NewIdempotencyIndex(1024)
//	ring := ingress.NewRingSP(1024)
//	gw := ingress.NewGateway(idx, ring)
//
//	var a, b model.AccountID
//	a[0], b[0] = 1, 2
//
//	tf := mkTransferBoundary(1, a, b, 100, 1)
//	tf.CreditAmount = 99 // unbalanced
//
//	if _, err := gw.Submit(tf); err != ingress.ErrUnbalanced {
//		t.Fatalf("got %v, want ErrUnbalanced", err)
//	}
//	if idx.Len() != 0 {
//		t.Fatalf("unbalanced transfer leaked into index (len=%d)", idx.Len())
//	}
//}

// Property: submitting the same ID twice returns the same seq, only
// one copy reaches the ring.
//func TestSubmitIdempotent(t *testing.T) {
//	idx := executor.NewIdempotencyIndex(1024)
//	ring := ingress.NewRingSP(1024)
//	gw := ingress.NewGateway(idx, ring)
//
//	var a, b model.AccountID
//	a[0], b[0] = 1, 2
//	tf := mkTransferBoundary(1, a, b, 100, 1)
//
//	seq1, err := gw.Submit(tf)
//	if err != nil {
//		t.Fatal(err)
//	}
//	seq2, err := gw.Submit(tf)
//	if err != nil {
//		t.Fatal(err)
//	}
//	if seq1 != seq2 {
//		t.Fatalf("replay returned different seq: %d vs %d", seq1, seq2)
//	}
//	if got := ring.Len(); got != 1 {
//		t.Fatalf("ring has %d items after replay, want 1", got)
//	}
//}

// Property: end-to-end without timeout. Submit -> ring -> Drain ->
// Account.Apply, balances match.
//func TestEndToEnd(t *testing.T) {
//	idx := executor.NewIdempotencyIndex(1024)
//	ring := ingress.NewRingSP(1024)
//	gw := ingress.NewGateway(idx, ring)
//	store := ledger.NewStore()
//
//	var alice, bob model.AccountID
//	alice[0], bob[0] = 1, 2
//	store.Register(alice)
//	store.Register(bob)
//
//	drain := ledger.NewDrain(ring, store)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	go drain.Run(ctx)
//
//	for i := 0; i < 100; i++ {
//		tf := mkTransferBoundary(byte(i), alice, bob, 10, uint64(i+1))
//		if _, err := gw.Submit(tf); err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	deadline := time.Now().Add(5 * time.Second)
//	for ring.Len() > 0 || drain.Applied() < 100 {
//		if time.Now().After(deadline) {
//			t.Fatalf("drain didn't finish: ring=%d applied=%d",
//				ring.Len(), drain.Applied())
//		}
//		runtime.Gosched()
//		time.Sleep(time.Millisecond)
//	}
//
//	a, _ := store.Get(alice)
//	b, _ := store.Get(bob)
//	if a.Balance != 1000 {
//		t.Fatalf("alice balance = %d, want 1000", a.Balance)
//	}
//	if b.Balance != -1000 {
//		t.Fatalf("bob balance = %d, want -1000", b.Balance)
//	}
//}

// Property: end-to-end with timeout wired. Every transfer applies
// before its deadline, the timer cancels everything, no TimeoutEvent
// fires, balances are correct.
func TestEndToEndWithTimeout(t *testing.T) {
	heap := executor.NewTimeoutHeap(1024)
	idx := executor.NewIdempotencyIndex(1024)
	ring := ingress.NewRingSP(1024)
	store := ledger.NewStore()

	var alice, bob model.AccountID
	alice[0], bob[0] = 1, 2
	store.Register(alice)
	store.Register(bob)

	// Wire the three goroutines.
	out := make(chan executor.TimeoutEvent, 1024)
	cancelCh := make(chan executor.TransferID, 1024)
	wakeup := make(chan struct{}, 1)

	timer := executor.NewTimer(heap, out, cancelCh, nil, wakeup)
	drain := ledger.NewDrainWithTimeout(ring, store, cancelCh)
	gw := ingress.NewGatewayWithTimeout(ingress.GatewayConfig{
		Index:       idx,
		Ring:        ring,
		Heap:        heap,
		TimeoutNano: int64(50 * time.Millisecond),
		Wakeup:      wakeup,
	})

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()
	go timer.Run(ctx)
	go drain.Run(ctx)

	for i := 0; i < 100; i++ {
		tf := mkTransferBoundary(byte(i), alice, bob, 10, uint64(i+1))
		if _, err := gw.Submit(tf); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for all 100 to be applied.
	deadline := time.Now().Add(2 * time.Second)
	for drain.Applied() < 100 {
		if time.Now().After(deadline) {
			t.Fatalf("drain stalled: applied=%d heap_live=%d",
				drain.Applied(), heap.Live())
		}
		time.Sleep(time.Millisecond)
	}

	// No spurious timeout should fire.
	select {
	case ev := <-out:
		t.Fatalf("timeout fired despite successful apply: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	// Balances correct.
	a, _ := store.Get(alice)
	b, _ := store.Get(bob)
	if a.Balance != 1000 || b.Balance != -1000 {
		t.Fatalf("balances: alice=%d bob=%d, want 1000/-1000", a.Balance, b.Balance)
	}

	// Every timeout was cancelled, so no live entries remain.
	if heap.Live() != 0 {
		t.Fatalf("heap.Live() = %d, want 0 after all cancels", heap.Live())
	}
}

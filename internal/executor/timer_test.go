package executor

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manual clock for tests.
type fakeClock struct {
	mu  sync.Mutex
	now int64
}

func (f *fakeClock) Now() int64      { f.mu.Lock(); defer f.mu.Unlock(); return f.now }
func (f *fakeClock) Set(n int64)     { f.mu.Lock(); f.now = n; f.mu.Unlock() }
func (f *fakeClock) Advance(d int64) { f.mu.Lock(); f.now += d; f.mu.Unlock() }

// runTimer starts the timer and returns a cancel func plus the event channel.
func runTimer(t *testing.T, heap *TimeoutHeap, clock *fakeClock) (<-chan TimeoutEvent, chan<- TransferID, context.CancelFunc) {
	t.Helper()
	out := make(chan TimeoutEvent, 1024)
	cancel := make(chan TransferID, 1024)
	timer := NewTimer(heap, out, cancel, clock.Now, nil)
	ctx, cancelFn := context.WithCancel(context.Background())
	go timer.Run(ctx)
	return out, cancel, cancelFn
}

func TestTimerFires(t *testing.T) {
	heap := NewTimeoutHeap(16)
	clock := &fakeClock{now: 1_000_000}
	out, _, cancelFn := runTimer(t, heap, clock)
	defer cancelFn()

	id := TransferID{1}
	heap.Push(id, clock.Now()+int64(time.Millisecond))

	// Nothing should fire yet.
	select {
	case ev := <-out:
		t.Fatalf("premature fire: %+v", ev)
	case <-time.After(5 * time.Millisecond):
	}

	clock.Advance(int64(2 * time.Millisecond))
	select {
	case ev := <-out:
		if ev.ID != id {
			t.Fatalf("wrong ID: %+v", ev)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("timer did not fire")
	}
}

func TestTimerCancel(t *testing.T) {
	heap := NewTimeoutHeap(16)
	clock := &fakeClock{now: 1_000_000}
	out, cancelCh, cancelFn := runTimer(t, heap, clock)
	defer cancelFn()

	id := TransferID{2}
	heap.Push(id, clock.Now()+int64(time.Millisecond))

	// Cancel before it fires.
	cancelCh <- id

	// Advance well past expiry.
	clock.Advance(int64(10 * time.Millisecond))

	select {
	case ev := <-out:
		t.Fatalf("fired after cancel: %+v", ev)
	case <-time.After(50 * time.Millisecond):
		// Correct: no event.
	}
}

func TestTimerDrains(t *testing.T) {
	heap := NewTimeoutHeap(128)
	clock := &fakeClock{now: 1_000_000}
	out, _, cancelFn := runTimer(t, heap, clock)
	defer cancelFn()

	const n = 100
	for i := 0; i < n; i++ {
		var id TransferID
		id[0] = byte(i)
		heap.Push(id, clock.Now()+int64(i+1)*1_000_000) // 1ms apart
	}

	// Collect all n events.
	got := make([]TimeoutEvent, 0, n)
	deadline := time.After(5 * time.Second)
	advance := time.NewTicker(2 * time.Millisecond)
	defer advance.Stop()
	for len(got) < n {
		select {
		case ev := <-out:
			got = append(got, ev)
		case <-advance.C:
			clock.Advance(int64(time.Millisecond))
		case <-deadline:
			t.Fatalf("only got %d/%d events", len(got), n)
		}
	}

	// Events must be in ascending ExpiryNano order.
	for i := 1; i < len(got); i++ {
		if got[i].ExpiryNano < got[i-1].ExpiryNano {
			t.Fatalf("out of order at %d: %d after %d", i, got[i].ExpiryNano, got[i-1].ExpiryNano)
		}
	}
}

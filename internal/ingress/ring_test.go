package ingress

//
//import (
//	"Axiom-Core/pkg/model"
//	"fmt"
//	"sync"
//	"sync/atomic"
//	"testing"
//	"time"
//)
//
//func mkTransfer(seq uint64) model.Transfer {
//	var t model.Transfer
//	t.Sequence = seq
//	return t
//}
//
//// Property 1: concurrent producers never overwrite an unconsumed slot.
//func TestNoOverwrite(t *testing.T) {
//	const cap = 8
//	r := NewRing(cap)
//	// Fill the ring to capacity.
//	for i := 0; i < cap; i++ {
//		if !r.Push(mkTransfer(uint64(i)), 1) {
//			t.Fatalf("push %d failed on empty ring", i)
//		}
//	}
//	// Next push must fail (ring full) within 1 spin.
//	if r.Push(mkTransfer(999), 1) {
//		t.Fatal("push succeeded on full ring")
//	}
//	// Pop one, then push must succeed.
//	if _, ok := r.Pop(); !ok {
//		t.Fatal("pop failed on full ring")
//	}
//	if !r.Push(mkTransfer(999), 1) {
//		t.Fatal("push failed after pop freed a slot")
//	}
//}
//
//// Property 2: consumer observes entries in producer order.
//func TestOrderSingleProducer(t *testing.T) {
//	r := NewRing(16)
//	const n = 1000
//	go func() {
//		for i := 0; i < n; i++ {
//			for !r.Push(mkTransfer(uint64(i)), -1) {
//			}
//		}
//	}()
//	for i := 0; i < n; i++ {
//		var tf model.Transfer
//		var ok bool
//		for {
//			tf, ok = r.Pop()
//			if ok {
//				break
//			}
//			time.Sleep(time.Microsecond)
//		}
//		if tf.Sequence != uint64(i) {
//			t.Fatalf("order violation: got %d want %d", tf.Sequence, i)
//		}
//	}
//}
//
//// Property 2b: multiple producers — each producer's items are in order,
//// and no item is lost or duplicated.
//func TestOrderMultiProducer(t *testing.T) {
//	r := NewRing(32)
//	const producers = 4
//	const perProducer = 500
//
//	var consumed atomic.Int64
//	var wg sync.WaitGroup
//	wg.Add(producers)
//	for p := 0; p < producers; p++ {
//		go func(pid uint64) {
//			defer wg.Done()
//			for i := 0; i < perProducer; i++ {
//				tf := mkTransfer(pid*10000 + uint64(i))
//				for !r.Push(tf, -1) {
//				}
//			}
//		}(uint64(p))
//	}
//
//	// Consumer: read exactly producers*perProducer items.
//	// Because multiple producers interleave, global order is not
//	// per-producer order across producers; we check per-producer
//	// monotonicity and total count.
//	lastByProducer := map[uint64]uint64{}
//	for consumed.Load() < producers*perProducer {
//		tf, ok := r.Pop()
//		if !ok {
//			time.Sleep(time.Microsecond)
//			continue
//		}
//		pid := tf.Sequence / 10000
//		idx := tf.Sequence % 10000
//		if prev, seen := lastByProducer[pid]; seen && idx <= prev {
//			t.Fatalf("producer %d out of order: %d after %d", pid, idx, prev)
//		}
//		lastByProducer[pid] = idx
//		consumed.Add(1)
//	}
//	wg.Wait()
//	if len(lastByProducer) != producers {
//		t.Fatalf("saw %d producers, want %d", len(lastByProducer), producers)
//	}
//}
//
//// Property 3: full and empty states are exact.
//func TestExactFullEmpty(t *testing.T) {
//	const cap = 4
//	r := NewRing(cap)
//	if r.Len() != 0 {
//		t.Fatalf("new ring len = %d, want 0", r.Len())
//	}
//	for i := 0; i < cap; i++ {
//		if !r.Push(mkTransfer(uint64(i)), 1) {
//			t.Fatalf("push %d failed", i)
//		}
//	}
//	if r.Len() != cap {
//		t.Fatalf("full ring len = %d, want %d", r.Len(), cap)
//	}
//	// Drain.
//	for i := 0; i < cap; i++ {
//		if _, ok := r.Pop(); !ok {
//			t.Fatalf("pop %d failed", i)
//		}
//	}
//	if r.Len() != 0 {
//		t.Fatalf("drained ring len = %d, want 0", r.Len())
//	}
//	if _, ok := r.Pop(); ok {
//		t.Fatal("pop succeeded on empty ring")
//	}
//}
//
//// Property 4: -race and varied GOMAXPROCS.
//// Run with: go test -race -run TestRace ./internal/ingress
//func TestRace(t *testing.T) {
//	if testing.Short() {
//		t.Skip("race test is slow")
//	}
//	r := NewRing(64)
//	const producers = 8
//	const perProducer = 2000
//	var wg sync.WaitGroup
//	wg.Add(producers)
//	for p := 0; p < producers; p++ {
//		go func() {
//			defer wg.Done()
//			for i := 0; i < perProducer; i++ {
//				for !r.Push(mkTransfer(1), -1) {
//				}
//			}
//		}()
//	}
//	var got int
//	for got < producers*perProducer {
//		if _, ok := r.Pop(); ok {
//			got++
//		}
//	}
//	wg.Wait()
//	if got != producers*perProducer {
//		t.Fatalf("consumed %d, want %d", got, producers*perProducer)
//	}
//}
//
//func BenchmarkPushPopSingleProducer(b *testing.B) {
//	r := NewRing(1024)
//	b.ReportAllocs()
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		r.Push(mkTransfer(uint64(i)), -1)
//		r.Pop()
//	}
//}
//
//func BenchmarkPushParallel(b *testing.B) {
//	for _, procs := range []int{1, 2, 4, 8} {
//		b.Run(fmt.Sprintf("procs=%d", procs), func(b *testing.B) {
//			r := NewRing(1 << 14)
//			var wg sync.WaitGroup
//			wg.Add(procs)
//			b.ResetTimer()
//			for p := 0; p < procs; p++ {
//				go func() {
//					defer wg.Done()
//					// Half of b.N per producer; total pushes == b.N
//					n := b.N / procs
//					for i := 0; i < n; i++ {
//						for !r.Push(mkTransfer(1), -1) {
//						}
//						if i&0xff == 0 {
//							// drain occasionally so producers don't
//							// just spin on a full ring — but Pop is
//							// single-consumer, so only one goroutine
//							// may pop. That's a benchmark structure
//							// problem; see note below.
//						}
//					}
//				}()
//			}
//			wg.Wait()
//		})
//	}
//}
//
//func BenchmarkThroughput(b *testing.B) {
//	for _, producers := range []int{1, 2, 4, 8} {
//		b.Run(fmt.Sprintf("producers=%d", producers), func(b *testing.B) {
//			r := NewRing(1 << 14)
//			var wg sync.WaitGroup
//			wg.Add(producers)
//			done := make(chan struct{})
//
//			go func() {
//				for {
//					select {
//					case <-done:
//						return
//					default:
//					}
//					r.Pop()
//				}
//			}()
//
//			b.ResetTimer()
//			per := b.N / producers
//			if per < 1 {
//				per = 1
//			}
//			for p := 0; p < producers; p++ {
//				go func() {
//					defer wg.Done()
//					for i := 0; i < per; i++ {
//						for !r.Push(mkTransfer(1), -1) {
//						}
//					}
//				}()
//			}
//			wg.Wait()
//			b.StopTimer()
//			close(done)
//		})
//	}
//}

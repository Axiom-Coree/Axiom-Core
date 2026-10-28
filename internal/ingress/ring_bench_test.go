package ingress

import (
	"testing"

	"Axiom-Core/pkg/model"
)

//func mkTransfer(seq uint64) model.Transfer {
//	var t model.Transfer
//	t.Sequence = seq
//	return t
//}

// Typed sinks. Assigning a Transfer to `any` boxes it (heap-allocates
// a 128-byte copy); assigning to a typed var does not.
var (
	sink         any
	sinkTransfer model.Transfer
)

func BenchmarkSingleProducer(b *testing.B) {
	b.Run("mpsc-path", func(b *testing.B) {
		r := NewRing(1024)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.Push(mkTransfer(uint64(i)), -1)
			r.Pop()
		}
	})
	b.Run("sp-path", func(b *testing.B) {
		r := NewRingSP(1024)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.PushSP(mkTransfer(uint64(i)), -1)
			r.Pop()
		}
	})
}

func BenchmarkPop128(b *testing.B) {
	r := NewRingSP(1024)
	for i := 0; i < 1024; i++ {
		r.PushSP(mkTransfer(uint64(i)), -1)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t, ok := r.Pop()
		if !ok {
			b.Fatal("unexpected empty")
		}
		sinkTransfer = t // typed — no boxing
		r.PushSP(mkTransfer(uint64(i)), -1)
	}
}

// BenchmarkThroughput measures end-to-end throughput with a dedicated
// consumer goroutine draining the ring. Producers push into the ring;
// the consumer pops as fast as it can. This is the contended case:
// producers and consumer run on different cores, and the shared
// head/published/tail words ping-pong between them.
//
// Compare mpsc vs sp sub-benchmarks: the SP path should be measurably
// faster because it avoids the head CAS.
//func BenchmarkThroughput(b *testing.B) {
//	for _, procs := range []int{1, 2, 4, 8} {
//		b.Run(fmt.Sprintf("mpsc/producers=%d", procs), func(b *testing.B) {
//			r := NewRing(1 << 14)
//			done := make(chan struct{})
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
//			var wg sync.WaitGroup
//			wg.Add(procs)
//			per := b.N / procs
//			if per < 1 {
//				per = 1
//			}
//			b.ResetTimer()
//			for p := 0; p < procs; p++ {
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
//	b.Run("sp/producers=1", func(b *testing.B) {
//		r := NewRingSP(1 << 14)
//		done := make(chan struct{})
//		go func() {
//			for {
//				select {
//				case <-done:
//					return
//				default:
//				}
//				r.Pop()
//			}
//		}()
//		b.ResetTimer()
//		for i := 0; i < b.N; i++ {
//			for !r.PushSP(mkTransfer(1), -1) {
//			}
//		}
//		b.StopTimer()
//		close(done)
//	})
//}

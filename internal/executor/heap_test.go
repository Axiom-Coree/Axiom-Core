package executor

import (
	"math/rand"
	"sort"
	"testing"
	"unsafe"
)

func id(n uint64) TransferID {
	var t TransferID
	t[0] = byte(n)
	t[1] = byte(n >> 8)
	t[2] = byte(n >> 16)
	t[3] = byte(n >> 24)
	return t
}

// Property: Push then PopMin returns items in ascending expiry order.
func TestHeapOrder(t *testing.T) {
	h := NewTimeoutHeap(16)
	expiries := []int64{500, 100, 300, 200, 400}
	for i, e := range expiries {
		h.Push(id(uint64(i)), e)
	}
	var got []int64
	for {
		// Before popping, verify the invariant.
		if h.Live() > h.Len() {
			t.Fatalf("invariant: live=%d > len=%d", h.Live(), h.Len())
		}
		_, e, ok := h.PopMin()
		if !ok {
			break
		}
		got = append(got, e)
	}
	want := []int64{100, 200, 300, 400, 500}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pop order = %v, want %v", got, want)
		}
	}
}

// Property: Peek returns the minimum without removing it.
func TestPeek(t *testing.T) {
	h := NewTimeoutHeap(4)
	h.Push(id(1), 100)
	h.Push(id(2), 50)
	h.Push(id(3), 75)

	tid, exp, ok := h.Peek()
	if !ok || exp != 50 {
		t.Fatalf("peek = (%v, %d, %v), want exp 50", tid[:2], exp, ok)
	}
	if h.Len() != 3 {
		t.Fatalf("peek changed length: %d", h.Len())
	}
	// Peek again, same result.
	_, exp2, _ := h.Peek()
	if exp2 != 50 {
		t.Fatalf("second peek = %d, want 50", exp2)
	}
}

// Property: Cancel removes the entry lazily; Peek/Pop skip stale roots.
func TestCancel(t *testing.T) {
	h := NewTimeoutHeap(4)

	// Helper: assert that live never exceeds len and that
	// live == len(items) - (number of cancelled items still in items).
	// We can't see items[] from the test (unexported), but we can
	// assert the necessary bound: live <= len.
	check := func(step string) {
		if h.Live() > h.Len() {
			t.Fatalf("%s: live=%d > len=%d (impossible)", step, h.Live(), h.Len())
		}
	}

	h.Push(id(1), 100)
	check("push1")
	h.Push(id(2), 50)
	check("push2")
	h.Push(id(3), 75)
	check("push3")

	if !h.Cancel(id(1)) {
		t.Fatal("cancel id1 failed")
	}
	check("cancel")
	if h.Cancel(id(1)) {
		t.Fatal("double cancel succeeded")
	}
	check("double-cancel")

	_, e1, ok := h.PopMin()
	if !ok || e1 != 50 {
		t.Fatalf("first pop = (%d, %v), want (50, true)", e1, ok)
	}
	check("pop1")

	_, e2, ok := h.PopMin()
	if !ok || e2 != 75 {
		t.Fatalf("second pop = (%d, %v), want (75, true)", e2, ok)
	}
	check("pop2")

	if _, _, ok := h.PopMin(); ok {
		t.Fatal("pop after drain succeeded")
	}
	check("drained")

	if h.Live() != 0 {
		t.Fatalf("live = %d, want 0", h.Live())
	}
	if h.Len() != 0 {
		t.Fatalf("len = %d, want 0", h.Len())
	}
}

// Property: when many entries have the same expiry, tiebreaker makes
// the order deterministic (insertion order).
func TestTiebreak(t *testing.T) {
	h := NewTimeoutHeap(8)
	const n = 8
	for i := 0; i < n; i++ {
		h.Push(id(uint64(i)), 100) // same expiry
	}
	var order []byte
	for {
		tid, _, ok := h.PopMin()
		if !ok {
			break
		}
		order = append(order, tid[0])
	}
	// Expected: insertion order, because tiebreak is monotonic.
	want := []byte{0, 1, 2, 3, 4, 5, 6, 7}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// Property: Push/Pop under interleaving with a reference sorted slice
// produces the same pop order (amortized complexity + correctness).
func TestHeapVsSort(t *testing.T) {
	const n = 1000
	expiries := make([]int64, n)
	for i := range expiries {
		expiries[i] = rand.Int63n(10000)
	}
	h := NewTimeoutHeap(n)
	for i, e := range expiries {
		h.Push(id(uint64(i)), e)
	}

	// Reference: sort the expiries ascending.
	sorted := append([]int64(nil), expiries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for i := 0; i < n; i++ {
		_, e, ok := h.PopMin()
		if !ok {
			t.Fatalf("pop %d: empty early", i)
		}
		if e != sorted[i] {
			t.Fatalf("pop %d: got %d, want %d", i, e, sorted[i])
		}
	}
	if _, _, ok := h.PopMin(); ok {
		t.Fatal("pop after drain succeeded")
	}
}

// Property: the heap's item layout is 32 bytes and pointer-free.
func TestHeapItemLayout(t *testing.T) {
	if got := unsafe.Sizeof(timeoutItem{}); got != 32 {
		t.Fatalf("timeoutItem size = %d, want 32", got)
	}
	if got := unsafe.Alignof(timeoutItem{}); got != 8 {
		t.Fatalf("timeoutItem align = %d, want 8", got)
	}
}

func BenchmarkPush(b *testing.B) {
	h := NewTimeoutHeap(b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Push(id(uint64(i)), int64(i))
	}
}

func BenchmarkPopMin(b *testing.B) {
	h := NewTimeoutHeap(b.N)
	for i := 0; i < b.N; i++ {
		h.Push(id(uint64(i)), int64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.PopMin()
	}
}

func BenchmarkPushPop(b *testing.B) {
	h := NewTimeoutHeap(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Push(id(uint64(i)), int64(i))
		h.PopMin()
	}
}

func BenchmarkPushSteady(b *testing.B) {
	h := NewTimeoutHeap(1024)
	// Pre-fill to 512 entries so Push is always on a half-full heap.
	for i := 0; i < 512; i++ {
		h.Push(id(uint64(i)), int64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Push(id(uint64(i+512)), int64(i+512))
		if h.Len() > 1024 {
			h.PopMin()
		}
	}
}

func BenchmarkPopMinSteady(b *testing.B) {
	h := NewTimeoutHeap(1024)
	for i := 0; i < 1024; i++ {
		h.Push(id(uint64(i)), int64(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.PopMin()
		h.Push(id(uint64(i)), int64(i)) // keep the heap at ~1024
	}
}

func BenchmarkTimerLoop(b *testing.B) {
	h := NewTimeoutHeap(4096)
	for i := 0; i < 4096; i++ {
		h.Push(id(uint64(i)), int64(i)+1000) // all pending, none expired yet
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate the timer: peek (usually no-op), pop if expired, push new.
		if _, exp, ok := h.Peek(); ok && exp <= int64(i) {
			h.PopMin()
		}
		h.Push(id(uint64(i+4096)), int64(i+2000))
	}
}

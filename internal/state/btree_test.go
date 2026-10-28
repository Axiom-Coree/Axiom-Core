package state

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"Axiom-Core/pkg/model"
)

// helpers

// mkID builds an AccountID with the given uint64 in the first 8 bytes.
// The remaining 8 bytes stay zero; for tests that's enough for 2^64
// distinct keys.
func mkID(n uint64) model.AccountID {
	var id model.AccountID
	binary.BigEndian.PutUint64(id[:8], n)
	return id
}

// mkAccount builds a model.Account with the given ID and balance.
func mkAccount(n uint64, balance model.Money) model.Account {
	return model.Account{ID: mkID(n), Balance: balance}
}

// ---- correctness tests ----

// Property: a single Put is visible to a subsequent Get.
func TestPutGet(t *testing.T) {
	s := NewStore()
	s.Put(mkAccount(1, 100))
	got, ok := s.Snapshot().Get(mkID(1))
	if !ok {
		t.Fatal("Get returned not-found for a key that was just put")
	}
	if got.Balance != 100 {
		t.Fatalf("Get returned balance %d, want 100", got.Balance)
	}
}

// Property: a Put updates an existing key (no duplicate).
func TestPutUpdates(t *testing.T) {
	s := NewStore()
	s.Put(mkAccount(1, 100))
	s.Put(mkAccount(1, 200))
	snap := s.Snapshot()
	got, ok := snap.Get(mkID(1))
	if !ok || got.Balance != 200 {
		t.Fatalf("Get = (%+v, %v), want balance 200", got, ok)
	}
	if n := snap.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1 (update, not insert)", n)
	}
}

// Property: a snapshot taken before a Put does not see the Put. This is
// the whole point of Day 5.
func TestSnapshotIsolation(t *testing.T) {
	s := NewStore()
	s.Put(mkAccount(1, 100))

	snapOld := s.Snapshot()

	// Two writes after the snapshot: update the same key, insert a new one.
	s.Put(mkAccount(1, 200))
	s.Put(mkAccount(2, 300))

	// Old snapshot: still sees balance 100, still has only key 1.
	if got, ok := snapOld.Get(mkID(1)); !ok || got.Balance != 100 {
		t.Fatalf("old snapshot Get(1) = (%+v, %v), want balance 100", got, ok)
	}
	if _, ok := snapOld.Get(mkID(2)); ok {
		t.Fatal("old snapshot sees key 2, which was inserted after the snapshot")
	}
	if n := snapOld.Len(); n != 1 {
		t.Fatalf("old snapshot Len = %d, want 1", n)
	}

	// New snapshot sees both.
	snapNew := s.Snapshot()
	if got, _ := snapNew.Get(mkID(1)); got.Balance != 200 {
		t.Fatalf("new snapshot Get(1) balance = %d, want 200", got.Balance)
	}
	if got, ok := snapNew.Get(mkID(2)); !ok || got.Balance != 300 {
		t.Fatalf("new snapshot Get(2) = (%+v, %v), want balance 300", got, ok)
	}
}

// Property: inserting more than maxKeys entries forces a root split,
// and every entry survives.
func TestSplitRoot(t *testing.T) {
	s := NewStore()
	const n = maxKeys + 1
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := s.Snapshot()
	if got := snap.Len(); got != n {
		t.Fatalf("Len = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		got, ok := snap.Get(mkID(uint64(i)))
		if !ok || got.Balance != model.Money(i) {
			t.Fatalf("Get(%d) = (%+v, %v), want balance %d", i, got, ok, i)
		}
	}
}

// Property: a large tree (many splits, 2+ levels deep) preserves all
// entries and their order.
func TestLargeTree(t *testing.T) {
	const n = 10_000
	s := NewStore()
	// Insert in a scrambled order to force splits all over the tree.
	rng := rand.New(rand.NewSource(42))
	order := rng.Perm(n)
	for _, i := range order {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := s.Snapshot()
	if got := snap.Len(); got != n {
		t.Fatalf("Len = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		got, ok := snap.Get(mkID(uint64(i)))
		if !ok || got.Balance != model.Money(i) {
			t.Fatalf("Get(%d) = (%+v, %v), want balance %d", i, got, ok, i)
		}
	}
}

// Property: Range visits every account in strictly ascending ID order.
func TestRangeOrdered(t *testing.T) {
	const n = 1000
	s := NewStore()
	// Insert scrambled.
	rng := rand.New(rand.NewSource(7))
	for _, i := range rng.Perm(n) {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := s.Snapshot()

	count := 0
	var prev model.AccountID
	first := true
	snap.Range(func(a model.Account) bool {
		if !first && !accountIDLess(prev, a.ID) {
			t.Fatalf("Range out of order: prev=%x current=%x", prev[:8], a.ID[:8])
		}
		prev = a.ID
		first = false
		count++
		return true
	})
	if count != n {
		t.Fatalf("Range visited %d, want %d", count, n)
	}
}

// Property: Range stops when the callback returns false.
func TestRangeStops(t *testing.T) {
	s := NewStore()
	for i := 0; i < 100; i++ {
		s.Put(mkAccount(uint64(i), 0))
	}
	count := 0
	s.Snapshot().Range(func(model.Account) bool {
		count++
		return count < 5
	})
	if count != 5 {
		t.Fatalf("Range visited %d, want 5", count)
	}
}

// Property: All returns every account sorted by ID.
func TestAll(t *testing.T) {
	const n = 500
	s := NewStore()
	rng := rand.New(rand.NewSource(1))
	for _, i := range rng.Perm(n) {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	all := s.Snapshot().All()
	if len(all) != n {
		t.Fatalf("All len = %d, want %d", len(all), n)
	}
	for i := 1; i < len(all); i++ {
		if !accountIDLess(all[i-1].ID, all[i].ID) {
			t.Fatalf("All out of order at %d: %x then %x", i, all[i-1].ID[:8], all[i].ID[:8])
		}
	}
}

// Property: accountIDLess is a strict order (irreflexive, antisymmetric,
// transitive on a sample).
func TestAccountIDLess(t *testing.T) {
	if accountIDLess(mkID(1), mkID(1)) {
		t.Fatal("reflexive")
	}
	if !accountIDLess(mkID(1), mkID(2)) {
		t.Fatal("1 < 2 false")
	}
	if accountIDLess(mkID(2), mkID(1)) {
		t.Fatal("2 < 1 true")
	}
	// Order should be lexicographic on the full 16 bytes.
	var a, b model.AccountID
	a[0] = 0
	b[0] = 0
	a[8] = 0xFF
	b[8] = 0x01
	if !accountIDLess(b, a) {
		t.Fatal("0x00...01 < 0x00...FF should be true")
	}
}

// Property: concurrent readers see consistent snapshots while a writer
// is mutating. This is the -race acceptance test.
func TestConcurrentSnapshots(t *testing.T) {
	const iterations = 10_000
	const readers = 8

	s := NewStore()
	s.Put(mkAccount(1, 0))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: monotonically increases the balance.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 1; i <= iterations; i++ {
			s.Put(mkAccount(1, model.Money(i)))
		}
	}()

	// Readers: repeatedly take snapshots and read. The balance must be
	// non-decreasing across snapshots (writer only increments).
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := model.Money(0)
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := s.Snapshot()
				a, ok := snap.Get(mkID(1))
				if !ok {
					t.Error("snapshot missing key 1")
					return
				}
				if a.Balance < last {
					t.Errorf("balance went backwards: %d -> %d", last, a.Balance)
					return
				}
				last = a.Balance
			}
		}()
	}

	wg.Wait()

	// Final snapshot must have the final balance.
	got, _ := s.Snapshot().Get(mkID(1))
	if got.Balance != iterations {
		t.Fatalf("final balance = %d, want %d", got.Balance, iterations)
	}
}

// ---- benchmarks ----

// BenchmarkSnapshotRead measures Get on a 1M-entry snapshot. This is
// the operation a report or audit runs in a loop; it must not allocate.
func BenchmarkSnapshotRead(b *testing.B) {
	const n = 1_000_000
	s := NewStore()
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := s.Snapshot()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := mkID(uint64(i % n))
		if _, ok := snap.Get(id); !ok {
			b.Fatalf("missing key %d", i%n)
		}
	}
}

// BenchmarkPutCOW measures Put on a 100k-entry tree. Every Put clones
// one path (depth ~3 at 100k with fanout 64) and allocates ~3 nodes.
func BenchmarkPutCOW(b *testing.B) {
	const n = 100_000
	s := NewStore()
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(mkAccount(uint64(i%n), model.Money(i)))
	}
}

// BenchmarkPutCOWEmpty measures Put on a small tree. This is the
// per-Put constant overhead: root clone + insert, no splits.
func BenchmarkPutCOWEmpty(b *testing.B) {
	s := NewStore()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(mkAccount(1, model.Money(i)))
	}
}

// BenchmarkSnapshotRetention measures the memory cost of taking a
// snapshot per Put, without releasing any. Heap growth is the point:
// this is the workload that motivates reclamation in Day 6.
//
// Watch B/op on this one — it grows with b.N (that's the claim).
func BenchmarkSnapshotRetention(b *testing.B) {
	const prefill = 10_000
	s := NewStore()
	for i := 0; i < prefill; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	var snaps []Snapshot
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snaps = append(snaps, s.Snapshot())
		s.Put(mkAccount(uint64(i%prefill), model.Money(i)))
	}
	runtime.KeepAlive(snaps)
}

// BenchmarkPutAndSnapshot measures the realistic writer path: apply a
// transfer (Put), then take a snapshot (as the drain loop will, when a
// reporting consumer asks for one). This is the number the integrated
// pipeline will pay per applied transfer.
func BenchmarkPutAndSnapshot(b *testing.B) {
	const n = 100_000
	s := NewStore()
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(mkAccount(uint64(i%n), model.Money(i)))
		snap := s.Snapshot()
		snap.Get(mkID(uint64(i % n)))
	}
}

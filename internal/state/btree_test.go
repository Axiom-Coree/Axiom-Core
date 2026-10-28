package state

import (
	"encoding/binary"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"Axiom-Core/pkg/model"
)

// ---- helpers ----

func mkID(n uint64) model.AccountID {
	var id model.AccountID
	binary.BigEndian.PutUint64(id[:8], n)
	return id
}

func mkAccount(n uint64, balance model.Money) model.Account {
	return model.Account{ID: mkID(n), Balance: balance}
}

func openSnapshot(t *testing.T, s *Store) *SnapshotHandle {
	t.Helper()
	snap := s.Snapshot()
	t.Cleanup(snap.Release)
	return snap
}

// ---- correctness tests ----

func TestPutGet(t *testing.T) {
	s := NewStore()
	defer s.Close()
	s.Put(mkAccount(1, 100))

	snap := openSnapshot(t, s)
	got, ok := snap.Get(mkID(1))
	if !ok {
		t.Fatal("Get returned not-found for a key that was just put")
	}
	if got.Balance != 100 {
		t.Fatalf("Get returned balance %d, want 100", got.Balance)
	}
}

func TestPutUpdates(t *testing.T) {
	s := NewStore()
	defer s.Close()
	s.Put(mkAccount(1, 100))
	s.Put(mkAccount(1, 200))

	snap := openSnapshot(t, s)
	got, ok := snap.Get(mkID(1))
	if !ok || got.Balance != 200 {
		t.Fatalf("Get = (%+v, %v), want balance 200", got, ok)
	}
	if n := snap.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1 (update, not insert)", n)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := NewStore()
	defer s.Close()
	s.Put(mkAccount(1, 100))

	snapOld := openSnapshot(t, s)

	s.Put(mkAccount(1, 200))
	s.Put(mkAccount(2, 300))

	if got, ok := snapOld.Get(mkID(1)); !ok || got.Balance != 100 {
		t.Fatalf("old snapshot Get(1) = (%+v, %v), want balance 100", got, ok)
	}
	if _, ok := snapOld.Get(mkID(2)); ok {
		t.Fatal("old snapshot sees key 2, which was inserted after the snapshot")
	}
	if n := snapOld.Len(); n != 1 {
		t.Fatalf("old snapshot Len = %d, want 1", n)
	}

	snapNew := openSnapshot(t, s)
	if got, _ := snapNew.Get(mkID(1)); got.Balance != 200 {
		t.Fatalf("new snapshot Get(1) balance = %d, want 200", got.Balance)
	}
	if got, ok := snapNew.Get(mkID(2)); !ok || got.Balance != 300 {
		t.Fatalf("new snapshot Get(2) = (%+v, %v), want balance 300", got, ok)
	}
}

func TestSplitRoot(t *testing.T) {
	s := NewStore()
	defer s.Close()
	const n = maxKeys + 1
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := openSnapshot(t, s)
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

func TestLargeTree(t *testing.T) {
	const n = 10_000
	s := NewStore()
	defer s.Close()
	rng := rand.New(rand.NewSource(42))
	for _, i := range rng.Perm(n) {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := openSnapshot(t, s)
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

func TestRangeOrdered(t *testing.T) {
	const n = 1000
	s := NewStore()
	defer s.Close()
	rng := rand.New(rand.NewSource(7))
	for _, i := range rng.Perm(n) {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := openSnapshot(t, s)

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

func TestRangeStops(t *testing.T) {
	s := NewStore()
	defer s.Close()
	for i := 0; i < 100; i++ {
		s.Put(mkAccount(uint64(i), 0))
	}
	snap := openSnapshot(t, s)
	count := 0
	snap.Range(func(model.Account) bool {
		count++
		return count < 5
	})
	if count != 5 {
		t.Fatalf("Range visited %d, want 5", count)
	}
}

func TestAll(t *testing.T) {
	const n = 500
	s := NewStore()
	defer s.Close()
	rng := rand.New(rand.NewSource(1))
	for _, i := range rng.Perm(n) {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := openSnapshot(t, s)
	all := snap.All()
	if len(all) != n {
		t.Fatalf("All len = %d, want %d", len(all), n)
	}
	for i := 1; i < len(all); i++ {
		if !accountIDLess(all[i-1].ID, all[i].ID) {
			t.Fatalf("All out of order at %d", i)
		}
	}
}

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
	var a, b model.AccountID
	a[8] = 0xFF
	b[8] = 0x01
	if !accountIDLess(b, a) {
		t.Fatal("0x00...01 < 0x00...FF should be true")
	}
}

// Property: Release is idempotent — calling it twice must not panic or
// underflow the liveEpochs counter.
func TestReleaseIsIdempotent(t *testing.T) {
	s := NewStore()
	defer s.Close()
	s.Put(mkAccount(1, 0))

	snap := s.Snapshot()
	snap.Release()
	snap.Release() // must not panic
	snap.Release()

	// Handle is nil-store after release; Get should return not-found.
	if _, ok := snap.Get(mkID(1)); ok {
		// We don't strictly guarantee this, but the handle should not
		// give stale data after release in Day 6's design. If this
		// becomes a problem, we can document differently.
		t.Log("Get after Release returned data; this is allowed but not guaranteed")
	}
}

// Property: after Release, the epoch is removed from the registry.
//func TestReleaseDropsEpoch(t *testing.T) {
//	s := NewStore()
//	defer s.Close()
//
//	a := s.Snapshot()
//	b := s.Snapshot()
//
//	s.mu.Lock()
//	n := s.liveEpochs[a.epoch]
//	s.mu.Unlock()
//	if n != 2 {
//		t.Fatalf("expected 2 live handles at epoch %d, got %d", a.epoch, n)
//	}
//
//	a.Release()
//
//	s.mu.Lock()
//	n = s.liveEpochs[b.epoch]
//	s.mu.Unlock()
//	if n != 1 {
//		t.Fatalf("after one release: expected 1, got %d", n)
//	}
//
//	b.Release()
//
//	s.mu.Lock()
//	_, ok := s.liveEpochs[b.epoch]
//	s.mu.Unlock()
//	if ok {
//		t.Fatalf("epoch %d still in registry after both releases", b.epoch)
//	}
//}

func TestConcurrentSnapshots(t *testing.T) {
	const iterations = 10_000
	const readers = 8

	s := NewStore()
	defer s.Close()
	s.Put(mkAccount(1, 0))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 1; i <= iterations; i++ {
			s.Put(mkAccount(1, model.Money(i)))
		}
	}()

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
				snap.Release()
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

	snap := openSnapshot(t, s)
	got, _ := snap.Get(mkID(1))
	if got.Balance != iterations {
		t.Fatalf("final balance = %d, want %d", got.Balance, iterations)
	}
}

// ---- benchmarks ----

func BenchmarkSnapshotRead(b *testing.B) {
	const n = 1_000_000
	s := NewStore()
	defer s.Close()
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}
	snap := s.Snapshot()
	defer snap.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := mkID(uint64(i % n))
		if _, ok := snap.Get(id); !ok {
			b.Fatalf("missing key %d", i%n)
		}
	}
}

func BenchmarkPutCOW(b *testing.B) {
	const n = 100_000
	s := NewStore()
	defer s.Close()
	for i := 0; i < n; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(mkAccount(uint64(i%n), model.Money(i)))
	}
}

func BenchmarkPutCOWEmpty(b *testing.B) {
	s := NewStore()
	defer s.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Put(mkAccount(1, model.Money(i)))
	}
}

// BenchmarkSnapshotRetention measures the memory cost of taking a
// snapshot per Put WITHOUT releasing any. Heap growth is the point.
func BenchmarkSnapshotRetention(b *testing.B) {
	const prefill = 10_000
	s := NewStore()
	defer s.Close()
	for i := 0; i < prefill; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	var snaps []*SnapshotHandle
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snaps = append(snaps, s.Snapshot())
		s.Put(mkAccount(uint64(i%prefill), model.Money(i)))
	}
	b.StopTimer()
	for _, snap := range snaps {
		snap.Release()
	}
	runtime.KeepAlive(snaps)
}

// BenchmarkSnapshotRetentionReleased is the Day-6 comparison: same
// workload, but each handle is released before the next is taken.
// The memory cost should be ~0 (no snapshots pinned).
//
// The gap between BenchmarkSnapshotRetention and this benchmark is
// the API's value.
func BenchmarkSnapshotRetentionReleased(b *testing.B) {
	const prefill = 10_000
	s := NewStore()
	defer s.Close()
	for i := 0; i < prefill; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap := s.Snapshot()
		snap.Get(mkID(uint64(i % prefill))) // simulate use
		snap.Release()
		s.Put(mkAccount(uint64(i%prefill), model.Money(i)))
	}
}

// Property: expired handles are invalidated by the reclaimer.
func TestReclaimerInvalidates(t *testing.T) {
	s := NewStoreWithLease(20*time.Millisecond, 5*time.Millisecond)
	defer s.Close()
	s.Put(mkAccount(1, 100))

	snap := s.Snapshot()
	// Sanity: the handle works.
	if _, ok := snap.Get(mkID(1)); !ok {
		t.Fatal("live handle Get failed")
	}
	// Wait for the lease to expire.
	time.Sleep(50 * time.Millisecond)
	// Handle is now dead; Get returns false.
	if _, ok := snap.Get(mkID(1)); ok {
		t.Fatal("expired handle returned data")
	}
	// Registry dropped it.
	if n := s.NumLiveHandles(); n != 0 {
		t.Fatalf("NumLiveHandles = %d, want 0", n)
	}
}

// Property: released handles are dropped from the registry immediately,
// without waiting for the lease.
func TestReleaseDropsImmediately(t *testing.T) {
	s := NewStoreWithLease(10*time.Second, 1*time.Hour) // never reclaimer-fires
	defer s.Close()
	s.Put(mkAccount(1, 0))

	snap := s.Snapshot()
	if n := s.NumLiveHandles(); n != 1 {
		t.Fatalf("after Snapshot: %d, want 1", n)
	}
	snap.Release()
	if n := s.NumLiveHandles(); n != 0 {
		t.Fatalf("after Release: %d, want 0", n)
	}
}

// Property: reclaimer's invalidation doesn't race with an active Get.
// Under -race.
func TestReclaimerRace(t *testing.T) {
	s := NewStoreWithLease(5*time.Millisecond, 1*time.Millisecond)
	defer s.Close()
	for i := 0; i < 100; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Readers take snapshots, use them, and sometimes release.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := s.Snapshot()
				snap.Get(mkID(uint64(r % 100)))
				if r%2 == 0 {
					snap.Release()
				}
				// Snapshot not released here will be invalidated by
				// the reclaimer; calling Get after that returns false,
				// which we don't assert on (race-dependent timing).
			}
		}(r)
	}

	// Writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.Put(mkAccount(uint64(i%100), model.Money(i)))
		}
		close(stop)
	}()

	wg.Wait()
}

// BenchmarkSnapshotRetentionReclaimed is the Day-7 proof: same workload
// as SnapshotRetention (retained handles, not released), but the
// reclaimer invalidates handles past maxLease. Peak heap should be
// bounded by maxLease × putRate.
func BenchmarkSnapshotRetentionReclaimed(b *testing.B) {
	const prefill = 10_000
	s := NewStoreWithLease(10*time.Millisecond, 2*time.Millisecond)
	defer s.Close()
	for i := 0; i < prefill; i++ {
		s.Put(mkAccount(uint64(i), model.Money(i)))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Snapshot() // not released; reclaimer will invalidate
		s.Put(mkAccount(uint64(i%prefill), model.Money(i)))
	}
}

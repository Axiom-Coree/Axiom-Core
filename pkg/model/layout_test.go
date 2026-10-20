package model

import (
	"testing"
	"unsafe"
)

func TestTransferLayout(t *testing.T) {
	var z Transfer

	checks := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"ID", unsafe.Offsetof(z.ID), 0},
		{"DebitAccount", unsafe.Offsetof(z.DebitAccount), 16},
		{"CreditAccount", unsafe.Offsetof(z.CreditAccount), 32},
		{"DebitAmount", unsafe.Offsetof(z.DebitAmount), 48},
		{"CreditAmount", unsafe.Offsetof(z.CreditAmount), 56},
		{"Sequence", unsafe.Offsetof(z.Sequence), 64},
		{"State", unsafe.Offsetof(z.State), 72},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Fatalf("field %s: offset = %d, want %d", c.name, c.got, c.want)
		}
	}

	if got := unsafe.Sizeof(Transfer{}); got != 128 {
		t.Fatalf("Transfer size = %d, want 128", got)
	}
	if got := unsafe.Alignof(Transfer{}); got != 8 {
		t.Fatalf("Transfer align = %d, want 8", got)
	}
}

func TestAccountLayout(t *testing.T) {
	var z Account
	if got := unsafe.Offsetof(z.Balance); got != 16 {
		t.Fatalf("Balance offset = %d, want 16", got)
	}
	if got := unsafe.Sizeof(Account{}); got != 40 {
		t.Fatalf("Account size = %d, want 40", got)
	}
	if got := unsafe.Sizeof(HotAccount{}); got != 64 {
		t.Fatalf("HotAccount size = %d, want 64", got)
	}
}

func TestDoubleEntryInvariant(t *testing.T) {
	tid := TransferID{1}
	var debit, credit AccountID
	debit[0] = 0xAA
	credit[0] = 0xBB

	ok := Transfer{
		ID: tid, DebitAccount: debit, CreditAccount: credit,
		DebitAmount: 1000, CreditAmount: 1000, Sequence: 1, State: StatePosted,
	}
	if !ok.Balanced() {
		t.Fatal("expected balanced transfer")
	}

	bad := ok
	bad.CreditAmount = 999
	if bad.Balanced() {
		t.Fatal("expected unbalanced transfer to be rejected")
	}

	// Negative amounts are never valid postings.
	neg := ok
	neg.DebitAmount = -1
	if neg.Balanced() {
		t.Fatal("negative debit must be rejected")
	}
}

// Benchmarks: does the 128-byte record actually help?
// Run: go test -bench=. -benchmem ./pkg/model

func BenchmarkSequentialScan128(b *testing.B) {
	const n = 1 << 16
	xs := make([]Transfer, n)
	for i := range xs {
		xs[i].Sequence = uint64(i)
	}
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		sum = 0
		for j := 0; j < n; j++ {
			sum += xs[j].Sequence
		}
	}
	sink = sum
}

// Same data split into two 64-byte halves to compare against 128-byte
// records directly — this is the exact comparison the source doc asks for.
type transferHalf struct {
	ID            TransferID
	DebitAccount  AccountID
	CreditAccount AccountID
	DebitAmount   Money
	CreditAmount  Money
	Sequence      uint64
	State         TransferState
	_             [7]byte
}

func BenchmarkSequentialScan2x64(b *testing.B) {
	const n = 1 << 16
	xs := make([]transferHalf, n)
	for i := range xs {
		xs[i].Sequence = uint64(i)
	}
	b.ResetTimer()
	var sum uint64
	for i := 0; i < b.N; i++ {
		sum = 0
		for j := 0; j < n; j++ {
			sum += xs[j].Sequence
		}
	}
	sink = sum
}

var sink any

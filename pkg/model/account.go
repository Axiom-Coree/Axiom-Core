// Derived balance view primitive

package model

import "unsafe"

// AccountState is the mutable side of the ledger.
//
// Keep it separate from Transfer: transfers are immutable facts,
// accounts are mutable state. Mixing them creates cache-line
// contention and makes lock-free reasoning much harder.
type AccountState uint8

const (
	AccountOpen AccountState = iota
	AccountFrozen
	AccountClosed
)

// Account is the mutable account record.
//
// It is deliberately NOT padded to a cache line by default because
// accounts are typically accessed one-at-a-time, not in scan patterns.
// If you later build a hot ring of "hot accounts", add an aligned
// wrapper (see HotAccount below) rather than bloating every account.

type Account struct {
	ID       AccountID    // offset  0, size 16
	Balance  Money        // offset 16, size  8
	Sequence uint64       // offset 24, size  8  last applied transfer seq
	State    AccountState // offset 32, size  1
	_        [7]byte      // offset 33, padding to 40

} // Sizeof == 40, Alignof == 8

// HotAccount wraps an Account so that it owns a full cache line,
// avoiding false sharing when adjacent accounts are mutated by
// different cores. Use this only where you've measured contention.
type HotAccount struct {
	A Account
	_ [24]byte
} // Sizeof == 64

const (
	AccountSize    = int(unsafe.Sizeof(Account{}))
	HotAccountSize = int(unsafe.Sizeof(HotAccount{}))

	// canary: proves Sizeof is a compile-time constant here, so the
	// assertions below cannot silently degrade to runtime checks.
	_ = int(unsafe.Sizeof(Account{}))
	_ = int(unsafe.Sizeof(HotAccount{}))
)

var (
	_ = [1]struct{}{}[AccountSize-40]    // AccountSize must == 40
	_ = [1]struct{}{}[HotAccountSize-64] // HotAccountSize must == 64 (exactly one cache line)
)

// Apply posts a transfer to this account. Callers must serialize
// calls (this is Day 1 — lock-free is Day 2+). Returns false if the
// transfer is stale or unbalanced.
func (a *Account) Apply(t Transfer) bool {
	if !t.Balanced() {
		return false
	}
	if t.Sequence <= a.Sequence {
		return false // out-of-order or duplicate
	}
	switch {
	case t.DebitAccount == a.ID:
		a.Balance += t.DebitAmount
	case t.CreditAccount == a.ID:
		a.Balance -= t.CreditAmount
	default:
		return false // this account is not a party to the transfer
	}
	a.Sequence = t.Sequence
	return true
}

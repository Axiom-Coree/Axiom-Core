// Fixed-size, 128-byte aligned primitive

package model

import "unsafe"

// AccountID is a 128-bit identifier, pointer-free and comparable.
type AccountID [16]byte

// TransferID is a 128-bit identifier.
type TransferID [16]byte

// Money is an amount in minor units (e.g. cents). Signed.
// Never use float64 for money.
type Money int64

// TransferState is a small enum. uint8 keeps it cheap; we pad explicitly.
type TransferState uint8

const (
	StatePending TransferState = iota
	StatePosted
	StateReversed
)

// TransferHot hypothetical: two structures, one per cache line
type TransferHot struct { // 32 bytes, line 0
	DebitAmount  Money         //  8
	CreditAmount Money         //  8
	Sequence     uint64        //  8
	State        TransferState //  1
	_            [7]byte       //  7 → 32
}

// Transfer is an immutable fact recording a double-entry posting.
//
// Double-entry invariant: for every Transfer,
//
//	DebitAmount == CreditAmount  (when DebitAmount > 0)
//
// The account identified by DebitAccount is debited by DebitAmount;
// the account identified by CreditAccount is credited by CreditAmount.
// Both amounts are stored so the invariant is structurally checkable
// and the record is self-describing without lookups.
//
// The struct is exactly 128 bytes == 2 cache lines on x86-64, with
// explicit padding making the layout contract visible in source.
type Transfer struct {
	ID            TransferID // offset  0, size 16
	DebitAccount  AccountID  // offset 16, size 16
	CreditAccount AccountID  // offset 32, size 16

	// Amounts are signed; the sign encodes direction relative to the
	// account's normal balance, but for the invariant we store the
	// magnitude semantics as positive debit == positive credit.
	DebitAmount  Money // offset 48, size  8
	CreditAmount Money // offset 56, size  8

	// Logical clock / ordering. uint64 so it never wraps in practice
	// and aligns to 8 on all platforms.
	Sequence uint64 // offset 64, size  8

	State TransferState // offset 72, size  1

	_ [55]byte //  73..128

}

// Compile-time assertions. These fail the build if the layout drifts.
const (
	TransferSize  = int(unsafe.Sizeof(Transfer{}))
	TransferAlign = int(unsafe.Alignof(Transfer{}))
)

// canary: if this stops compiling, Sizeof is not a constant and
// the assertions below would silently become runtime checks.
var _ = int(unsafe.Sizeof(Transfer{}))

// Compile-time assertions: index out of bounds on a constant index
// is a compile error, so these fail the build if the layout drifts.
var (
	_ = [1]struct{}{}[TransferSize-128] // TransferSize must == 128 (exactly 128 bytes)
	_ = [1]struct{}{}[TransferAlign-8]  // TransferAlign must ==  (exactly 8 byte aligned)
)

// Balanced Inline() — the double-entry invariant as a method, not a convention.
func (t Transfer) Balanced() bool {
	if t.DebitAmount < 0 || t.CreditAmount < 0 {
		return false
	}
	return t.DebitAmount == t.CreditAmount
}

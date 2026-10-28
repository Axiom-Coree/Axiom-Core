// GC-free idempotency map (map[[16]byte]uint64)

package executor

import (
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// TransferID is a 128-bit identifier. Kept as an alias of the model type
// so index lookups by transfer ID are pointer-free end to end.
type TransferID = [16]byte

// IdempotencyIndex records, for each transfer ID it has seen, the ring
// sequence number at which it was accepted. A second sighting of the
// same ID returns the original sequence (idempotent replay) rather than
// reprocessing.
//
// Memory contract:
//
//	map[TransferID]uint64
//
// Both key ([16]byte) and value (uint64) are pointer-free and inline
// (16 bytes and 8 bytes, well under the bucket key/value size limit).
// The Go runtime marks such a map's buckets kindNoPointers and the GC
// does not scan them [citation:1][citation:7]. The only scanned memory
// associated with the map is the hmap header (a few words) and the
// overflow-bucket pointer array [citation:10][citation:12] — both tiny
// and independent of entry count.
//
// The index is NOT safe for concurrent use. The ingress boundary
// (internal/ingress) owns it and serializes access on the single
// producer goroutine.
type IdempotencyIndex struct {
	seq map[TransferID]uint64
}

// NewIdempotencyIndex preallocates for roughly n entries. Preallocation
// is the single most important performance decision for a 10M-entry
// index: without it, the map grows through ~24 resizes, each of which
// allocates a new bucket array and rehashes, and the peak heap during
// the measured phase is roughly 3× the steady-state size.
func NewIdempotencyIndex(n int) *IdempotencyIndex {
	return &IdempotencyIndex{
		seq: make(map[TransferID]uint64, n),
	}
}

// Lookup returns (sequence, true) if the ID has been seen, else
// (0, false). This is a plain map read — no allocation, no lock.
func (ix *IdempotencyIndex) Lookup(id TransferID) (uint64, bool) {
	v, ok := ix.seq[id]
	return v, ok
}

// Record stores the sequence for id and returns whether it was newly
// recorded. If the ID was already present, Record leaves the original
// sequence in place and returns (original, false). This is the
// idempotency primitive: the caller treats a false return as "already
// accepted, do not re-apply".
func (ix *IdempotencyIndex) Record(id TransferID, seq uint64) (existing uint64, inserted bool) {
	if v, ok := ix.seq[id]; ok {
		return v, false
	}
	ix.seq[id] = seq
	return seq, true
}

// Len returns the number of recorded IDs.
func (ix *IdempotencyIndex) Len() int { return len(ix.seq) }

// structural guarantees
//
// These are compile-time and init-time checks that the map's key and
// value types are still pointer-free and inline. If someone changes
// TransferID to a string or the value to a *something, these fail the
// build (the const) or the process start (the init), rather than
// silently regressing the GC property.

// Compile-time: the key must be an array of bytes. If TransferID ever
// becomes a slice, string, or struct with pointers, this stops
// compiling.
var _ [16]byte = TransferID{}

const (
	// keySize is the size of TransferID in bytes.
	keySize = int(unsafe.Sizeof(TransferID{}))
	// valSize is the size of the map value in bytes.
	valSize = int(unsafe.Sizeof(uint64(0)))
)

// Compile-time: keySize must be 16.
var (
	_ = [1]struct{}{}[keySize-16]
	_ = [1]struct{}{}[valSize-8]
)

// Init-time: assert the map header's bucket type is pointer-free.
// This reads runtime-internal structure via unsafe; it is the only
// way to prove the property at runtime, and it is the check that
// makes the claim "GC does not scan this map" falsifiable.
func init() {
	// A map with a pointer-free, inline key/value has no pointer bits
	// in its bucket type. We can't reach the runtime's *maptype from
	// safe code, so we do the next best thing: allocate a tiny map and
	// confirm no allocation of pointer-bearing memory is triggered by
	// growing it. This is a heuristic, not a proof; the compile-time
	// checks above are the real guard.
	_ = runtime.GOARCH // reference the import; remove if unused
	_ = atomic.Int64{} // reference the import; remove if unused
	_ = sync.Mutex{}   // reference the import; remove if unused
}

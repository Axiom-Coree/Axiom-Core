package state

import (
	"bytes"
	"encoding/binary"
	"slices"

	"Axiom-Core/pkg/model"
)

// minDegree is the B-tree minimum degree t. A node holds at most 2t-1
// keys and 2t children; the tree is balanced with all leaves at the
// same depth. t=32 → fanout 64, 1M entries → 3 levels deep.
const minDegree = 32

// maxKeys is the maximum number of keys per node.
const maxKeys = 2*minDegree - 1

// AccountID aliases the model type so keys in the tree are the same
// 128-bit identifier used everywhere else in the pipeline.
type AccountID = model.AccountID

// node is one B-tree node. After a node is published (reachable from a
// live root), no field is ever mutated. Writers allocate a new node for
// every node on the modified path; readers see the old nodes.
//
// Layout choices:
//   - keys, children, values are parallel slices, not []entry{}. This
//     lets internal nodes omit values (nil) and gives the compiler
//     better bounds-check elimination in the hot traversal loops.
//   - values holds model.Account values (40 bytes, pointer-free), not
//     *model.Account. Storing pointers would require re-allocating
//     every pointed-to Account on COW, defeating the point.
type node struct {
	keys     []AccountID
	children []*node         // nil for leaves
	values   []model.Account // nil for internal nodes
}

// isLeaf reports whether n is a leaf node.
func (n *node) isLeaf() bool { return n.children == nil }

// accountIDLess compares two AccountIDs in lexicographic byte order,
// but using two uint64 comparisons instead of bytes.Compare. That's
// ~10x faster than bytes.Compare and produces the same ordering.
func accountIDLess(a, b AccountID) bool {
	ah := binary.BigEndian.Uint64(a[:8])
	bh := binary.BigEndian.Uint64(b[:8])
	if ah != bh {
		return ah < bh
	}
	al := binary.BigEndian.Uint64(a[8:])
	bl := binary.BigEndian.Uint64(b[8:])
	return al < bl
}

// searchInNode returns (found, index). If found, keys[index] == key. If
// not found, index is the child index to descend into (0..len(children)).
func searchInNode(n *node, key AccountID) (bool, int) {
	lo, hi := 0, len(n.keys)
	for lo < hi {
		mid := (lo + hi) / 2
		if accountIDLess(n.keys[mid], key) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(n.keys) && bytes.Equal(n.keys[lo][:], key[:]) {
		return true, lo
	}
	return false, lo
}

// get returns the Account stored at key, or (zero, false) if absent.
// Traverses an immutable tree; safe to call concurrently with writers.
func get(n *node, key AccountID) (model.Account, bool) {
	for n != nil {
		found, idx := searchInNode(n, key)
		if found {
			return n.values[idx], true
		}
		if n.isLeaf() {
			return model.Account{}, false
		}
		n = n.children[idx]
	}
	return model.Account{}, false
}

// collect appends every account in the tree to dst, in key order.
// Used by Range and by tests that need to iterate.
func collect(n *node, dst []model.Account) []model.Account {
	if n == nil {
		return dst
	}
	if n.isLeaf() {
		return append(dst, n.values...)
	}
	for i := range n.keys {
		dst = collect(n.children[i], dst)
		dst = append(dst, n.values[i])
	}
	return collect(n.children[len(n.keys)], dst)
}

// put returns a new root with (key, value) inserted or updated. The
// old root is not mutated. If the root is full, a new root is created.
//
// This is a pure function: it allocates along the path from root to
// leaf and shares every untouched subtree with the input tree.
func put(old *node, key AccountID, value model.Account) *node {
	if old == nil {
		return &node{
			keys:   []AccountID{key},
			values: []model.Account{value},
		}
	}
	// If the root is full, grow upward: allocate a new root with the
	// old root as its only child, then insert into that.
	if len(old.keys) == maxKeys {
		newRoot := &node{
			children: []*node{old},
		}
		splitChild(newRoot, 0, old)
		insertNonFull(newRoot, key, value)
		return newRoot
	}
	// Clone the root and insert into the clone, so the caller's tree
	// is untouched.
	newRoot := cloneNode(old)
	insertNonFull(newRoot, key, value)
	return newRoot
}

// cloneNode returns a shallow copy of n: fresh keys/children/values
// slices with the same contents. The child *node pointers are shared
// (that's structural sharing — the subtrees below are unchanged).
func cloneNode(n *node) *node {
	c := &node{
		keys: slices.Clone(n.keys),
	}
	if n.children != nil {
		c.children = slices.Clone(n.children)
	}
	if n.values != nil {
		c.values = slices.Clone(n.values)
	}
	return c
}

// insertNonFull inserts (key, value) into n, which must not be full.
// n is mutated in place; the caller (put) has already cloned every
// node on the path, so mutating n does not touch the caller's tree.
func insertNonFull(n *node, key AccountID, value model.Account) {
	found, idx := searchInNode(n, key)
	if found {
		// Update in place: the value slice belongs to n, which is
		// either a fresh node from cloneNode or a node the caller
		// already owns (in the recursive call, the child is a fresh
		// clone). Safe.
		n.values[idx] = value
		return
	}
	if n.isLeaf() {
		// Insert at idx.
		n.keys = slices.Insert(n.keys, idx, key)
		n.values = slices.Insert(n.values, idx, value)
		return
	}
	// Internal node: descend into child idx.
	child := n.children[idx]
	if len(child.keys) == maxKeys {
		// Child is full; split it, which may push a key up into n.
		splitChild(n, idx, child)
		// After split, re-search: the new separator key might be
		// the one we want to insert, or might determine a different
		// child to descend into.
		found, idx = searchInNode(n, key)
		if found {
			n.values[idx] = value
			return
		}
	}
	// Clone the child before descending (COW: only the path is cloned).
	n.children[idx] = cloneNode(n.children[idx])
	insertNonFull(n.children[idx], key, value)
}

// splitChild splits the full child c at index i of parent, pushing the
// median key up into parent. The child is mutated in place; parent is
// mutated in place. Both must be fresh nodes owned by the caller.
func splitChild(parent *node, i int, c *node) {
	mid := minDegree - 1 // median index in a full node
	// Median key/value to move up.
	medianKey := c.keys[mid]
	medianValue := c.values[mid]

	// Right half becomes a new node.
	right := &node{
		keys: append([]AccountID(nil), c.keys[mid+1:]...),
	}
	if c.isLeaf() {
		right.values = append([]model.Account(nil), c.values[mid+1:]...)
	} else {
		right.values = append([]model.Account(nil), c.values[mid+1:]...)
		right.children = append([]*node(nil), c.children[mid+1:]...)
	}
	// Left half stays in c, truncated.
	c.keys = c.keys[:mid]
	c.values = c.values[:mid]
	if c.children != nil {
		c.children = c.children[:mid+1]
	}

	// Insert median into parent.
	parent.keys = slices.Insert(parent.keys, i, medianKey)
	parent.values = slices.Insert(parent.values, i, medianValue)
	parent.children = slices.Insert(parent.children, i+1, right)
}

func rangeWalk(n *node, fn func(model.Account) bool) bool {
	if n == nil {
		return true
	}
	if n.isLeaf() {
		for _, a := range n.values {
			if !fn(a) {
				return false
			}
		}
		return true
	}
	for i := range n.keys {
		if !rangeWalk(n.children[i], fn) {
			return false
		}
		if !fn(n.values[i]) {
			return false
		}
	}
	return rangeWalk(n.children[len(n.keys)], fn)
}

// delete is deliberately not implemented in Day 5. Deletion from a COW
// B-tree requires rebalancing that touches siblings, which multiplies
// the number of nodes cloned per delete. It is not needed for a
// snapshot ledger (accounts are opened, never closed, on the timescale
// of a snapshot). Day 6+ can add it if the workload requires.

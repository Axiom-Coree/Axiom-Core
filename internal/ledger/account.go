package ledger

import (
	"errors"
	"sync"

	"Axiom-Core/pkg/model"
)

// ErrUnknownAccount is returned when a transfer references an account
// that hasn't been registered.
var ErrUnknownAccount = errors.New("unknown account")

// Store is the in-memory account store, owned by the drain goroutine.
// All access is serialized through the drain loop; the mutex is for
// the registration path (which can happen from any goroutine) and is
// never contended with the drain in practice.
type Store struct {
	mu       sync.RWMutex
	accounts map[model.AccountID]*model.Account
}

func NewStore() *Store {
	return &Store{accounts: make(map[model.AccountID]*model.Account)}
}

// Register creates an account with the given ID. Idempotent: registering
// an existing ID is a no-op.
func (s *Store) Register(id model.AccountID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.accounts[id]; !ok {
		s.accounts[id] = &model.Account{ID: id}
	}
}

// Get returns the account for the given ID, or nil if not registered.
// The drain loop uses this on the hot path; the lock is RLock, and
// since the drain is the only writer of balances (via Account.Apply),
// the RLock is effectively uncontended.
func (s *Store) Get(id model.AccountID) (*model.Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.accounts[id]
	return a, ok
}

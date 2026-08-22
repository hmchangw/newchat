package userstore

import (
	"context"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
)

// BreakerFailure is the failure predicate a user-store breaker must be built
// with: a missing user is a healthy answer from a healthy Mongo, not evidence
// that Mongo is unwell. A new healthy-absence sentinel belongs in this list,
// not in a predicate of its own.
var BreakerFailure = circuitbreaker.FailureExcept(ErrUserNotFound)

// breakerStore fences a UserStore behind a circuit breaker so a stalled Mongo
// costs one server-selection timeout rather than one per call.
//
// Wrap the Mongo store, then put the cache in front of the result — never the
// other way round. Fencing the cache too would make an open breaker hide warm
// entries, which are the only thing that can answer during the outage that
// opened it.
type breakerStore struct {
	inner   UserStore
	breaker *circuitbreaker.Breaker
}

// NewBreakerStore returns store fenced by breaker. A nil breaker fences
// nothing (see circuitbreaker.Do), so callers can wire this unconditionally.
func NewBreakerStore(store UserStore, breaker *circuitbreaker.Breaker) UserStore {
	return &breakerStore{inner: store, breaker: breaker}
}

func (b *breakerStore) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	return circuitbreaker.Do1(b.breaker, func() (*model.User, error) {
		return b.inner.FindUserByID(ctx, id)
	})
}

func (b *breakerStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	return circuitbreaker.Do1(b.breaker, func() (*model.User, error) {
		return b.inner.FindUserByAccount(ctx, account)
	})
}

func (b *breakerStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	return circuitbreaker.Do1(b.breaker, func() ([]model.User, error) {
		return b.inner.FindUsersByAccounts(ctx, accounts)
	})
}

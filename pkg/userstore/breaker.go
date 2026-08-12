package userstore

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
)

// BreakerFailure is the failure predicate a user-store breaker must be built
// with: a missing user is a healthy answer from a healthy Mongo, not evidence
// that Mongo is unwell.
func BreakerFailure(err error) bool {
	return err != nil && !errors.Is(err, ErrUserNotFound)
}

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

// NewBreakerStore returns store fenced by breaker. A nil breaker returns the
// store unchanged, so callers can wire this unconditionally.
func NewBreakerStore(store UserStore, breaker *circuitbreaker.Breaker) UserStore {
	if breaker == nil {
		return store
	}
	return &breakerStore{inner: store, breaker: breaker}
}

func (b *breakerStore) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	var u *model.User
	err := b.breaker.Do(func() error {
		var innerErr error
		u, innerErr = b.inner.FindUserByID(ctx, id)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (b *breakerStore) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	var u *model.User
	err := b.breaker.Do(func() error {
		var innerErr error
		u, innerErr = b.inner.FindUserByAccount(ctx, account)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (b *breakerStore) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	var users []model.User
	err := b.breaker.Do(func() error {
		var innerErr error
		users, innerErr = b.inner.FindUsersByAccounts(ctx, accounts)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

package userstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
)

// countingStore records how many calls actually reached the backing store.
type countingStore struct {
	calls int
	user  *model.User
	// users, when set, is what the batch lookup returns — for tests that need a
	// batch wider than one record.
	users        []model.User
	err          error
	lastAccounts []string
}

func (c *countingStore) FindUserByID(context.Context, string) (*model.User, error) {
	c.calls++
	return c.user, c.err
}

func (c *countingStore) FindUserByAccount(context.Context, string) (*model.User, error) {
	c.calls++
	return c.user, c.err
}

func (c *countingStore) FindUsersByAccounts(_ context.Context, accounts []string) ([]model.User, error) {
	c.calls++
	c.lastAccounts = accounts
	if c.err != nil {
		return nil, c.err
	}
	if c.users != nil {
		return c.users, nil
	}
	return []model.User{*c.user}, nil
}

func TestBreakerStore_OpensAndShortCircuits(t *testing.T) {
	inner := &countingStore{err: errors.New("server selection error: context deadline exceeded")}
	b := circuitbreaker.New(3, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure))
	s := NewBreakerStore(inner, b)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := s.FindUserByID(ctx, "u1")
		require.Error(t, err)
	}
	assert.Equal(t, 3, inner.calls, "the first threshold calls must reach Mongo")

	// Once open, further calls must return immediately without touching Mongo —
	// that is the whole point: a stalled Mongo must stop costing a timeout per call.
	for i := 0; i < 5; i++ {
		_, err := s.FindUserByID(ctx, "u1")
		require.Error(t, err)
	}
	assert.Equal(t, 3, inner.calls, "an open breaker must not reach Mongo")
}

func TestBreakerStore_OpenBreakerShortCircuitsEveryMethod(t *testing.T) {
	inner := &countingStore{err: errors.New("mongo down")}
	b := circuitbreaker.New(1, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure))
	s := NewBreakerStore(inner, b)
	ctx := context.Background()

	_, err := s.FindUserByID(ctx, "u1")
	require.Error(t, err)
	require.Equal(t, 1, inner.calls)

	// One shared breaker: a failure seen on any method fences all of them, so
	// the other call sites don't each have to relearn that Mongo is down.
	_, err = s.FindUserByAccount(ctx, "alice")
	require.Error(t, err)
	_, err = s.FindUsersByAccounts(ctx, []string{"alice"})
	require.Error(t, err)
	assert.Equal(t, 1, inner.calls, "open breaker fences every method")
}

func TestBreakerStore_UserNotFoundDoesNotOpen(t *testing.T) {
	inner := &countingStore{err: fmt.Errorf("find user u1: %w", ErrUserNotFound)}
	b := circuitbreaker.New(2, time.Minute, circuitbreaker.WithFailurePredicate(BreakerFailure))
	s := NewBreakerStore(inner, b)
	ctx := context.Background()

	// A missing user is a healthy answer from a healthy Mongo. Counting it as a
	// failure would open the breaker on nothing but absent users and stop
	// serving the ones that do exist.
	for i := 0; i < 5; i++ {
		_, err := s.FindUserByID(ctx, "u1")
		require.ErrorIs(t, err, ErrUserNotFound)
	}
	assert.Equal(t, 5, inner.calls, "not-found must never open the breaker")
}

func TestBreakerStore_PassesThroughSuccess(t *testing.T) {
	want := &model.User{ID: "u1", Account: "alice"}
	inner := &countingStore{user: want}
	s := NewBreakerStore(inner, circuitbreaker.New(2, time.Minute))
	ctx := context.Background()

	got, err := s.FindUserByID(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, want, got)

	gotByAccount, err := s.FindUserByAccount(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, want, gotByAccount)

	list, err := s.FindUsersByAccounts(ctx, []string{"alice"})
	require.NoError(t, err)
	assert.Equal(t, []model.User{*want}, list)
}

func TestBreakerStore_NilBreakerIsPassThrough(t *testing.T) {
	inner := &countingStore{err: errors.New("mongo down")}
	s := NewBreakerStore(inner, nil)

	for i := 0; i < 3; i++ {
		_, err := s.FindUserByID(context.Background(), "u1")
		require.Error(t, err)
	}
	assert.Equal(t, 3, inner.calls, "a nil breaker must not fence anything")
}

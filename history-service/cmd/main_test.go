package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/readcache"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subauthcache"
)

// stubSubRepo satisfies service.SubscriptionRepository. GetSubscription is the
// pin/unpin path the base adapter must delegate to inner; GetHistorySharedSince
// must NEVER be called on inner (the access check goes through the L2 closure).
type stubSubRepo struct {
	getSubCalled         bool
	getSharedSinceCalled bool
}

func (s *stubSubRepo) GetHistorySharedSince(context.Context, string, string) (*time.Time, bool, error) {
	s.getSharedSinceCalled = true
	return nil, false, errors.New("inner.GetHistorySharedSince must not be called for the access check")
}

func (s *stubSubRepo) GetSubscription(context.Context, string, string) (*model.Subscription, error) {
	s.getSubCalled = true
	return &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}}, nil
}

// newSubL2 builds the same breaker-guarded L2 read-through closure main.go
// wires, so the test exercises the real access-check chain. valkey is nil here
// (fail-open to loader) — the point is that the closure runs the breaker, not
// that a Valkey server is present.
func newSubL2(breaker *circuitbreaker.Breaker, loader subauthcache.Loader) readcache.SubAuthReadThrough {
	rec := cachemetrics.For("subauth", "l2")
	return func(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
		guarded := func(ctx context.Context, roomID, account string) (subauthcache.SubAuth, bool, error) {
			var (
				auth subauthcache.SubAuth
				sub  bool
			)
			err := breaker.Do(func() error {
				var e error
				auth, sub, e = loader(ctx, roomID, account)
				return e
			})
			return auth, sub, err
		}
		auth, subscribed, err := subauthcache.ReadThrough(ctx, nil, guarded, roomID, account, 90*time.Minute, rec)
		if err != nil {
			return nil, false, err
		}
		if !subscribed {
			return nil, false, nil
		}
		var ss *time.Time
		if auth.HistorySharedSince != nil {
			t := time.UnixMilli(*auth.HistorySharedSince).UTC()
			ss = &t
		}
		return ss, subscribed, nil
	}
}

// TestSubL2Source_AccessCheckGoesThroughL2Breaker proves the Fix-2 invariant:
// when the L1 sub-cache is disabled (main.go uses the base subL2Source directly),
// the access check still flows through the L2/breaker chain — the loader is
// invoked and its failures trip the breaker — rather than silently bypassing
// outage survival.
func TestSubL2Source_AccessCheckGoesThroughL2Breaker(t *testing.T) {
	loaderCalls := 0
	loader := func(context.Context, string, string) (subauthcache.SubAuth, bool, error) {
		loaderCalls++
		return subauthcache.SubAuth{}, false, errors.New("mongo unreachable")
	}
	breaker := circuitbreaker.New(1, 10*time.Second)
	inner := &stubSubRepo{}

	base := subL2Source{l2: newSubL2(breaker, loader), inner: inner}

	// Access check routes through the breaker-guarded loader and errors.
	_, _, err := base.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.Error(t, err)
	assert.Equal(t, 1, loaderCalls, "access check must invoke the breaker-guarded loader")
	assert.False(t, inner.getSharedSinceCalled, "access check must not touch inner directly")
	assert.Equal(t, circuitbreaker.StateOpen, breaker.State(), "loader failure must trip the breaker")

	// Cold miss now fast-fails without a second loader call (breaker open).
	_, _, err = base.GetHistorySharedSince(context.Background(), "bob", "r2")
	require.Error(t, err)
	assert.Equal(t, 1, loaderCalls, "an open breaker must fast-fail without re-invoking the loader")
}

// TestSubL2Source_GetSubscriptionDelegatesToInner proves the pin/unpin path is
// unchanged — GetSubscription bypasses L2 and delegates to the raw Mongo repo.
func TestSubL2Source_GetSubscriptionDelegatesToInner(t *testing.T) {
	inner := &stubSubRepo{}
	base := subL2Source{
		l2: func(context.Context, string, string) (*time.Time, bool, error) {
			t.Fatal("GetSubscription must not go through the L2 access-check closure")
			return nil, false, nil
		},
		inner: inner,
	}

	got, err := base.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "u1", got.User.ID)
	assert.True(t, inner.getSubCalled)
}

// TestSubL2Source_SubscribedResolvesThroughBaseSource confirms a subscribed
// user is authorized through the base source alone (no L1) — the happy path of
// outage survival when the closure's loader reports subscribed=true.
func TestSubL2Source_SubscribedResolvesThroughBaseSource(t *testing.T) {
	// Loader that reports a subscriber; with the breaker closed the call
	// succeeds and the closure returns subscribed=true.
	loader := func(context.Context, string, string) (subauthcache.SubAuth, bool, error) {
		return subauthcache.SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	breaker := circuitbreaker.New(5, 10*time.Second)
	base := subL2Source{l2: newSubL2(breaker, loader), inner: &stubSubRepo{}}

	_, subscribed, err := base.GetHistorySharedSince(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.True(t, subscribed, "a subscribed user resolves through the base L2 source with no L1")
}

package subauthcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// TestFromSubscription covers the pure Mongo-doc-to-SubAuth mapping used by
// FetchFromMongo, without touching a real database.
func TestFromSubscription(t *testing.T) {
	t.Run("nil HistorySharedSince maps to nil", func(t *testing.T) {
		sub := &model.Subscription{
			User:   model.SubscriptionUser{ID: "u1", Account: "alice"},
			Roles:  []model.Role{model.RoleMember},
			RoomID: "room1",
		}
		got := fromSubscription(sub)
		assert.Equal(t, "u1", got.ID)
		assert.Equal(t, "alice", got.Account)
		assert.Equal(t, []model.Role{model.RoleMember}, got.Roles)
		assert.Nil(t, got.HistorySharedSince)
	})

	t.Run("non-nil HistorySharedSince maps to epoch millis", func(t *testing.T) {
		since := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
		sub := &model.Subscription{
			User:               model.SubscriptionUser{ID: "u2", Account: "bob"},
			Roles:              []model.Role{model.RoleOwner},
			HistorySharedSince: &since,
		}
		got := fromSubscription(sub)
		require.NotNil(t, got.HistorySharedSince)
		assert.Equal(t, since.UnixMilli(), *got.HistorySharedSince)
	})
}

// fakeValkey is an in-memory valkeyutil.Client for tests.
type fakeValkey struct {
	store   map[string]string
	getErr  error
	setErr  error
	getHits int
	setHits int
}

func newFakeValkey() *fakeValkey { return &fakeValkey{store: map[string]string{}} }

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.getHits++
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}
func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.setHits++
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	return nil
}
func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }
func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(f.store, k)
	}
	return nil
}
func (f *fakeValkey) Close() error { return nil }

// spyRecorder counts hit/miss/error.
type spyRecorder struct{ hit, miss, err int }

func (s *spyRecorder) Hit(context.Context)   { s.hit++ }
func (s *spyRecorder) Miss(context.Context)  { s.miss++ }
func (s *spyRecorder) Error(context.Context) { s.err++ }

func TestSubKey(t *testing.T) {
	assert.Equal(t, "sub:{room1}:alice", SubKey("room1", "alice"))
}

func TestReadThrough_L2Hit_SkipsLoader(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	// pre-populate L2
	seed := SubAuth{ID: "u1", Account: "alice", Roles: []model.Role{model.RoleOwner}}
	_, _, err := ReadThrough(context.Background(), fv,
		func(context.Context, string, string) (SubAuth, bool, error) { return seed, true, nil },
		"room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	// second call: loader must NOT be invoked
	got, subscribed, err := ReadThrough(context.Background(), fv,
		func(context.Context, string, string) (SubAuth, bool, error) {
			t.Fatal("loader must not run on L2 hit")
			return SubAuth{}, false, nil
		}, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, []model.Role{model.RoleOwner}, got.Roles)
	assert.GreaterOrEqual(t, rec.hit, 1)
}

func TestReadThrough_L2Miss_LoadsAndPopulates(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loads := 0
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		loads++
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	_, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, 1, loads)
	assert.Equal(t, 1, fv.setHits, "subscribed result must populate L2")
	assert.Equal(t, 1, rec.miss)
}

func TestReadThrough_NotSubscribed_NotCached(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, nil // confirmed non-subscriber
	}
	_, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "bob", time.Hour, rec)
	require.NoError(t, err)
	assert.False(t, subscribed)
	assert.Equal(t, 0, fv.setHits, "negative result must not be cached")
}

func TestReadThrough_LoaderError_Propagates_NoCache(t *testing.T) {
	fv := newFakeValkey()
	rec := &spyRecorder{}
	sentinel := errors.New("mongo down / breaker open")
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{}, false, sentinel
	}
	_, _, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 0, fv.setHits)
}

func TestReadThrough_NilClient_FailsOpenToLoader(t *testing.T) {
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := ReadThrough(context.Background(), nil, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err)
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
}

func TestReadThrough_ValkeySetError_SwallowedReturnsLoaded(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey unreachable")
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1", Account: "alice"}, true, nil
	}
	got, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err, "a Valkey Set failure must be swallowed, not fail the call")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, fv.setHits, "populate must have been attempted")
}

func TestReadThrough_ValkeyGetError_FailsOpenToLoader(t *testing.T) {
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey unreachable")
	rec := &spyRecorder{}
	loader := func(context.Context, string, string) (SubAuth, bool, error) {
		return SubAuth{ID: "u1"}, true, nil
	}
	got, subscribed, err := ReadThrough(context.Background(), fv, loader, "room1", "alice", time.Hour, rec)
	require.NoError(t, err, "a Valkey error must degrade to the loader, not fail the call")
	assert.True(t, subscribed)
	assert.Equal(t, "u1", got.ID)
	assert.Equal(t, 1, rec.err)
}

package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

func TestCachedSubStore_HitMiss(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	want := &model.Subscription{
		User:  model.SubscriptionUser{ID: "u-alice", Account: "alice"},
		Roles: []model.Role{model.RoleMember},
	}
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(want, nil).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	got, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u-alice", got.User.ID)
	assert.Equal(t, "alice", got.User.Account)
	assert.Equal(t, []model.Role{model.RoleMember}, got.Roles)

	// Second call: cache hit, inner not called again.
	got2, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u-alice", got2.User.ID)
	assert.Equal(t, "alice", got2.User.Account)
	assert.Equal(t, []model.Role{model.RoleMember}, got2.Roles)
}

func TestCachedSubStore_PreservesUserID(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	want := &model.Subscription{
		User:  model.SubscriptionUser{ID: "u-alice-123", Account: "alice"},
		Roles: []model.Role{model.RoleMember},
	}
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(want, nil).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	// Miss path: populates the cache.
	got, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u-alice-123", got.User.ID, "User.ID must survive cache write")
	assert.Equal(t, "alice", got.User.Account)

	// Hit path: must also return User.ID, not empty string.
	got2, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u-alice-123", got2.User.ID, "User.ID must survive cache hit — empty here would corrupt downstream messages")
}

func TestCachedSubStore_NotSubscribedNotCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(nil, errNotSubscribed).Times(2)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	_, err = cached.GetSubscription(context.Background(), "alice", "r1")
	assert.ErrorIs(t, err, errNotSubscribed)
	_, err = cached.GetSubscription(context.Background(), "alice", "r1")
	assert.ErrorIs(t, err, errNotSubscribed)
}

func TestCachedSubStore_TransientErrorNotCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	boom := errors.New("transient")
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(nil, boom).Times(2)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	_, err = cached.GetSubscription(context.Background(), "alice", "r1")
	assert.ErrorIs(t, err, boom)
	_, err = cached.GetSubscription(context.Background(), "alice", "r1")
	assert.ErrorIs(t, err, boom)
}

func TestCachedSubStore_TTLExpires(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	want := &model.Subscription{User: model.SubscriptionUser{Account: "alice"}, Roles: []model.Role{model.RoleMember}}
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(want, nil).Times(2)

	cached, err := newCachedSubStore(inner, 10, 50*time.Millisecond)
	require.NoError(t, err)

	_, _ = cached.GetSubscription(context.Background(), "alice", "r1")
	_, _ = cached.GetSubscription(context.Background(), "alice", "r1")
	time.Sleep(75 * time.Millisecond)
	_, _ = cached.GetSubscription(context.Background(), "alice", "r1")
}

func TestCachedSubStore_SingleflightDedupsConcurrentMisses(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	var calls atomic.Int32
	gate := make(chan struct{})
	want := &model.Subscription{User: model.SubscriptionUser{Account: "alice"}, Roles: []model.Role{model.RoleMember}}
	inner.EXPECT().
		GetSubscription(gomock.Any(), "alice", "r1").
		DoAndReturn(func(_ context.Context, _, _ string) (*model.Subscription, error) {
			calls.Add(1)
			<-gate
			return want, nil
		}).
		Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	const N = 20
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, _ = cached.GetSubscription(context.Background(), "alice", "r1")
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(gate)
	wg.Wait()
	assert.Equal(t, int32(1), calls.Load())
}

func TestCachedSubStore_GetRoomMetaPassesThrough(t *testing.T) {
	// GetRoomMeta is not cached by the sub cache; it passes through.
	// (Caching of GetRoomMeta is the metacache wrapper's job.)
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	inner.EXPECT().GetRoomMeta(gomock.Any(), "r1").Return(roommetacache.Meta{ID: "r1", UserCount: 1}, nil).Times(2)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	_, _ = cached.GetRoomMeta(context.Background(), "r1")
	_, _ = cached.GetRoomMeta(context.Background(), "r1")
}

func TestCachedSubStore_LeaderCancelDoesNotPoisonWaiters(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	want := &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}, Roles: []model.Role{model.RoleMember}}
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").DoAndReturn(
		func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-block
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return want, nil
		}).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, e := cached.GetSubscription(leaderCtx, "alice", "r1")
		leaderDone <- e
	}()
	<-entered

	waiterReady := make(chan struct{})
	waiterDone := make(chan error, 1)
	go func() {
		close(waiterReady)
		_, e := cached.GetSubscription(context.Background(), "alice", "r1")
		waiterDone <- e
	}()
	<-waiterReady

	cancelLeader()
	require.ErrorIs(t, <-leaderDone, context.Canceled)
	close(block)
	require.NoError(t, <-waiterDone, "waiter must not be poisoned by the leader's cancel")

	// Cache populated: a fresh hit does not call inner again (Times(1) enforces this).
	got, err := cached.GetSubscription(context.Background(), "alice", "r1")
	require.NoError(t, err)
	assert.Equal(t, "u1", got.User.ID)
}

func TestCachedSubStore_CallerCancelReturnsCtxErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	block := make(chan struct{})
	defer close(block)
	entered := make(chan struct{}, 1)
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").DoAndReturn(
		func(ctx context.Context, _, _ string) (*model.Subscription, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-block
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &model.Subscription{User: model.SubscriptionUser{Account: "alice"}}, nil
		}).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, e := cached.GetSubscription(ctx, "alice", "r1")
		done <- e
	}()
	<-entered
	cancel()

	select {
	case e := <-done:
		require.ErrorIs(t, e, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller did not return on its own ctx cancel within 2s")
	}
}

func TestNewCachedSubStore_RejectsInvalidArgs(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)

	tests := []struct {
		name    string
		size    int
		ttl     time.Duration
		wantErr string
	}{
		{"zero size", 0, time.Minute, "size must be positive"},
		{"negative size", -1, time.Minute, "size must be positive"},
		{"zero ttl", 10, 0, "ttl must be positive"},
		{"negative ttl", 10, -1, "ttl must be positive"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := newCachedSubStore(inner, tc.size, tc.ttl)
			assert.Nil(t, c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

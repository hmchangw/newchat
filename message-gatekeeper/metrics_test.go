package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeRecorder counts cache outcomes for assertions.
type fakeRecorder struct{ hits, misses, errs int }

func (r *fakeRecorder) Hit(context.Context)   { r.hits++ }
func (r *fakeRecorder) Miss(context.Context)  { r.misses++ }
func (r *fakeRecorder) Error(context.Context) { r.errs++ }

func TestCachedSubStore_Metrics_HitMissNotSubscribedError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	inner := NewMockStore(ctrl)
	want := &model.Subscription{User: model.SubscriptionUser{ID: "u1", Account: "alice"}}
	inner.EXPECT().GetSubscription(gomock.Any(), "alice", "r1").Return(want, nil).Times(1)
	inner.EXPECT().GetSubscription(gomock.Any(), "bob", "r1").Return(nil, errNotSubscribed).Times(1)
	inner.EXPECT().GetSubscription(gomock.Any(), "carol", "r1").Return(nil, errors.New("mongo down")).Times(1)

	cached, err := newCachedSubStore(inner, 10, time.Minute)
	require.NoError(t, err)
	rec := &fakeRecorder{}
	cached.metrics = rec

	// Miss: not cached, store returns a subscription.
	_, err = cached.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	// Hit: served from cache.
	_, err = cached.GetSubscription(ctx, "alice", "r1")
	require.NoError(t, err)
	// errNotSubscribed → clean miss, not an error.
	_, err = cached.GetSubscription(ctx, "bob", "r1")
	require.ErrorIs(t, err, errNotSubscribed)
	// Real store failure → error.
	_, err = cached.GetSubscription(ctx, "carol", "r1")
	require.Error(t, err)

	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 2, rec.misses) // alice load + bob not-subscribed
	assert.Equal(t, 1, rec.errs)
}

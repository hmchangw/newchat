package userstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/userstore"
)

// fakeRecorder counts cache outcomes for assertions.
type fakeRecorder struct{ hits, misses, errs int }

func (r *fakeRecorder) Hit(context.Context)   { r.hits++ }
func (r *fakeRecorder) Miss(context.Context)  { r.misses++ }
func (r *fakeRecorder) Error(context.Context) { r.errs++ }

func TestCache_Metrics_HitMissError(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore(model.User{ID: "u1", Account: "a1"})
	rec := &fakeRecorder{}
	cache, err := userstore.NewCache(store, 10, time.Minute, userstore.WithMetrics(rec))
	require.NoError(t, err)

	// Miss: not cached, backing load succeeds.
	_, err = cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 0, rec.errs)

	// Hit: served from cache.
	_, err = cache.FindUserByID(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 0, rec.errs)

	// Error: backing load fails.
	store.err = errors.New("mongo down")
	_, err = cache.FindUserByID(ctx, "u2")
	require.Error(t, err)
	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 1, rec.errs)
}

// A cache built without WithMetrics must still work against the package-default
// recorder and never panic.
func TestCache_Metrics_DefaultRecorderNoPanic(t *testing.T) {
	cache, err := userstore.NewCache(newFakeStore(model.User{ID: "u1"}), 10, time.Minute)
	require.NoError(t, err)
	assert.NotPanics(t, func() { _, _ = cache.FindUserByID(context.Background(), "u1") })
}

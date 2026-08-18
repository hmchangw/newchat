package histdegrade

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newUnusableStore returns a Store backed by a client that has never
// connected to a real server. It requires no network access: mongo.Connect
// only builds the client, and the returned context is already canceled, so
// every driver call fails deterministically at the context check before any
// I/O is attempted.
func newUnusableStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://192.0.2.1:27017"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Disconnect(context.Background())
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return NewStore(client.Database("histdegrade_unit_error_test")), ctx
}

func TestCachedReader_DegradedSince(t *testing.T) {
	t.Run("returns nil when no marker is set", func(t *testing.T) {
		r := NewCachedReader(func(context.Context, string) (*Marker, error) { return nil, nil }, time.Minute)
		since, err := r.DegradedSince(context.Background(), "site-a")
		require.NoError(t, err)
		assert.Nil(t, since)
	})

	t.Run("returns the marker timestamp when set", func(t *testing.T) {
		r := NewCachedReader(func(context.Context, string) (*Marker, error) {
			return &Marker{SiteID: "site-a", DegradedSince: 1700000000000}, nil
		}, time.Minute)
		since, err := r.DegradedSince(context.Background(), "site-a")
		require.NoError(t, err)
		require.NotNil(t, since)
		assert.Equal(t, int64(1700000000000), *since)
	})

	t.Run("caches within the ttl", func(t *testing.T) {
		var calls atomic.Int64
		r := NewCachedReader(func(context.Context, string) (*Marker, error) {
			calls.Add(1)
			return &Marker{SiteID: "site-a", DegradedSince: 1}, nil
		}, time.Minute)

		for range 5 {
			_, err := r.DegradedSince(context.Background(), "site-a")
			require.NoError(t, err)
		}
		assert.Equal(t, int64(1), calls.Load())
	})

	t.Run("refetches after the ttl expires", func(t *testing.T) {
		var calls atomic.Int64
		r := NewCachedReader(func(context.Context, string) (*Marker, error) {
			calls.Add(1)
			return nil, nil
		}, time.Nanosecond)

		_, err := r.DegradedSince(context.Background(), "site-a")
		require.NoError(t, err)
		_, err = r.DegradedSince(context.Background(), "site-a")
		require.NoError(t, err)
		assert.Equal(t, int64(2), calls.Load())
	})

	t.Run("propagates the loader error and does not cache it", func(t *testing.T) {
		var calls atomic.Int64
		wantErr := errors.New("mongo unavailable")
		r := NewCachedReader(func(context.Context, string) (*Marker, error) {
			calls.Add(1)
			return nil, wantErr
		}, time.Minute)

		_, err := r.DegradedSince(context.Background(), "site-a")
		require.ErrorIs(t, err, wantErr)
		_, err = r.DegradedSince(context.Background(), "site-a")
		require.ErrorIs(t, err, wantErr)
		assert.Equal(t, int64(2), calls.Load(), "errors must not be cached")
	})

	t.Run("caches per site", func(t *testing.T) {
		r := NewCachedReader(func(_ context.Context, siteID string) (*Marker, error) {
			if siteID == "site-a" {
				return &Marker{SiteID: siteID, DegradedSince: 11}, nil
			}
			return nil, nil
		}, time.Minute)

		a, err := r.DegradedSince(context.Background(), "site-a")
		require.NoError(t, err)
		require.NotNil(t, a)
		b, err := r.DegradedSince(context.Background(), "site-b")
		require.NoError(t, err)
		assert.Nil(t, b)
	})
}

func TestStore_ErrorPaths(t *testing.T) {
	t.Run("Set wraps a driver error", func(t *testing.T) {
		s, ctx := newUnusableStore(t)
		err := s.Set(ctx, "site-err", 1)
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "set history degraded marker for site-err")
	})

	t.Run("Clear wraps a driver error", func(t *testing.T) {
		s, ctx := newUnusableStore(t)
		err := s.Clear(ctx, "site-err")
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "clear history degraded marker for site-err")
	})

	t.Run("Get wraps a driver error", func(t *testing.T) {
		s, ctx := newUnusableStore(t)
		got, err := s.Get(ctx, "site-err")
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "get history degraded marker for site-err")
		assert.Nil(t, got)
	})
}

//go:build integration

package roomsubcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func setupValkey(t *testing.T) valkeyutil.Client {
	t.Helper()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	return valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
}

func TestValkeyCache_Integration_SetGetInvalidate(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	members := []Member{
		{ID: "u1", Account: "alice"},
		{ID: "u2", Account: "bob"},
	}
	require.NoError(t, cache.Set(ctx, "room-1", members, time.Minute))

	got, err := cache.Get(ctx, "room-1")
	require.NoError(t, err)
	assert.Equal(t, members, got)

	require.NoError(t, cache.Invalidate(ctx, "room-1"))

	_, err = cache.Get(ctx, "room-1")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Integration_MissOnUnsetRoom(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	_, err := cache.Get(ctx, "never-set")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

func TestValkeyCache_Integration_TTLExpires(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "room-ttl", []Member{{ID: "u1", Account: "a"}}, time.Second))

	// Poll for expiry — Valkey honors TTL with sub-second granularity but
	// asserting on a precise deadline is flaky. Allow up to 5s.
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = cache.Get(ctx, "room-ttl")
		if lastErr != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.ErrorIs(t, lastErr, valkeyutil.ErrCacheMiss, "expected key to expire within 5s")
}

func TestValkeyCache_Integration_EmptyListIsCacheHit(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "empty-room", []Member{}, time.Minute))

	got, err := cache.Get(ctx, "empty-room")
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

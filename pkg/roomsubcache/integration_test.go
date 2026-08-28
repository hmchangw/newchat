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

	got, ok := cache.Get(ctx, "room-1")
	require.True(t, ok)
	assert.Equal(t, members, got.V)

	cache.Invalidate(ctx, "room-1")

	_, ok = cache.Get(ctx, "room-1")
	assert.False(t, ok)
}

func TestValkeyCache_Integration_MissOnUnsetRoom(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	_, ok := cache.Get(ctx, "never-set")
	assert.False(t, ok)
}

func TestValkeyCache_Integration_TTLExpires(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "room-ttl", []Member{{ID: "u1", Account: "a"}}, time.Second))

	// Poll for expiry — Valkey honors TTL with sub-second granularity but
	// asserting on a precise deadline is flaky. Allow up to 5s.
	deadline := time.Now().Add(5 * time.Second)
	served := true
	for time.Now().Before(deadline) {
		if _, served = cache.Get(ctx, "room-ttl"); !served {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.False(t, served, "expected key to expire within 5s")
}

func TestValkeyCache_Integration_EmptyListIsCacheHit(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "empty-room", []Member{}, time.Minute))

	got, ok := cache.Get(ctx, "empty-room")
	require.True(t, ok)
	assert.Empty(t, got.V, "an empty member list must round-trip as a hit, not a miss")
	assert.NotZero(t, got.CachedAt, "a hit must carry a confirmation stamp, or it can never be refreshed")
}

// Slide is what keeps delivery alive: when Mongo cannot re-confirm an entry,
// the deadline is pushed back and the cached members are served anyway. If it
// did not extend a real TTL the entry would expire mid-outage and fan-out
// would start failing, so this exercises it against a real server rather than
// a fake.
func TestValkeyCache_Integration_SlideExtendsTheDeadline(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "room-slide", []Member{{ID: "u1", Account: "alice"}}, 2*time.Second))
	cache.Slide(ctx, "room-slide", time.Hour)

	// Well past the original 2s deadline: without the slide this key is gone.
	time.Sleep(3 * time.Second)

	got, ok := cache.Get(ctx, "room-slide")
	require.True(t, ok, "the slid entry must outlive its original TTL")
	assert.Equal(t, []Member{{ID: "u1", Account: "alice"}}, got.V)
}

// The slide must use EXPIRE, not SET. A membership change can bust the entry
// between the read and the slide, and a slide that rewrote the value would
// resurrect members who were just removed — handing back access the source of
// truth had already withdrawn.
func TestValkeyCache_Integration_SlideCannotResurrectAnInvalidatedEntry(t *testing.T) {
	client := setupValkey(t)
	cache := NewValkeyCache(client)
	ctx := context.Background()

	require.NoError(t, cache.Set(ctx, "room-busted", []Member{{ID: "u1", Account: "alice"}}, time.Minute))
	cache.Invalidate(ctx, "room-busted")

	cache.Slide(ctx, "room-busted", time.Hour) // sliding an absent key is a no-op

	_, ok := cache.Get(ctx, "room-busted")
	assert.False(t, ok, "a slide must never resurrect an invalidated entry")
}

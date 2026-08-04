//go:build integration

package badgecache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func TestBadgeCache_BumpMissThenSeedThenBump(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	_, ok := c.Bump(ctx, "alice", "roomB")
	assert.False(t, ok, "no key yet → miss")

	n, ok := c.Seed(ctx, "alice", []string{"roomA"}, "roomB")
	require.True(t, ok)
	assert.Equal(t, 2, n, "seed ∪ trigger")

	n, ok = c.Bump(ctx, "alice", "roomB")
	require.True(t, ok)
	assert.Equal(t, 2, n, "SADD idempotent")

	n, ok = c.Bump(ctx, "alice", "roomC")
	require.True(t, ok)
	assert.Equal(t, 3, n)

	c.ClearRoom(ctx, "alice", "roomA")
	n, _ = c.Bump(ctx, "alice", "roomC")
	assert.Equal(t, 2, n)

	c.ClearAll(ctx, "alice")
	_, ok = c.Bump(ctx, "alice", "roomC")
	assert.False(t, ok, "cleared key → miss again")
}

func TestBadgeCache_CapAt10(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	rooms := make([]string, 15)
	for i := range rooms {
		rooms[i] = fmt.Sprintf("room%02d", i)
	}
	n, ok := c.Seed(context.Background(), "alice", rooms, "roomX")
	require.True(t, ok)
	assert.Equal(t, 10, n)
}

// TestBadgeCache_ClearRoomAndClearAll_MissingKey_NoPanic locks in the fail-open
// contract: clearing an account with no cache entry (never seeded, or already
// expired/cleared) must be a silent no-op — never a panic, never an error surfaced.
func TestBadgeCache_ClearRoomAndClearAll_MissingKey_NoPanic(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.ClearRoom(ctx, "bob", "roomA") })
	assert.NotPanics(t, func() { c.ClearAll(ctx, "bob") })

	// still a miss afterward — no key was accidentally created.
	_, ok := c.Bump(ctx, "bob", "roomA")
	assert.False(t, ok)
}

// TestBadgeCache_Reseed_ReplacesPriorContents confirms Reseed is a full
// replace (DEL + SADD), not a merge — stale room IDs from a prior seed/bump
// must not survive a Reseed that omits them.
func TestBadgeCache_Reseed_ReplacesPriorContents(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	n, ok := c.Seed(ctx, "carol", []string{"roomA", "roomB"}, "roomC")
	require.True(t, ok)
	require.Equal(t, 3, n)

	c.Reseed(ctx, "carol", []string{"roomX", "roomY"})

	n, ok = c.Bump(ctx, "carol", "roomX")
	require.True(t, ok, "reseeded key must exist")
	assert.Equal(t, 2, n, "roomX already present + roomY, roomA/roomB/roomC dropped")

	members, err := rdb.SMembers(ctx, Key("carol")).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"roomX", "roomY"}, members)
}

// TestBadgeCache_Reseed_EmptyRoomIDs_JustDeletes confirms an empty roomIDs
// slice degrades Reseed to a plain delete (no key recreated).
func TestBadgeCache_Reseed_EmptyRoomIDs_JustDeletes(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "dave", []string{"roomA"}, "roomB")
	require.True(t, ok)

	c.Reseed(ctx, "dave", nil)

	_, ok = c.Bump(ctx, "dave", "roomA")
	assert.False(t, ok, "reseeding with no rooms leaves the key deleted")
}

// TestBadgeCache_Bump_RefreshesTTL confirms a Bump on an existing key extends
// its expiry rather than just touching membership.
func TestBadgeCache_Bump_RefreshesTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "erin", []string{"roomA"}, "roomB")
	require.True(t, ok)

	_, ok = c.Bump(ctx, "erin", "roomC")
	require.True(t, ok)

	ttl, err := rdb.TTL(ctx, Key("erin")).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "TTL must be refreshed and positive after Bump")
}

// TestBadgeCache_ValkeyError_FailsOpen points the cache at a port nothing
// listens on so every call fails at the transport layer, exercising the
// fail-open contract (no error return, no panic, ok=false for count methods)
// on all six methods without touching the shared cluster.
func TestBadgeCache_ValkeyError_FailsOpen(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	c := New(rdb, time.Hour)
	ctx := context.Background()

	_, ok := c.Bump(ctx, "frank", "roomA")
	assert.False(t, ok)

	_, ok = c.Seed(ctx, "frank", []string{"roomA"}, "roomB")
	assert.False(t, ok)

	assert.NotPanics(t, func() { c.ClearRoom(ctx, "frank", "roomA") })
	assert.NotPanics(t, func() { c.ClearAll(ctx, "frank") })
	assert.NotPanics(t, func() { c.Reseed(ctx, "frank", []string{"roomA"}) })
}

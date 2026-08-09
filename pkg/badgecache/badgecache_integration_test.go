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
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	assert.Empty(t, c.BumpBatch(ctx, []string{"alice"}, "roomB"), "no key yet → miss")

	n, ok := c.Seed(ctx, "alice", []string{"roomA"}, "roomB")
	require.True(t, ok)
	assert.Equal(t, 2, n, "seed ∪ trigger")

	assert.Equal(t, map[string]int{"alice": 2}, c.BumpBatch(ctx, []string{"alice"}, "roomB"), "SADD idempotent")

	assert.Equal(t, map[string]int{"alice": 3}, c.BumpBatch(ctx, []string{"alice"}, "roomC"))

	c.ClearRoom(ctx, "alice", "roomA")
	assert.Equal(t, map[string]int{"alice": 2}, c.BumpBatch(ctx, []string{"alice"}, "roomC"))

	c.ClearAll(ctx, "alice")
	assert.Empty(t, c.BumpBatch(ctx, []string{"alice"}, "roomC"), "cleared key → miss again")
}

func TestBadgeCache_CapAt10(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
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
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.ClearRoom(ctx, "bob", "roomA") })
	assert.NotPanics(t, func() { c.ClearAll(ctx, "bob") })

	// still a miss afterward — no key was accidentally created.
	assert.Empty(t, c.BumpBatch(ctx, []string{"bob"}, "roomA"))
}

// TestBadgeCache_Reseed_ReplacesPriorContents confirms Reseed is a full
// replace (DEL + SADD), not a merge — stale room IDs from a prior seed/bump
// must not survive a Reseed that omits them.
func TestBadgeCache_Reseed_ReplacesPriorContents(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	n, ok := c.Seed(ctx, "carol", []string{"roomA", "roomB"}, "roomC")
	require.True(t, ok)
	require.Equal(t, 3, n)

	c.Reseed(ctx, "carol", []string{"roomX", "roomY"})

	assert.Equal(t, map[string]int{"carol": 2}, c.BumpBatch(ctx, []string{"carol"}, "roomX"),
		"reseeded key must exist: roomX already present + roomY, roomA/roomB/roomC dropped")

	members, err := rdb.SMembers(ctx, Key("carol")).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"roomX", "roomY"}, members)
}

// TestBadgeCache_Reseed_EmptyRoomIDs_JustDeletes confirms an empty roomIDs
// slice degrades Reseed to a plain delete (no key recreated).
func TestBadgeCache_Reseed_EmptyRoomIDs_JustDeletes(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "dave", []string{"roomA"}, "roomB")
	require.True(t, ok)

	c.Reseed(ctx, "dave", nil)

	assert.Empty(t, c.BumpBatch(ctx, []string{"dave"}, "roomA"), "reseeding with no rooms leaves the key deleted")
}

// TestBadgeCache_Bump_RefreshesTTL confirms a bump on an existing key extends
// its expiry rather than just touching membership.
func TestBadgeCache_Bump_RefreshesTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "erin", []string{"roomA"}, "roomB")
	require.True(t, ok)

	require.Contains(t, c.BumpBatch(ctx, []string{"erin"}, "roomC"), "erin")

	ttl, err := rdb.TTL(ctx, Key("erin")).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0), "TTL must be refreshed and positive after bump")
}

// TestBadgeCache_BumpBatch_MixedHitMiss pipelines a batch across seeded and
// unseeded accounts: seeded accounts come back with their post-add size,
// unseeded ones are simply absent (the caller's cue to Seed from Mongo).
func TestBadgeCache_BumpBatch_MixedHitMiss(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "gina", []string{"roomA"}, "")
	require.True(t, ok)
	_, ok = c.Seed(ctx, "hank", []string{"roomA", "roomB"}, "")
	require.True(t, ok)
	// "iris" is never seeded → miss.

	counts := c.BumpBatch(ctx, []string{"gina", "hank", "iris"}, "roomNew")
	assert.Equal(t, map[string]int{"gina": 2, "hank": 3}, counts)

	// The batch's SADD really landed and is idempotent on repeat.
	counts = c.BumpBatch(ctx, []string{"gina"}, "roomNew")
	assert.Equal(t, map[string]int{"gina": 2}, counts)

	// TTL refreshed on the batch path too.
	ttl, err := rdb.TTL(ctx, Key("hank")).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))
}

// TestBadgeCache_BumpBatch_NoScriptSelfHeals flushes the script cache after a
// seed, so the pipelined EVALSHA returns NOSCRIPT — the pipelined EVAL retry
// pass must still produce the count (and re-cache the script's SHA).
func TestBadgeCache_BumpBatch_NoScriptSelfHeals(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "jane", []string{"roomA"}, "")
	require.True(t, ok)

	// Empty every master's script cache; the seeded DATA key survives.
	err := rdb.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		return master.ScriptFlush(ctx).Err()
	})
	require.NoError(t, err)

	counts := c.BumpBatch(ctx, []string{"jane"}, "roomB")
	assert.Equal(t, map[string]int{"jane": 2}, counts, "NOSCRIPT must fall back per-account, not surface as a miss")
}

// TestBadgeCache_BumpBatch_EmptyAccounts must not touch Valkey at all — a nil
// client would panic on any command.
func TestBadgeCache_BumpBatch_EmptyAccounts(t *testing.T) {
	c := New(nil, time.Hour, DefaultMaxCount)
	counts := c.BumpBatch(context.Background(), nil, "roomA")
	assert.Empty(t, counts)
}

// TestBadgeCache_ValkeyError_FailsOpen points the cache at a port nothing
// listens on so every call fails at the transport layer, exercising the
// fail-open contract (no error return, no panic, ok=false for count methods)
// on all six methods without touching the shared cluster.
func TestBadgeCache_ValkeyError_FailsOpen(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	assert.Empty(t, c.BumpBatch(ctx, []string{"frank", "grace"}, "roomA"), "batch fails open to all-absent")

	_, ok := c.Seed(ctx, "frank", []string{"roomA"}, "roomB")
	assert.False(t, ok)

	assert.NotPanics(t, func() { c.ClearRoom(ctx, "frank", "roomA") })
	assert.NotPanics(t, func() { c.ClearAll(ctx, "frank") })
	assert.NotPanics(t, func() { c.Reseed(ctx, "frank", []string{"roomA"}) })
}

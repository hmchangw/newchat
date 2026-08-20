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

// TestBadgeCache_Reseed_EmptyRoomIDs_DeletesSetKeepsFresh confirms an empty
// roomIDs slice deletes the set (no stale members survive) while stamping the
// freshness marker — the recorded state is "fresh, zero unread", not absence.
func TestBadgeCache_Reseed_EmptyRoomIDs_DeletesSetKeepsFresh(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "dave", []string{"roomA"}, "roomB")
	require.True(t, ok)

	c.Reseed(ctx, "dave", nil)

	exists, err := rdb.Exists(ctx, Key("dave")).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "empty reseed must delete the set key")
	n, fresh := c.Count(ctx, "dave")
	require.True(t, fresh, "empty reseed records fresh zero, not absence")
	assert.Equal(t, 0, n)
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

// TestBadgeCache_Marker_FreshZeroAfterEmptyReseed: an empty reseed records
// "fresh, zero unread" — Count serves 0 without a recompute, and a bump on the
// marker-only state creates the set (count 1) instead of missing.
func TestBadgeCache_Marker_FreshZeroAfterEmptyReseed(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", nil)

	n, fresh := c.Count(ctx, "alice")
	require.True(t, fresh, "empty reseed must leave a fresh marker")
	assert.Equal(t, 0, n)

	counts := c.BumpBatch(ctx, []string{"alice"}, "roomA")
	assert.Equal(t, map[string]int{"alice": 1}, counts, "marker-only state must be a hit that creates the set")

	n, fresh = c.Count(ctx, "alice")
	require.True(t, fresh)
	assert.Equal(t, 1, n)
}

// TestBadgeCache_Count_StaleWithoutMarker: no marker → fresh=false regardless
// of set contents (legacy sets self-migrate via the caller's recompute).
func TestBadgeCache_Count_StaleWithoutMarker(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, fresh := c.Count(ctx, "bob")
	assert.False(t, fresh, "no state at all → stale")

	// Simulate a legacy set written without a marker.
	require.NoError(t, rdb.SAdd(ctx, Key("bob"), "roomA").Err())
	_, fresh = c.Count(ctx, "bob")
	assert.False(t, fresh, "set without marker → stale")
}

// TestBadgeCache_ClearSemantics_Marker: ClearRoom is an exact removal so the
// marker survives; ClearAll must delete the marker with the set.
func TestBadgeCache_ClearSemantics_Marker(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	_, ok := c.Seed(ctx, "carol", []string{"roomA", "roomB"}, "")
	require.True(t, ok)

	c.ClearRoom(ctx, "carol", "roomA")
	n, fresh := c.Count(ctx, "carol")
	require.True(t, fresh, "ClearRoom is an exact removal — marker must survive")
	assert.Equal(t, 1, n)

	c.ClearAll(ctx, "carol")
	_, fresh = c.Count(ctx, "carol")
	assert.False(t, fresh, "ClearAll must delete the marker with the set")
}

// TestBadgeCache_Count_Uncapped: Count reports the true set size — the 10-cap
// applies only to Bump/Seed returns.
func TestBadgeCache_Count_Uncapped(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	rooms := make([]string, 15)
	for i := range rooms {
		rooms[i] = fmt.Sprintf("room%02d", i)
	}
	_, ok := c.Seed(context.Background(), "dave", rooms, "")
	require.True(t, ok)
	n, fresh := c.Count(context.Background(), "dave")
	require.True(t, fresh)
	assert.Equal(t, 15, n)
}

// A bump is not a verification: it must add the room without extending the
// marker's countdown, so the staleness bound keeps running.
func TestBadgeCache_BumpDoesNotExtendMarkerTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount, WithMarkerTTL(30*time.Second))
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})
	before, err := rdb.PTTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	require.Positive(t, before)

	// Wait past the resolution of PTTL so a refresh would be visible as an
	// increase. This observes a Valkey TTL counting down in real time — there
	// is no sync-primitive substitute for watching a countdown elapse, so the
	// CLAUDE.md ban on time.Sleep for goroutine synchronization does not apply.
	time.Sleep(1100 * time.Millisecond)
	assert.Equal(t, map[string]int{"alice": 2}, c.BumpBatch(ctx, []string{"alice"}, "roomB"))

	after, err := rdb.PTTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	assert.Less(t, after, before, "bump must not re-stamp the marker")
}

// TestBadgeCache_Seed_StampsMarkerAndSetTTLs confirms Seed (not just Reseed)
// stamps the marker with markerTTL and the set with ttl, asserting both
// PTTLs directly (§9: "Seed/Reseed stamp the marker with markerTTL, the set
// with ttl (assert both PTTLs)").
func TestBadgeCache_Seed_StampsMarkerAndSetTTLs(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount, WithMarkerTTL(30*time.Second))
	ctx := context.Background()

	n, ok := c.Seed(ctx, "alice", []string{"roomA"}, "roomB")
	require.True(t, ok)
	require.Equal(t, 2, n)

	setPTTL, err := rdb.PTTL(ctx, Key("alice")).Result()
	require.NoError(t, err)
	markerPTTL, err := rdb.PTTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)

	assert.Positive(t, setPTTL)
	assert.Positive(t, markerPTTL)
	assert.LessOrEqual(t, markerPTTL, 30*time.Second, "marker stamped with markerTTL")
	assert.Greater(t, setPTTL, markerPTTL, "set stamped with the longer ttl")
}

// TestBadgeCache_Seed_ZeroRoomIDsEmptyTrigger exercises seedScript's
// #ARGV > 2 false branch directly via Seed: no roomIDs and an empty trigger
// means nothing to SADD, so the set must be absent while the marker is still
// stamped — the legitimate "fresh, zero unread" state.
func TestBadgeCache_Seed_ZeroRoomIDsEmptyTrigger(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	n, ok := c.Seed(ctx, "alice", nil, "")
	require.True(t, ok)
	assert.Equal(t, 0, n)

	exists, err := rdb.Exists(ctx, Key("alice")).Result()
	require.NoError(t, err)
	assert.Zero(t, exists, "no roomIDs and no trigger must leave the set absent")

	n, fresh := c.Count(ctx, "alice")
	require.True(t, fresh, "marker must still be stamped: fresh, zero unread")
	assert.Equal(t, 0, n)
}

// Marker and set carry independent lifetimes; the marker is the shorter one.
func TestBadgeCache_MarkerTTLShorterThanSetTTL(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount, WithMarkerTTL(30*time.Second))
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})

	setTTL, err := rdb.TTL(ctx, Key("alice")).Result()
	require.NoError(t, err)
	markerTTL, err := rdb.TTL(ctx, MarkerKey("alice")).Result()
	require.NoError(t, err)
	assert.Greater(t, setTTL, markerTTL)
	assert.LessOrEqual(t, markerTTL, 30*time.Second)
}

// An expired marker means the set is unverified: bump must miss so the caller
// recomputes, even though the set is still present.
func TestBadgeCache_BumpMissesWhenMarkerExpiredButSetSurvives(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"roomA"})
	require.NoError(t, rdb.Del(ctx, MarkerKey("alice")).Err()) // simulate marker expiry
	require.Equal(t, int64(1), rdb.Exists(ctx, Key("alice")).Val(), "set still present")

	assert.Empty(t, c.BumpBatch(ctx, []string{"alice"}, "roomB"), "unverified set ⇒ miss")

	n, fresh := c.Count(ctx, "alice")
	assert.False(t, fresh)
	assert.Zero(t, n)
}

// On a miss the caller holds fresh Mongo truth, and any surviving set is
// unverified — so Seed replaces rather than unions.
func TestBadgeCache_SeedReplacesUnverifiedSet(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", []string{"stale1", "stale2"})
	require.NoError(t, rdb.Del(ctx, MarkerKey("alice")).Err())

	n, ok := c.Seed(ctx, "alice", []string{"fresh1"}, "trigger1")
	require.True(t, ok)
	assert.Equal(t, 2, n, "fresh1 ∪ trigger1 only — the stale set must not survive")

	members, err := rdb.SMembers(ctx, Key("alice")).Result()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fresh1", "trigger1"}, members)
}

// Marker-present/set-absent (fresh all-read) is still a hit that recreates the set.
func TestBadgeCache_BumpHitsOnMarkerOnlyState(t *testing.T) {
	rdb := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	c := New(rdb, time.Hour, DefaultMaxCount)
	ctx := context.Background()

	c.Reseed(ctx, "alice", nil) // fresh, zero unread: marker only
	require.Equal(t, int64(0), rdb.Exists(ctx, Key("alice")).Val())

	assert.Equal(t, map[string]int{"alice": 1}, c.BumpBatch(ctx, []string{"alice"}, "roomA"))
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

	_, fresh := c.Count(ctx, "frank")
	assert.False(t, fresh, "Valkey error → stale (caller recomputes)")

	_, ok := c.Seed(ctx, "frank", []string{"roomA"}, "roomB")
	assert.False(t, ok)

	assert.NotPanics(t, func() { c.ClearRoom(ctx, "frank", "roomA") })
	assert.NotPanics(t, func() { c.ClearAll(ctx, "frank") })
	assert.NotPanics(t, func() { c.Reseed(ctx, "frank", []string{"roomA"}) })
}

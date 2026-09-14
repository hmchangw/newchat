//go:build integration

package roomtimescache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func newTier(t *testing.T, ttl time.Duration) (*Tier, valkeyutil.Client) {
	t.Helper()
	cluster := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	client := valkeyutil.WrapClusterClient(cluster)
	return NewTier(client, ttl, nil), client
}

// The round trip has to survive real JSON through a real cluster, not just the
// in-memory fake: the wire format is what a future build has to keep reading.
func TestTier_StoreThenFallback_Integration(t *testing.T) {
	tier, _ := newTier(t, time.Minute)
	ctx := context.Background()

	created := time.UnixMilli(1_600_000_000_000).UTC()
	tier.Store(ctx, "room-1", created)

	got, found := tier.Fallback(ctx, "room-1")

	require.True(t, found)
	assert.Equal(t, created, got)
}

func TestTier_Fallback_MissOnAnUnknownRoom_Integration(t *testing.T) {
	tier, _ := newTier(t, time.Minute)

	_, found := tier.Fallback(context.Background(), "never-stored")

	assert.False(t, found)
}

// The key carries a {room} hash tag, so both generations of a room's entries
// live in one slot and a multi-key delete over one room cannot go CROSSSLOT.
func TestTier_KeyIsColocatedWithTheRoomsOtherEntries_Integration(t *testing.T) {
	cluster := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	ctx := context.Background()

	slotA, err := cluster.ClusterKeySlot(ctx, Key("room-1")).Result()
	require.NoError(t, err)
	slotB, err := cluster.ClusterKeySlot(ctx, "sub:{room-1}:alice").Result()
	require.NoError(t, err)

	assert.Equal(t, slotB, slotA, "room-times must share the room's slot")
}

// Serving a fallback re-arms the deadline, so a room stays cheap to read for as
// long as the outage lasts rather than expiring partway through it.
func TestTier_Fallback_ExtendsTheDeadline_Integration(t *testing.T) {
	cluster := testutil.SharedValkeyCluster(t)
	t.Cleanup(func() { testutil.FlushValkey(t) })
	client := valkeyutil.WrapClusterClient(cluster)
	ctx := context.Background()

	// Store on a short TTL, then let it decay before reading it back.
	shortLived := NewTier(client, 30*time.Second, nil)
	shortLived.Store(ctx, "room-1", time.UnixMilli(1_600_000_000_000).UTC())

	longLived := NewTier(client, 10*time.Minute, nil)
	_, found := longLived.Fallback(ctx, "room-1")
	require.True(t, found)

	ttl, err := cluster.TTL(ctx, Key("room-1")).Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, 30*time.Second, "the read must have pushed the deadline out")
}

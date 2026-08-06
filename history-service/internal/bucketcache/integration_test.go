//go:build integration

package bucketcache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestCache_ReactionsRoundTripThroughValkey is the reason the codec is gob, not
// JSON: models.Message.Reactions is a struct-keyed, marshal-only map. This proves
// a reacted message survives the gob → Valkey → gob round-trip losslessly.
func TestCache_ReactionsRoundTripThroughValkey(t *testing.T) {
	t.Cleanup(func() { testutil.FlushValkey(t) })
	vk := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	c, err := NewCache(vk, 1<<20, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	reacted := time.Unix(1000, 0).UTC()
	msgs := []models.Message{{
		RoomID:    "r1",
		MessageID: "m1",
		CreatedAt: time.Unix(500, 0).UTC(),
		Reactions: cassandra.Reactions{
			{Emoji: "👍", UserAccount: "u1"}: {UserID: "id1", Account: "u1", ReactedAt: reacted},
		},
	}}
	c.Put(ctx, "r1", 100, msgs)

	// A fresh Cache instance has a cold L1, forcing the read to come from Valkey.
	c2, err := NewCache(vk, 1<<20, time.Minute)
	require.NoError(t, err)
	got, ok := c2.Get(ctx, "r1", 100)
	require.True(t, ok, "served from shared L2 (Valkey)")
	require.Len(t, got, 1)
	require.Len(t, got[0].Reactions, 1)

	ri, present := got[0].Reactions[cassandra.ReactionKey{Emoji: "👍", UserAccount: "u1"}]
	require.True(t, present, "struct-keyed reaction survived the gob round-trip")
	assert.Equal(t, "id1", ri.UserID)
	assert.True(t, reacted.Equal(ri.ReactedAt))
}

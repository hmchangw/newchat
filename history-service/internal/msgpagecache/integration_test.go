//go:build integration

package msgpagecache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

func TestMain(m *testing.M) { testutil.RunTests(m) }

func setupValkey(t *testing.T) valkeyutil.Client {
	t.Helper()
	t.Cleanup(func() { testutil.FlushValkey(t) })
	return valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
}

// sampleReactions builds a struct-keyed reaction map — the value that is not
// JSON-round-trippable and is the reason the cache codec is gob.
func sampleReactions() models.Reactions {
	return models.Reactions{
		models.ReactionKey{Emoji: "👍", UserAccount: "u2"}: models.ReactorInfo{
			UserID: "u2-id", EngName: "Bob", Account: "u2", ReactedAt: time.Unix(1_700_000_000, 0).UTC(),
		},
	}
}

// TestReader_L2RoundTrip_PreservesReactions is the crux integration check: a
// page carrying a struct-keyed Reactions map is cached to real Valkey by one
// replica and served to a second replica (cold L1) with the reactions intact —
// proving the gob blob survives the go-redis round-trip losslessly.
func TestReader_L2RoundTrip_PreservesReactions(t *testing.T) {
	pinClock(t)
	client := setupValkey(t)
	ctx := context.Background()

	inner1 := newStubReader("hello")
	inner1.reactions = sampleReactions()
	r1 := newTestReader(t, inner1, client)

	got1, err := r1.GetMessagesBefore(ctx, "room-x", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got1.Data, 1)
	require.Equal(t, inner1.reactions, got1.Data[0].Reactions)
	require.Equal(t, 1, inner1.count("before"))

	// Second replica: cold L1, shared L2 → serves from Valkey without touching Cassandra.
	inner2 := newStubReader("different")
	r2 := newTestReader(t, inner2, client)
	got2, err := r2.GetMessagesBefore(ctx, "room-x", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	require.Len(t, got2.Data, 1)
	assert.Equal(t, "hello", got2.Data[0].Msg, "served from shared L2, not the second reader")
	assert.Equal(t, sampleReactions(), got2.Data[0].Reactions, "struct-keyed reactions survived the gob+Valkey round-trip")
	assert.Equal(t, 0, inner2.count("before"), "L2 hit must not hit the backing reader")
}

func TestReader_Bump_InvalidatesAcrossReplicas(t *testing.T) {
	pinClock(t)
	client := setupValkey(t)
	ctx := context.Background()

	writer := newTestReader(t, newStubReader("v1"), client)
	_, err := writer.GetMessagesBefore(ctx, "room-y", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)

	// A different replica reads the fresh value, bumps, then must reload.
	inner := newStubReader("v1")
	reader := newTestReader(t, inner, client)
	_, err = reader.GetMessagesBefore(ctx, "room-y", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 0, inner.count("before"), "served from shared L2")

	reader.Bump(ctx, "room-y")
	_, err = reader.GetMessagesBefore(ctx, "room-y", sealedBefore(), deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 1, inner.count("before"), "after bump the generation advanced → reload")
}

func TestReader_HotBucket_WritesNoValkeyKeys(t *testing.T) {
	pinClock(t)
	client := setupValkey(t)
	ctx := context.Background()

	inner := newStubReader("hot")
	r := newTestReader(t, inner, client)

	_, err := r.GetMessagesBefore(ctx, "room-z", fixedNow, deepFloor(), pageReq())
	require.NoError(t, err)
	require.Equal(t, 1, inner.count("before"))

	// A second identical call must also be live (no caching of the hot bucket).
	_, err = r.GetMessagesBefore(ctx, "room-z", fixedNow, deepFloor(), pageReq())
	require.NoError(t, err)
	assert.Equal(t, 2, inner.count("before"), "the current bucket is never cached")
}

//go:build integration

package atrest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/testutil"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// TestMain drives testutil's container cleanup for this package's integration
// tests. pkg/atrest has no other TestMain — see Step 1.
func TestMain(m *testing.M) { testutil.RunTests(m) }

// TestL2DEKStore_SurvivesMongoOutage is the end-to-end proof of this feature:
// a room whose wrapped DEK is warm in Valkey still resolves while the Mongo DEK
// store is unusable, and a cold room does not.
func TestL2DEKStore_SurvivesMongoOutage(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "atrest_dek_l2")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	mongoStore := NewMongoDEKStore(db.Collection(CollectionName))
	row := RoomDataKey{ID: "room1", WrappedDEK: []byte("wrapped-ciphertext"), CreatedAt: time.Now().UTC().Truncate(time.Millisecond)}
	require.NoError(t, mongoStore.Upsert(ctx, row))

	breaker := circuitbreaker.New(1, 50*time.Millisecond)
	store := NewL2DEKStore(mongoStore, valkey, time.Hour, breaker, nil)

	// Warm the L2 while Mongo is healthy.
	got, err := store.Get(ctx, "room1")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, row.WrappedDEK, got.WrappedDEK)

	// Simulate the Mongo outage with a store whose server is unreachable, while
	// the SAME (already warmed) Valkey L2 stays healthy.
	outageStore := NewL2DEKStore(unreachableDEKStore(t), valkey, time.Hour,
		circuitbreaker.New(1, 50*time.Millisecond), nil)

	warm, err := outageStore.Get(ctx, "room1")
	require.NoError(t, err, "warm room must resolve from the L2 during a Mongo outage")
	require.NotNil(t, warm)
	assert.Equal(t, row.WrappedDEK, warm.WrappedDEK)

	// The other half of the claim: a cold room has nowhere to fall back to, so
	// the outage must surface rather than be papered over as an absent DEK.
	_, err = outageStore.Get(ctx, "cold-room-never-warmed")
	require.Error(t, err, "a cold room must surface the Mongo failure, not resolve to (nil, nil)")
}

// unreachableDEKStore returns a Mongo-backed DEK store whose server is
// genuinely unreachable, so every operation fails with a server-selection
// error and the circuit breaker actually sees it. Pointing at a nonexistent
// DATABASE would not do: FindOne then returns ErrNoDocuments, which
// mongoDEKStore.Get maps to (nil, nil) — a success that models no outage.
func unreachableDEKStore(t *testing.T) DEKStore {
	t.Helper()
	// 127.0.0.1:1 refuses immediately; the short selection timeout keeps the
	// test fast if the platform blackholes the connect instead.
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://127.0.0.1:1/").
		SetServerSelectionTimeout(500 * time.Millisecond).
		SetConnectTimeout(500 * time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := client.Disconnect(context.Background()); err != nil {
			t.Logf("disconnect unreachable mongo client: %v", err)
		}
	})
	return NewMongoDEKStore(client.Database("chat").Collection(CollectionName))
}

// TestL2DEKStore_ReplaceInvalidates_Integration proves rotation correctness
// against a real Valkey: after Replace, the next read sees the new wrapped DEK.
func TestL2DEKStore_ReplaceInvalidates_Integration(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "atrest_dek_l2_rot")
	valkey := valkeyutil.WrapClusterClient(testutil.SharedValkeyCluster(t))
	t.Cleanup(func() { testutil.FlushValkey(t) })

	mongoStore := NewMongoDEKStore(db.Collection(CollectionName))
	require.NoError(t, mongoStore.Upsert(ctx, RoomDataKey{ID: "room2", WrappedDEK: []byte("old"), CreatedAt: time.Now().UTC()}))

	store := NewL2DEKStore(mongoStore, valkey, time.Hour, circuitbreaker.New(5, time.Second), nil)
	_, err := store.Get(ctx, "room2") // warm
	require.NoError(t, err)

	require.NoError(t, store.Replace(ctx, RoomDataKey{ID: "room2", WrappedDEK: []byte("rewrapped"), CreatedAt: time.Now().UTC()}))

	got, err := store.Get(ctx, "room2")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, []byte("rewrapped"), got.WrappedDEK, "rotation must not be masked by a stale L2 entry")
}

//go:build integration

package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestConnectRead_ConnectsAndReads(t *testing.T) {
	ctx := context.Background()
	client, err := ConnectRead(ctx, testutil.MongoURI(t), "", "")
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_connect_read_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	_, err = db.Collection("docs").InsertOne(ctx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

func TestConnect_UnreachableMongo_FailsWithoutDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), unreachableMongo, "", "",
		WithPool(fastFailPool()))
	require.Error(t, err)
	assert.Nil(t, client)
	assert.Contains(t, err.Error(), "mongo ping")
}

func TestConnect_UnreachableMongo_SucceedsWithDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), unreachableMongo, "", "",
		WithPool(fastFailPool()), WithDegradedStart())
	require.NoError(t, err)
	require.NotNil(t, client)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	// The client is usable in the sense that calls fail cleanly rather than panic.
	err = client.Database("degraded").Collection("docs").
		FindOne(context.Background(), bson.M{"_id": "x"}).Err()
	require.Error(t, err)
	assert.NotErrorIs(t, err, mongo.ErrNoDocuments)
}

func TestConnect_ReachableMongo_DegradedStartUnchangedHappyPath(t *testing.T) {
	ctx := context.Background()
	client, err := Connect(ctx, testutil.MongoURI(t), "", "", WithDegradedStart())
	require.NoError(t, err)
	t.Cleanup(func() { Disconnect(context.Background(), client) })

	db := client.Database("mongoutil_degraded_start_test")
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	_, err = db.Collection("docs").InsertOne(ctx, bson.M{"_id": "x"})
	require.NoError(t, err)
	n, err := db.Collection("docs").CountDocuments(ctx, bson.M{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
}

// testutil's Mongo has no --auth, so any credentials fail SCRAM with 18: the server answered and said no.
func TestConnect_RejectedCredentials_FatalEvenWithDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), testutil.MongoURI(t), "nobody", "wrong",
		WithPool(fastFailPool()), WithDegradedStart())
	requireAuthRejected(t, client, err)
}

// With MinPoolSize > 0 the warm-up connections fail SCRAM before the ping's checkout, so the rejection
// must come from the pool event; the unit test covers the race, this one the real server's error shape.
func TestConnect_RejectedCredentials_FatalWithWarmPool(t *testing.T) {
	client, err := Connect(context.Background(), testutil.MongoURI(t), "nobody", "wrong",
		WithPool(warmPool()), WithDegradedStart())
	requireAuthRejected(t, client, err)
}

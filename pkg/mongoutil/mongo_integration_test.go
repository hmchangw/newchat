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

// unreachableMongo is an address nothing listens on, with a server-selection
// bound short enough to keep these tests quick.
const unreachableMongo = "mongodb://127.0.0.1:1/?connectTimeoutMS=200"

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

// testutil's Mongo runs without --auth and has no users, so any credentials
// fail SCRAM with AuthenticationFailed (18): the server answered and said no.
// That is the one startup failure WithDegradedStart must not tolerate.
func TestConnect_RejectedCredentials_FatalEvenWithDegradedStart(t *testing.T) {
	client, err := Connect(context.Background(), testutil.MongoURI(t), "nobody", "wrong",
		WithPool(fastFailPool()), WithDegradedStart())
	requireAuthRejected(t, client, err)
}

// With MinPoolSize > 0 the pool's warm-up connections fail SCRAM before the
// ping's checkout, which then sees only the cleared pool; the rejection has to
// come from the pool event instead. The unit test covers the race against an
// in-process fake; this one proves the real server's error shape is matched.
func TestConnect_RejectedCredentials_FatalWithWarmPool(t *testing.T) {
	client, err := Connect(context.Background(), testutil.MongoURI(t), "nobody", "wrong",
		WithPool(warmPool()), WithDegradedStart())
	requireAuthRejected(t, client, err)
}

//go:build integration

package mongoutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

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

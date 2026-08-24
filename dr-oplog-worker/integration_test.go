//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoTargetStore_UpsertAndDelete(t *testing.T) {
	db := testutil.MongoDB(t, "droplog")
	store := NewMongoTargetStore(db)
	ctx := context.Background()
	const coll = "rooms"

	require.NoError(t, store.UpsertByID(ctx, coll, "r1", bson.D{{Key: "_id", Value: "r1"}, {Key: "name", Value: "general"}}))
	var got bson.M
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.Equal(t, "general", got["name"])

	// Idempotent re-upsert (redelivery) with new content — one doc, replaced.
	require.NoError(t, store.UpsertByID(ctx, coll, "r1", bson.D{{Key: "_id", Value: "r1"}, {Key: "name", Value: "renamed"}}))
	n, err := db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "r1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.Equal(t, "renamed", got["name"])

	require.NoError(t, store.DeleteByID(ctx, coll, "r1"))
	n, err = db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "r1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Delete of a missing row is a no-op, not an error.
	require.NoError(t, store.DeleteByID(ctx, coll, "ghost"))
}

func TestHandle_EndToEnd_ThroughStore(t *testing.T) {
	db := testutil.MongoDB(t, "droplog-e2e")
	h := &handler{
		collections: map[string]struct{}{"rooms": {}},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	ev := oplogEvent{Op: "insert", Collection: "rooms",
		DocumentKey:  []byte(`{"_id":"r9"}`),
		FullDocument: []byte(`{"_id":"r9","name":"neo"}`)}
	require.NoError(t, h.handle(ctx, ev))
	var got bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r9"}).Decode(&got))
	assert.Equal(t, "neo", got["name"])
}

// The DR feed uses updateLookup: update events carry the full post-image and upsert directly,
// with no source-Mongo re-read. Verify verbatim replacement lands in the backup.
func TestHandle_Update_EndToEnd_SelfContained(t *testing.T) {
	db := testutil.MongoDB(t, "droplog-update")
	h := &handler{
		collections: map[string]struct{}{"subscriptions": {}},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "insert", Collection: "subscriptions",
		DocumentKey: []byte(`{"_id":"s1"}`), FullDocument: []byte(`{"_id":"s1","unread":3}`)}))
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "update", Collection: "subscriptions",
		DocumentKey: []byte(`{"_id":"s1"}`), FullDocument: []byte(`{"_id":"s1","unread":0}`)}))
	var got bson.M
	require.NoError(t, db.Collection("subscriptions").FindOne(ctx, bson.M{"_id": "s1"}).Decode(&got))
	assert.EqualValues(t, 0, got["unread"])
}

// The room's CrossSite locality flag must survive the DR feed verbatim (spec §7 / design §4.1) —
// the backup's broadcast path depends on it.
func TestHandle_CrossSiteFlag_RoundTrips(t *testing.T) {
	db := testutil.MongoDB(t, "droplog-crosssite")
	h := &handler{
		collections: map[string]struct{}{"rooms": {}},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "insert", Collection: "rooms",
		DocumentKey: []byte(`{"_id":"r1"}`), FullDocument: []byte(`{"_id":"r1","name":"fed","crossSite":true}`)}))
	var got bson.M
	require.NoError(t, db.Collection("rooms").FindOne(ctx, bson.M{"_id": "r1"}).Decode(&got))
	assert.Equal(t, true, got["crossSite"])
}

func TestHandle_Delete_EndToEnd(t *testing.T) {
	db := testutil.MongoDB(t, "droplog-delete")
	h := &handler{
		collections: map[string]struct{}{"room_members": {}},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "insert", Collection: "room_members",
		DocumentKey: []byte(`{"_id":"m1"}`), FullDocument: []byte(`{"_id":"m1","role":"member"}`)}))
	n, err := db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "m1"})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	require.NoError(t, h.handle(ctx, oplogEvent{Op: "delete", Collection: "room_members", DocumentKey: []byte(`{"_id":"m1"}`)}))
	n, err = db.Collection("room_members").CountDocuments(ctx, bson.M{"_id": "m1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

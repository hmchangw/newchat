//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/migration"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestMongoTargetStore_UpsertAndDelete(t *testing.T) {
	db := testutil.MongoDB(t, "directxfer")
	store := NewMongoTargetStore(db)
	ctx := context.Background()
	const coll = "rocketchat_avatar"

	require.NoError(t, store.UpsertByID(ctx, coll, "a1", bson.D{{Key: "_id", Value: "a1"}, {Key: "blob", Value: "x"}}))
	var got bson.M
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "a1"}).Decode(&got))
	assert.Equal(t, "x", got["blob"])

	// Idempotent re-upsert (redelivery) with new content — one doc, replaced.
	require.NoError(t, store.UpsertByID(ctx, coll, "a1", bson.D{{Key: "_id", Value: "a1"}, {Key: "blob", Value: "y"}}))
	n, err := db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "a1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	require.NoError(t, db.Collection(coll).FindOne(ctx, bson.M{"_id": "a1"}).Decode(&got))
	assert.Equal(t, "y", got["blob"])

	require.NoError(t, store.DeleteByID(ctx, coll, "a1"))
	n, err = db.Collection(coll).CountDocuments(ctx, bson.M{"_id": "a1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// Delete of a missing row is a no-op, not an error.
	require.NoError(t, store.DeleteByID(ctx, coll, "ghost"))
}

func TestHandle_EndToEnd_ThroughStore(t *testing.T) {
	db := testutil.MongoDB(t, "directxfer-e2e")
	h := &handler{
		collections: map[string]struct{}{"rocketchat_avatar": {}},
		lookups:     map[string]migration.SourceLookup{},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	ev := oplogEvent{Op: "insert", Collection: "rocketchat_avatar",
		DocumentKey:  []byte(`{"_id":"u9"}`),
		FullDocument: []byte(`{"_id":"u9","username":"neo"}`)}
	require.NoError(t, h.handle(ctx, ev))
	var got bson.M
	require.NoError(t, db.Collection("rocketchat_avatar").FindOne(ctx, bson.M{"_id": "u9"}).Decode(&got))
	assert.Equal(t, "neo", got["username"])
}

func TestHandle_Update_EndToEnd(t *testing.T) {
	// update re-reads the current source doc (fakeLookup stands in for the source) and upserts it
	// verbatim into a real target collection — a second collection, per spec §8.
	db := testutil.MongoDB(t, "directxfer-update")
	h := &handler{
		collections: map[string]struct{}{"ufsTokens": {}},
		lookups:     map[string]migration.SourceLookup{"ufsTokens": &fakeLookup{doc: []byte(`{"_id":"t1","state":"fresh"}`)}},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "update", Collection: "ufsTokens", DocumentKey: []byte(`{"_id":"t1"}`)}))
	var got bson.M
	require.NoError(t, db.Collection("ufsTokens").FindOne(ctx, bson.M{"_id": "t1"}).Decode(&got))
	assert.Equal(t, "fresh", got["state"])
}

func TestHandle_Delete_EndToEnd(t *testing.T) {
	// insert then delete through the handler against a real third collection.
	db := testutil.MongoDB(t, "directxfer-delete")
	h := &handler{
		collections: map[string]struct{}{"user_devices": {}},
		lookups:     map[string]migration.SourceLookup{},
		target:      NewMongoTargetStore(db),
	}
	ctx := context.Background()
	require.NoError(t, h.handle(ctx, oplogEvent{Op: "insert", Collection: "user_devices",
		DocumentKey: []byte(`{"_id":"d1"}`), FullDocument: []byte(`{"_id":"d1","ua":"x"}`)}))
	n, err := db.Collection("user_devices").CountDocuments(ctx, bson.M{"_id": "d1"})
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	require.NoError(t, h.handle(ctx, oplogEvent{Op: "delete", Collection: "user_devices", DocumentKey: []byte(`{"_id":"d1"}`)}))
	n, err = db.Collection("user_devices").CountDocuments(ctx, bson.M{"_id": "d1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

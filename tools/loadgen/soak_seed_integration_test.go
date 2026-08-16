//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/pkg/testutil"
)

func TestSeedSoak_PreservesBorrowedAndUnrelatedMongoData(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_seed")
	usersCollection := db.Collection("users")
	roomsCollection := db.Collection("rooms")
	subscriptionsCollection := db.Collection("subscriptions")

	users := makeSoakUsers(10, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := usersCollection.InsertMany(ctx, userDocs)
	require.NoError(t, err)
	beforeUsers := readRawDocuments(t, usersCollection)

	_, err = roomsCollection.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "unrelated-room"},
		{Key: "siteId", Value: "site-a"},
	})
	require.NoError(t, err)
	_, err = subscriptionsCollection.InsertOne(ctx, bson.D{
		{Key: "_id", Value: "unrelated-sub"},
		{Key: "roomId", Value: "unrelated-room"},
	})
	require.NoError(t, err)
	const indexName = "siteId_1_soak_seed_test"
	_, err = roomsCollection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "siteId", Value: 1}},
		Options: options.Index().SetName(indexName),
	})
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(roomsCollection, time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 10
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.4
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3
	input := soakSeedInput{
		RunID:             cfg.RunID,
		SiteID:            "site-a",
		MongoDatabase:     db.Name(),
		CassandraKeyspace: "chat",
		Seed:              42,
		Config:            &cfg,
	}

	first, err := seedSoak(ctx, store, keyStore, &input, newProductionSoakIDs())
	require.NoError(t, err)
	firstRoomID := first.Rooms[0].ID
	for collection, document := range map[string]any{
		"thread_rooms": bson.D{
			{Key: "_id", Value: "old-thread"},
			{Key: "roomId", Value: firstRoomID},
		},
		"thread_subscriptions": bson.D{
			{Key: "_id", Value: "old-thread-sub"},
			{Key: "roomId", Value: firstRoomID},
			{Key: "threadRoomId", Value: "old-thread"},
		},
		"room_data_keys": bson.D{
			{Key: "_id", Value: firstRoomID},
			{Key: "wrappedDEK", Value: []byte("old-wrapped-dek")},
		},
	} {
		_, err := db.Collection(collection).InsertOne(ctx, document)
		require.NoError(t, err, collection)
	}
	second, err := seedSoak(ctx, store, keyStore, &input, newProductionSoakIDs())
	require.NoError(t, err)

	assert.Equal(t, beforeUsers, readRawDocuments(t, usersCollection))
	assert.Equal(t, int64(1), countDocuments(t, roomsCollection, bson.D{{Key: "_id", Value: "unrelated-room"}}))
	assert.Equal(t, int64(1), countDocuments(t, subscriptionsCollection, bson.D{{Key: "_id", Value: "unrelated-sub"}}))
	assert.Equal(t, int64(len(second.Rooms)), countDocuments(
		t,
		roomsCollection,
		bson.D{{Key: "soakRunId", Value: cfg.RunID}},
	))
	assert.Equal(t, int64(len(second.Subscriptions)), countDocuments(
		t,
		subscriptionsCollection,
		bson.D{{Key: "soakRunId", Value: cfg.RunID}},
	))
	assert.Equal(t, int64(len(second.Subscriptions)), countDocuments(
		t,
		subscriptionsCollection,
		bson.D{
			{Key: "soakRunId", Value: cfg.RunID},
			{Key: "open", Value: true},
		},
	))
	assert.Equal(t, int64(len(second.Rooms)), countDocuments(
		t,
		roomsCollection,
		bson.D{
			{Key: "soakRunId", Value: cfg.RunID},
			{Key: "encKey", Value: bson.D{{Key: "$exists", Value: true}}},
		},
	))
	assert.Equal(t, len(first.Rooms), len(second.Rooms))
	for _, collection := range []string{
		"thread_rooms",
		"thread_subscriptions",
		"room_data_keys",
	} {
		assert.Equal(t, int64(0), countDocuments(
			t,
			db.Collection(collection),
			bson.D{{Key: "roomId", Value: firstRoomID}},
		), collection)
	}
	assert.Equal(t, int64(0), countDocuments(
		t,
		db.Collection("room_data_keys"),
		bson.D{{Key: "_id", Value: firstRoomID}},
	))

	_, found := findIndex(ctx, t, roomsCollection, indexName)
	assert.True(t, found)

	var manifest soakManifest
	require.NoError(t, db.Collection(soakManifestCollection).
		FindOne(ctx, bson.D{{Key: "_id", Value: cfg.RunID}}).
		Decode(&manifest))
	assert.Equal(t, soakManifestSeeded, manifest.State)
}

func TestSeedSoak_RejectsExistingDMRoomBeforeWritingManifest(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_seed_dm_conflict")
	users := makeSoakUsers(10, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	cfg := validSoakConfig(t)
	cfg.MaxUsers = 10
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.4
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3
	expected, err := buildSoakTopology(
		users,
		&cfg,
		"site-a",
		42,
		newSequenceSoakIDs(),
	)
	require.NoError(t, err)
	var conflictingRoomID string
	for i := range expected.Rooms {
		if expected.Rooms[i].Type == model.RoomTypeDM {
			conflictingRoomID = expected.Rooms[i].ID
			break
		}
	}
	require.NotEmpty(t, conflictingRoomID)
	_, err = db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: conflictingRoomID},
		{Key: "siteId", Value: "site-a"},
	})
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	input := soakSeedInput{
		RunID:             cfg.RunID,
		SiteID:            "site-a",
		MongoDatabase:     db.Name(),
		CassandraKeyspace: "chat",
		Seed:              42,
		Config:            &cfg,
	}

	_, err = seedSoak(
		ctx,
		store,
		keyStore,
		&input,
		newSequenceSoakIDs(),
	)

	require.Error(t, err)
	assert.Equal(t, int64(1), countDocuments(
		t,
		db.Collection("rooms"),
		bson.D{{Key: "_id", Value: conflictingRoomID}},
	))
	assert.Equal(t, int64(0), countDocuments(
		t,
		db.Collection(soakManifestCollection),
		bson.D{},
	))
}

func readRawDocuments(t *testing.T, collection *mongo.Collection) []bson.Raw {
	t.Helper()
	cursor, err := collection.Find(
		context.Background(),
		bson.D{},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	require.NoError(t, err)
	var documents []bson.Raw
	require.NoError(t, cursor.All(context.Background(), &documents))
	return documents
}

func countDocuments(t *testing.T, collection *mongo.Collection, filter any) int64 {
	t.Helper()
	count, err := collection.CountDocuments(context.Background(), filter)
	require.NoError(t, err)
	return count
}

func TestSoakStore_ReadsRoomStateFromThePrimary(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_roomstate")
	store := &mongoSoakStore{db: db}

	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "room-1", "name": "soak-channel"})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.M{
		"_id": "sub-1", "roomId": "room-1", "u": bson.M{"account": "user-2"}, "muted": true,
	})
	require.NoError(t, err)
	_, err = db.Collection("room_members").InsertOne(ctx, bson.M{
		"_id": "rm-1", "rid": "room-1",
		"member": bson.M{"type": "individual", "id": "u2", "account": "user-2"},
	})
	require.NoError(t, err)

	name, found, err := store.RoomName(ctx, "room-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "soak-channel", name)

	member, err := store.IsRoomMember(ctx, "room-1", "user-2")
	require.NoError(t, err)
	assert.True(t, member)

	muted, known, err := store.SubscriptionMuted(ctx, "room-1", "user-2")
	require.NoError(t, err)
	assert.True(t, known)
	assert.True(t, muted)
}

func TestSoakStore_ReportsAbsentRoomStateWithoutError(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_roomstate_absent")
	store := &mongoSoakStore{db: db}

	_, found, err := store.RoomName(ctx, "missing-room")
	require.NoError(t, err)
	assert.False(t, found, "an absent room is an answer, never an error")

	member, err := store.IsRoomMember(ctx, "missing-room", "user-2")
	require.NoError(t, err)
	assert.False(t, member)

	_, known, err := store.SubscriptionMuted(ctx, "missing-room", "user-2")
	require.NoError(t, err)
	assert.False(t, known)
}

func TestSoakStore_RejectsRoomStateLookupsWithoutIdentifiers(t *testing.T) {
	ctx := context.Background()
	store := &mongoSoakStore{db: testutil.MongoDB(t, "loadgen_soak_roomstate_args")}

	_, _, err := store.RoomName(ctx, "")
	require.Error(t, err)

	_, err = store.IsRoomMember(ctx, "room-1", "")
	require.Error(t, err)

	_, _, err = store.SubscriptionMuted(ctx, "", "user-2")
	require.Error(t, err)
}

func TestSoakStore_AppendOwnedRoomsKeepsTeardownPaging(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_ownership_append")
	store := &mongoSoakStore{db: db}

	require.NoError(t, store.ReplaceOwnershipChunks(ctx, "run-1", [][]string{{"room-1", "room-2"}}))
	_, err := db.Collection("rooms").InsertOne(ctx, bson.M{"_id": "room-3", "name": "created"})
	require.NoError(t, err)
	require.NoError(t, store.AppendOwnedRooms(ctx, "run-1", []string{"room-3"}))

	after := ""
	var collected []string
	for {
		page, pageErr := store.NextOwnershipPage(ctx, "run-1", after, soakOwnershipChunkSize)
		require.NoError(t, pageErr)
		if page == nil {
			break
		}
		require.Greater(t, page.Cursor, after, "the teardown cursor must strictly advance")
		collected = append(collected, page.RoomIDs...)
		after = page.Cursor
	}
	assert.ElementsMatch(t, []string{"room-1", "room-2", "room-3"}, collected)

	var room struct {
		SoakRunID string `bson:"soakRunId"`
	}
	require.NoError(t, db.Collection("rooms").
		FindOne(ctx, bson.D{{Key: "_id", Value: "room-3"}}).Decode(&room))
	assert.Equal(t, "run-1", room.SoakRunID,
		"teardown only deletes rooms carrying the run marker")
}

func TestSoakStore_AppendOwnedRoomsIsANoOpWithoutRooms(t *testing.T) {
	ctx := context.Background()
	store := &mongoSoakStore{db: testutil.MongoDB(t, "loadgen_soak_ownership_noop")}

	require.NoError(t, store.AppendOwnedRooms(ctx, "run-1", nil))
	require.Error(t, store.AppendOwnedRooms(ctx, "", []string{"room-1"}))

	page, err := store.NextOwnershipPage(ctx, "run-1", "", soakOwnershipChunkSize)
	require.NoError(t, err)
	assert.Nil(t, page)
}

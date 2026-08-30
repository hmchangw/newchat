//go:build integration

package main

import (
	"context"
	"math/rand" // #nosec G404 -- load generator randomness, never used for secrets // nosemgrep: math-random-used
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

// The soak phase never rebuilds the topology in memory — it loads it back from
// MongoDB. Every unit test built a topology directly, so nothing covered the
// seed -> LoadTopology -> pool path, and a field the loader dropped took the
// whole run down before it sent a single request.
func TestLoadTopology_RestoresEnoughStateToBuildTheRoomPool(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_reload")

	users := makeSoakUsers(12, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.6
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3

	seeded, err := seedSoak(ctx, store, keyStore, &soakSeedInput{
		RunID: cfg.RunID, SiteID: "site-a", MongoDatabase: db.Name(),
		CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
	}, newProductionSoakIDs())
	require.NoError(t, err)
	require.NotEmpty(t, seeded.BorrowedUsers)

	reloaded, err := store.LoadTopology(ctx, cfg.RunID, "site-a")
	require.NoError(t, err)

	assert.NotEmpty(t, reloaded.BorrowedUsers,
		"the member lane draws every candidate from BorrowedUsers")
	assert.Len(t, reloaded.ActiveUsers, len(seeded.ActiveUsers))

	// The real assertion: the reloaded topology must actually build a pool.
	// Without it the workload exits before sending anything.
	pool, err := newSoakRoomStatePool(
		&reloaded, cfg.MemberQuarantineMax, NewMetrics(), rand.New(rand.NewSource(1)),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, pool.RoomIDs())
}

// Created rooms share the ownership ledger with the seeded topology. Their
// subscriptions are written by room-service and carry neither loadgen's
// soakRunId marker nor isSubscribed, so restart must recover them by owned room
// scope and use row existence as channel membership.
func TestLoadTopology_RestoresRoomsCreatedDuringTheRun(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_reload_created")

	users := makeSoakUsers(12, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.6
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3

	seeded, err := seedSoak(ctx, store, keyStore, &soakSeedInput{
		RunID: cfg.RunID, SiteID: "site-a", MongoDatabase: db.Name(),
		CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
	}, newProductionSoakIDs())
	require.NoError(t, err)

	const createdRoomID = "created-room-1"
	_, err = db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: createdRoomID},
		{Key: "name", Value: soakCreatedRoomPrefix(cfg.RunID) + "one"},
		{Key: "type", Value: model.RoomTypeChannel},
		{Key: "siteId", Value: "site-a"},
	})
	require.NoError(t, err)
	_, err = db.Collection("subscriptions").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "created-subscription-1"},
		{Key: "u", Value: bson.D{
			{Key: "_id", Value: seeded.ActiveUsers[0].ID},
			{Key: "account", Value: seeded.ActiveUsers[0].Account},
		}},
		{Key: "roomId", Value: createdRoomID},
		{Key: "siteId", Value: "site-a"},
		{Key: "roles", Value: bson.A{model.RoleOwner}},
		{Key: "roomType", Value: model.RoomTypeChannel},
	})
	require.NoError(t, err)
	require.NoError(t, store.AppendOwnedRooms(ctx, cfg.RunID, []string{createdRoomID}))

	reloaded, err := store.LoadTopology(ctx, cfg.RunID, "site-a")
	require.NoError(t, err)

	assert.Len(t, reloaded.Rooms, len(seeded.Rooms)+1)
	foundCreated := false
	for i := range reloaded.Rooms {
		foundCreated = foundCreated || reloaded.Rooms[i].ID == createdRoomID
	}
	assert.True(t, foundCreated)
	_, err = newSoakRuntimeSelector(&reloaded, &cfg, 42)
	require.NoError(t, err)
}

// The pool seeds its mute and read-cursor expectations from the reloaded
// subscriptions. A projection that drops those fields makes a restarted process
// expect the opposite mute state and report a correctness violation it caused.
func TestLoadTopology_RestoresMuteAndReadCursorState(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_reload_state")

	users := makeSoakUsers(12, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.6
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3

	_, err = seedSoak(ctx, store, keyStore, &soakSeedInput{
		RunID: cfg.RunID, SiteID: "site-a", MongoDatabase: db.Name(),
		CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
	}, newProductionSoakIDs())
	require.NoError(t, err)

	// Mimic a mark-read and a mute that landed before the pod was replaced.
	lastSeen := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	result, err := db.Collection("subscriptions").UpdateOne(
		ctx,
		bson.D{{Key: "soakRunId", Value: cfg.RunID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "muted", Value: true},
			{Key: "lastSeenAt", Value: lastSeen},
		}}},
	)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.ModifiedCount)

	reloaded, err := store.LoadTopology(ctx, cfg.RunID, "site-a")
	require.NoError(t, err)

	muted, seen := 0, 0
	for i := range reloaded.Subscriptions {
		if reloaded.Subscriptions[i].Muted {
			muted++
		}
		if reloaded.Subscriptions[i].LastSeenAt != nil {
			seen++
		}
	}
	assert.Equal(t, 1, muted, "a muted subscription must survive the reload")
	assert.Equal(t, 1, seen, "the read cursor is the next mark-read's baseline")
}

// The create budget is a per-run cap, so a restart must subtract what the run
// already created. Seeded rooms carry the same ownership marker and outnumber
// the budget many times over, so counting those would zero the allowance the
// moment any process restarted.
func TestCountCreatedRooms_ExcludesTheSeededPopulation(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_created_count")

	users := makeSoakUsers(12, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.6
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3

	_, err = seedSoak(ctx, store, keyStore, &soakSeedInput{
		RunID: cfg.RunID, SiteID: "site-a", MongoDatabase: db.Name(),
		CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
	}, newProductionSoakIDs())
	require.NoError(t, err)

	created, err := store.CountCreatedRooms(ctx, cfg.RunID)
	require.NoError(t, err)
	assert.Equal(t, 0, created, "seeded rooms are not create-lane rooms")

	_, err = db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "room-created-1"},
		{Key: "soakRunId", Value: cfg.RunID},
		{Key: "name", Value: soakCreatedRoomPrefix(cfg.RunID) + "abc"},
		{Key: "siteId", Value: "site-a"},
	})
	require.NoError(t, err)

	created, err = store.CountCreatedRooms(ctx, cfg.RunID)
	require.NoError(t, err)
	assert.Equal(t, 1, created)

	roomID, found, err := store.RoomIDByName(ctx, "site-a", soakCreatedRoomPrefix(cfg.RunID)+"abc")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "room-created-1", roomID)

	// room-service creates the room; the ownership marker is written afterwards
	// by AppendOwnedRooms. A room stranded between the two still spent budget,
	// so a count that required the marker would let a crash loop re-spend the
	// whole allowance every restart.
	_, err = db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: "room-created-2"},
		{Key: "name", Value: soakCreatedRoomPrefix(cfg.RunID) + "def"},
		{Key: "siteId", Value: "site-a"},
	})
	require.NoError(t, err)

	created, err = store.CountCreatedRooms(ctx, cfg.RunID)
	require.NoError(t, err)
	assert.Equal(t, 2, created, "a created room that is not yet claimed still spent budget")
}

// insertRoomServiceCreatedRoom writes a room and its subscriptions the way
// room-service does when the create lane calls room.create: no soakRunId, since
// the marker is a loadgen-private field the service knows nothing about. The
// run claims the room afterwards, which is the only stamping that happens.
func insertRoomServiceCreatedRoom(
	t *testing.T,
	ctx context.Context,
	store *mongoSoakStore,
	runID, siteID, roomID string,
	accounts []model.SubscriptionUser,
) {
	t.Helper()
	_, err := store.db.Collection("rooms").InsertOne(ctx, bson.D{
		{Key: "_id", Value: roomID},
		{Key: "name", Value: soakCreatedRoomPrefix(runID) + "abc"},
		{Key: "type", Value: model.RoomTypeChannel},
		{Key: "siteId", Value: siteID},
		{Key: "userCount", Value: len(accounts)},
	})
	require.NoError(t, err)

	documents := make([]any, 0, len(accounts))
	for i, account := range accounts {
		roles := []model.Role{model.RoleMember}
		if i == 0 {
			roles = []model.Role{model.RoleOwner}
		}
		documents = append(documents, bson.D{
			{Key: "_id", Value: roomID + "-sub-" + account.Account},
			{Key: "u", Value: bson.D{
				{Key: "_id", Value: account.ID},
				{Key: "account", Value: account.Account},
			}},
			{Key: "roomId", Value: roomID},
			{Key: "siteId", Value: siteID},
			{Key: "roles", Value: roles},
			{Key: "roomType", Value: model.RoomTypeChannel},
			{Key: "isSubscribed", Value: true},
			{Key: "joinedAt", Value: time.Now().UTC()},
		})
	}
	_, err = store.db.Collection("subscriptions").InsertMany(ctx, documents)
	require.NoError(t, err)

	require.NoError(t, store.AppendOwnedRooms(ctx, runID, []string{roomID}))
}

// A room the create lane made is claimed by stamping soakRunId on the room, but
// its subscriptions are written by room-service and never carry that marker.
// Loading subscriptions by the marker therefore returns the room with no
// members, and the runtime selector refuses to start:
//
//	soak room %q has no subscribed member
//
// One created room is enough to make every later restart of that run fail, so
// at the default create rate a soak brands its own restart unusable about
// twenty seconds in.
func TestLoadTopology_RestoresSubscriptionsRoomServiceWroteForCreatedRooms(t *testing.T) {
	ctx := context.Background()
	db := testutil.MongoDB(t, "loadgen_soak_created_reload")

	users := makeSoakUsers(12, "site-a")
	userDocs := make([]any, len(users))
	for i := range users {
		userDocs[i] = users[i]
	}
	_, err := db.Collection("users").InsertMany(ctx, userDocs)
	require.NoError(t, err)

	store := &mongoSoakStore{db: db}
	keyStore := roomkeystore.NewMongoStore(db.Collection("rooms"), time.Hour)
	t.Cleanup(func() { require.NoError(t, keyStore.Close()) })
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 12
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.6
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3

	seeded, err := seedSoak(ctx, store, keyStore, &soakSeedInput{
		RunID: cfg.RunID, SiteID: "site-a", MongoDatabase: db.Name(),
		CassandraKeyspace: "chat", Seed: 42, Config: &cfg,
	}, newProductionSoakIDs())
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(seeded.ActiveUsers), 2)

	const createdRoomID = "created-room-1"
	members := []model.SubscriptionUser{
		{ID: seeded.ActiveUsers[0].ID, Account: seeded.ActiveUsers[0].Account},
		{ID: seeded.ActiveUsers[1].ID, Account: seeded.ActiveUsers[1].Account},
	}
	insertRoomServiceCreatedRoom(t, ctx, store, cfg.RunID, "site-a", createdRoomID, members)

	reloaded, err := store.LoadTopology(ctx, cfg.RunID, "site-a")
	require.NoError(t, err)

	var createdRoomSubscriptions int
	for i := range reloaded.Subscriptions {
		if reloaded.Subscriptions[i].RoomID == createdRoomID {
			createdRoomSubscriptions++
		}
	}
	assert.Equal(t, len(members), createdRoomSubscriptions,
		"the created room is loaded, so its subscriptions must load with it")

	// The assertion that matches what an operator sees: a restarted process
	// cannot build its selector at all.
	_, err = newSoakRuntimeSelector(&reloaded, &cfg, 7)
	require.NoError(t, err,
		"a run that created a room must still be able to restart")
}

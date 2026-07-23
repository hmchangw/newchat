package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
)

func TestSoakUserQuery_UsesExactFilterAndProjection(t *testing.T) {
	filter := soakUserFilter("site-a")
	projection := soakUserProjection()
	filterMap := bsonDMap(filter)

	assert.Equal(t, "site-a", lookupBSONValue(t, filter, "siteId"))
	assert.Contains(t, filterMap, "deactivated")
	assert.Contains(t, filterMap, "account")
	assert.Contains(t, filterMap, "roles")
	assert.Equal(t, bson.M{
		"_id":         1,
		"account":     1,
		"siteId":      1,
		"deactivated": 1,
		"roles":       1,
		"engName":     1,
		"chineseName": 1,
	}, bsonDMap(projection))
}

func TestChunkSoakRoomIDs_BoundsOwnershipDocuments(t *testing.T) {
	roomIDs := make([]string, 4501)
	for i := range roomIDs {
		roomIDs[i] = "room"
	}

	chunks := chunkSoakRoomIDs(roomIDs, 2000)

	require.Len(t, chunks, 3)
	assert.Len(t, chunks[0], 2000)
	assert.Len(t, chunks[1], 2000)
	assert.Len(t, chunks[2], 501)
}

func TestSeedSoak_WritesOwnedTopologyAndManifestWithoutMutatingUsers(t *testing.T) {
	users := makeSoakUsers(10, "site-a")
	original := cloneSoakUsers(users)
	store := &recordingSoakSeedStore{users: users}
	keys := &recordingRoomKeyStore{}
	input := testSoakSeedInput(t)

	topology, err := seedSoak(context.Background(), store, keys, &input, newSequenceSoakIDs())
	require.NoError(t, err)

	assert.Equal(t, original, users)
	assert.Equal(t, []soakManifestState{soakManifestSeeding, soakManifestSeeded}, store.manifestStates)
	assert.Equal(t, 1, store.resetCalls)
	assert.Len(t, store.rooms, input.Config.RoomCount)
	assert.Len(t, store.subscriptions, len(topology.Subscriptions))
	assert.Len(t, keys.setRoomIDs, input.Config.RoomCount)
	require.NotEmpty(t, store.ownershipChunks)
	assert.Equal(t, input.RunID, store.lastManifest.ID)
	assert.Equal(t, input.SiteID, store.lastManifest.SiteID)
	assert.Equal(t, input.MongoDatabase, store.lastManifest.MongoDatabase)
	assert.Equal(t, input.CassandraKeyspace, store.lastManifest.CassandraKeyspace)
	assert.Equal(t, len(topology.BorrowedUsers), store.lastManifest.BorrowedUserCount)
	assert.Equal(t, len(topology.ActiveUsers), store.lastManifest.ActiveUserCount)
	assert.Equal(t, len(topology.Rooms), store.lastManifest.RoomCount)
	assert.Equal(t, len(topology.Subscriptions), store.lastManifest.SubscriptionCount)
	assert.NotEmpty(t, store.lastManifest.ConfigDigest)
}

func TestSeedSoak_RetryCleansOnlyRunOwnedPartialData(t *testing.T) {
	store := &recordingSoakSeedStore{
		users:             makeSoakUsers(10, "site-a"),
		failSubscriptions: errors.New("injected partial seed failure"),
	}
	input := testSoakSeedInput(t)

	_, err := seedSoak(context.Background(), store, &recordingRoomKeyStore{}, &input, newSequenceSoakIDs())
	require.Error(t, err)
	assert.Equal(t, soakManifestSeeding, store.lastManifest.State)
	assert.Equal(t, 1, store.resetCalls)

	store.failSubscriptions = nil
	_, err = seedSoak(context.Background(), store, &recordingRoomKeyStore{}, &input, newSequenceSoakIDs())
	require.NoError(t, err)
	assert.Equal(t, 2, store.resetCalls)
	assert.Equal(t, []string{input.RunID, input.RunID}, store.resetRunIDs)
	assert.Equal(t, soakManifestSeeded, store.lastManifest.State)
}

func TestSeedSoak_StopsBeforeWritesWhenBorrowUsersFails(t *testing.T) {
	store := &recordingSoakSeedStore{borrowErr: errors.New("mongo unavailable")}
	input := testSoakSeedInput(t)

	_, err := seedSoak(
		context.Background(),
		store,
		&recordingRoomKeyStore{},
		&input,
		newSequenceSoakIDs(),
	)

	require.Error(t, err)
	assert.Zero(t, store.resetCalls)
	assert.Empty(t, store.manifestStates)
}

type recordingSoakSeedStore struct {
	users             []model.User
	borrowErr         error
	failSubscriptions error
	resetCalls        int
	resetRunIDs       []string
	rooms             []model.Room
	subscriptions     []model.Subscription
	ownershipChunks   [][]string
	manifestStates    []soakManifestState
	lastManifest      soakManifest
}

func (s *recordingSoakSeedStore) BorrowUsers(
	_ context.Context,
	_ string,
	_ int,
) ([]model.User, error) {
	return s.users, s.borrowErr
}

func (s *recordingSoakSeedStore) ResetOwned(_ context.Context, runID string) error {
	s.resetCalls++
	s.resetRunIDs = append(s.resetRunIDs, runID)
	s.rooms = nil
	s.subscriptions = nil
	s.ownershipChunks = nil
	return nil
}

func (s *recordingSoakSeedStore) PutManifest(_ context.Context, manifest *soakManifest) error {
	s.lastManifest = *manifest
	s.manifestStates = append(s.manifestStates, manifest.State)
	return nil
}

func (s *recordingSoakSeedStore) InsertOwnedRooms(
	_ context.Context,
	_ string,
	rooms []model.Room,
) error {
	s.rooms = append(s.rooms, rooms...)
	return nil
}

func (s *recordingSoakSeedStore) InsertOwnedSubscriptions(
	_ context.Context,
	_ string,
	subscriptions []model.Subscription,
) error {
	if s.failSubscriptions != nil {
		return s.failSubscriptions
	}
	s.subscriptions = append(s.subscriptions, subscriptions...)
	return nil
}

func (s *recordingSoakSeedStore) ReplaceOwnershipChunks(
	_ context.Context,
	_ string,
	chunks [][]string,
) error {
	s.ownershipChunks = append(s.ownershipChunks, chunks...)
	return nil
}

type recordingRoomKeyStore struct {
	setRoomIDs []string
}

func (s *recordingRoomKeyStore) Set(
	_ context.Context,
	roomID string,
	_ roomkeystore.RoomKeyPair,
) (int, error) {
	s.setRoomIDs = append(s.setRoomIDs, roomID)
	return 1, nil
}

func (s *recordingRoomKeyStore) Delete(_ context.Context, _ string) error {
	return nil
}

func testSoakSeedInput(t *testing.T) soakSeedInput {
	t.Helper()
	cfg := validSoakConfig(t)
	cfg.MaxUsers = 10
	cfg.ActiveUsers = 6
	cfg.RoomCount = 5
	cfg.ChannelRatio = 0.4
	cfg.ChannelMembers = 3
	cfg.ReactionsPerHotMessage = 3
	return soakSeedInput{
		RunID:             cfg.RunID,
		SiteID:            "site-a",
		MongoDatabase:     "chat",
		CassandraKeyspace: "chat",
		Seed:              42,
		Config:            &cfg,
	}
}

func lookupBSONValue(t *testing.T, doc bson.D, key string) any {
	t.Helper()
	for _, element := range doc {
		if element.Key == key {
			return element.Value
		}
	}
	t.Fatalf("key %q not found", key)
	return nil
}

func bsonDMap(doc bson.D) bson.M {
	result := make(bson.M, len(doc))
	for _, element := range doc {
		result[element.Key] = element.Value
	}
	return result
}

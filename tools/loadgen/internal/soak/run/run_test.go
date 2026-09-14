package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomkeystore"
	"github.com/hmchangw/chat/tools/loadgen/internal/soak/topology"
)

func TestSoakRunManifest_BSONContractAndLegacyRead(t *testing.T) {
	now := time.Date(2026, 8, 29, 1, 2, 3, 0, time.UTC)
	manifest := Manifest{
		ID: "run-a", State: StateRunning, RunMode: "continuous",
		SiteID: "site-a", MongoDatabase: "chat", CassandraKeyspace: "chat",
		ConfigDigest: "digest", BorrowedUserCount: 2, ActiveUserCount: 2,
		ActiveUserIDs: []string{"u-1", "u-2"}, RoomCount: 4,
		SubscriptionCount: 5, StartedAt: now, UpdatedAt: now,
		SeededAt: &now, CleanedAt: &now, FirstStartedAt: &now, Deadline: &now,
		CompletedAt: &now, LastStoppedAt: &now, LastHeartbeatAt: &now,
		ConfiguredDuration: 24 * time.Hour, RestartCount: 2,
	}
	encoded, err := bson.Marshal(manifest)
	require.NoError(t, err)
	var document bson.M
	require.NoError(t, bson.Unmarshal(encoded, &document))
	assert.Equal(t, []string{
		"_id", "activeUserCount", "activeUserIds", "borrowedUserCount",
		"cassandraKeyspace", "cleanedAt", "completedAt", "configDigest",
		"configuredDuration", "deadline", "firstStartedAt", "lastHeartbeatAt",
		"lastStoppedAt", "mongoDatabase", "restartCount", "roomCount", "runMode",
		"seededAt", "siteId", "startedAt", "state", "subscriptionCount", "updatedAt",
	}, sortedKeys(document))
	var roundTripped Manifest
	require.NoError(t, bson.Unmarshal(encoded, &roundTripped))
	assert.Equal(t, manifest, roundTripped)

	legacy, err := bson.Marshal(bson.D{
		{Key: "_id", Value: "legacy"}, {Key: "state", Value: string(StateSeeded)},
		{Key: "startedAt", Value: now}, {Key: "updatedAt", Value: now},
	})
	require.NoError(t, err)
	var decoded Manifest
	require.NoError(t, bson.Unmarshal(legacy, &decoded))
	assert.Nil(t, decoded.Deadline)
	assert.Nil(t, decoded.LastHeartbeatAt)
	reencoded, err := bson.Marshal(decoded)
	require.NoError(t, err)
	document = nil
	require.NoError(t, bson.Unmarshal(reencoded, &document))
	assert.NotContains(t, document, "deadline")
	assert.NotContains(t, document, "lastHeartbeatAt")
}

func TestSoakRunSeed_PreservesOrderedActiveUsersAndFencesActiveRun(t *testing.T) {
	input := validSeedInput()
	store := &recordingStore{users: makeUsers(10, "site-a")}
	keys := &recordingKeys{}

	built, err := Seed(context.Background(), store, keys, &input, sequenceIDs())
	require.NoError(t, err)
	require.Len(t, store.manifests, 2)
	assert.Equal(t, []ManifestState{StateSeeding, StateSeeded}, []ManifestState{
		store.manifests[0].State, store.manifests[1].State,
	})
	assert.Equal(t, userIDs(built.ActiveUsers), store.manifests[1].ActiveUserIDs)
	assert.Equal(t, input.ConfigDigest, store.manifests[1].ConfigDigest)
	assert.Len(t, keys.roomIDs, input.Topology.RoomCount)

	now := time.Now().UTC()
	store = &recordingStore{manifest: &Manifest{
		ID: input.RunID, State: StateRunning, LastHeartbeatAt: &now,
	}}
	_, err = Seed(context.Background(), store, keys, &input, sequenceIDs())
	require.Error(t, err)
	assert.Zero(t, store.borrowCalls)
	assert.Empty(t, store.manifests)
}

func TestSoakRunSeed_StopsAtFailedPersistenceStage(t *testing.T) {
	_, err := Seed(context.Background(), &recordingStore{}, &recordingKeys{}, nil, sequenceIDs())
	require.Error(t, err)
	input := validSeedInput()
	input.Topology.RunID = "other"
	_, err = Seed(context.Background(), &recordingStore{}, &recordingKeys{}, &input, sequenceIDs())
	require.Error(t, err)

	tests := []struct {
		name   string
		mutate func(*recordingStore)
		want   string
	}{
		{"find manifest", func(s *recordingStore) { s.findErr = errors.New("find") }, "check existing"},
		{"borrow users", func(s *recordingStore) { s.borrowErr = errors.New("borrow") }, "borrow real users"},
		{"find conflicts", func(s *recordingStore) { s.conflictErr = errors.New("conflict query") }, "check soak room ID conflicts"},
		{"reset owned", func(s *recordingStore) { s.resetErr = errors.New("reset") }, "reset partial topology"},
		{"write manifest", func(s *recordingStore) { s.putErr = errors.New("manifest") }, "record seeding manifest"},
		{"write ownership", func(s *recordingStore) { s.ownershipErr = errors.New("ownership") }, "record soak ownership"},
		{"insert rooms", func(s *recordingStore) { s.roomsErr = errors.New("rooms") }, "insert soak rooms"},
		{"insert subscriptions", func(s *recordingStore) { s.subscriptionsErr = errors.New("subscriptions") }, "insert soak subscriptions"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &recordingStore{users: makeUsers(10, "site-a")}
			tt.mutate(store)
			input := validSeedInput()
			_, err := Seed(context.Background(), store, &recordingKeys{}, &input, sequenceIDs())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}

	input = validSeedInput()
	input.Topology.ActiveUsers = 0
	_, err = Seed(context.Background(), &recordingStore{
		users: makeUsers(10, "site-a"),
	}, &recordingKeys{}, &input, sequenceIDs())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build soak topology")

	input = validSeedInput()
	_, err = Seed(context.Background(), &recordingStore{
		users: makeUsers(10, "site-a"), conflictIDs: []string{"room"},
	}, &recordingKeys{}, &input, sequenceIDs())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conflicts with non-owned data")

	input = validSeedInput()
	_, err = Seed(context.Background(), &recordingStore{
		users: makeUsers(10, "site-a"),
	}, &recordingKeys{err: errors.New("keys")}, &input, sequenceIDs())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seed soak room keys")
}

func TestSoakRunTeardown_PagesBatchesAndRejectsLiveLease(t *testing.T) {
	cfg := TeardownConfig{
		RunID: "run-a", CassandraCleanup: "none", HeartbeatStaleAfter: time.Minute,
		BatchRooms: 2, BatchTimeout: time.Second,
	}
	store := &recordingStore{
		manifest: &Manifest{ID: "run-a", State: StateSeeded},
		pages:    [][]string{{"room-1", "room-2", "room-3"}, {"room-4"}},
	}
	found, err := Teardown(context.Background(), store, nil, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, [][]string{{"room-1", "room-2"}, {"room-3"}, {"room-4"}}, store.deleted)
	assert.True(t, store.ownershipDeleted)
	assert.True(t, store.cleaned)

	now := time.Now().UTC()
	store = &recordingStore{manifest: &Manifest{
		ID: "run-a", State: StateRunning, LastHeartbeatAt: &now,
	}}
	_, err = Teardown(context.Background(), store, nil, &cfg, "chat")
	require.Error(t, err)
	assert.Empty(t, store.deleted)
	assert.False(t, store.cleaned)
}

func TestSoakRunTeardown_RejectsNonAdvancingCursor(t *testing.T) {
	store := &recordingStore{
		manifest: &Manifest{ID: "run-a", State: StateSeeded},
		pages:    [][]string{{"room-1"}, {"room-2"}},
		cursors:  []string{"chunk-1", "chunk-1"},
	}
	_, err := Teardown(context.Background(), store, nil, &TeardownConfig{
		RunID: "run-a", CassandraCleanup: "none", BatchRooms: 1,
		BatchTimeout: time.Second,
	}, "chat")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not advance")
	assert.Equal(t, [][]string{{"room-1"}}, store.deleted)
	assert.False(t, store.cleaned)
}

func TestSoakRunTeardown_ValidatesDestructiveInputsAndFailures(t *testing.T) {
	_, err := Teardown(context.Background(), &recordingStore{}, nil, nil, "chat")
	require.Error(t, err)

	base := TeardownConfig{
		RunID: "run-a", CassandraCleanup: "none", HeartbeatStaleAfter: time.Minute,
		BatchRooms: 1, BatchTimeout: time.Second,
	}
	tests := []struct {
		name   string
		mutate func(*recordingStore, *TeardownConfig)
		want   string
	}{
		{"unknown mode", func(_ *recordingStore, c *TeardownConfig) { c.CassandraCleanup = "drop" }, "unknown Cassandra cleanup"},
		{"find manifest", func(s *recordingStore, _ *TeardownConfig) { s.findErr = errors.New("find") }, "find soak manifest"},
		{"page", func(s *recordingStore, _ *TeardownConfig) { s.pageErr = errors.New("page") }, "page ownership"},
		{"empty page", func(s *recordingStore, _ *TeardownConfig) { s.pages = [][]string{{}} }, "invalid empty ownership page"},
		{"delete batch", func(s *recordingStore, _ *TeardownConfig) { s.deleteErr = errors.New("delete") }, "delete owned Mongo data"},
		{"delete ownership", func(s *recordingStore, _ *TeardownConfig) { s.deleteOwnershipErr = errors.New("ownership") }, "delete ownership records"},
		{"mark cleaned", func(s *recordingStore, _ *TeardownConfig) { s.markErr = errors.New("mark") }, "mark soak run"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			store := &recordingStore{
				manifest: &Manifest{ID: "run-a", State: StateSeeded},
				pages:    [][]string{{"room-1"}},
			}
			tt.mutate(store, &cfg)
			_, err := Teardown(context.Background(), store, nil, &cfg, "chat")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}

	now := time.Now().UTC()
	_, err = Teardown(context.Background(), &recordingStore{manifest: &Manifest{
		ID: "run-a", State: StateRunning, LastHeartbeatAt: &now,
	}}, nil, &base, "chat")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "active soak run")

	truncate := base
	truncate.CassandraCleanup = "truncate"
	_, err = Teardown(context.Background(), &recordingStore{}, nil, &truncate, "chat")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refuse Cassandra truncate")
}

func TestSoakRunTeardown_TruncatesExactTablesAndHandlesNoOpStates(t *testing.T) {
	cfg := TeardownConfig{
		RunID: "run-a", CassandraCleanup: "truncate", ConfirmKeyspace: "chat",
		HeartbeatStaleAfter: time.Minute, BatchRooms: 1, BatchTimeout: time.Second,
	}
	cleaner := &recordingCleaner{keyspace: "chat"}
	store := &recordingStore{manifest: &Manifest{ID: "run-a", State: StateSeeded}}
	found, err := Teardown(context.Background(), store, cleaner, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, cassandraTables, cleaner.tables)
	assert.True(t, store.cleaned)

	found, err = Teardown(context.Background(), &recordingStore{}, cleaner, &cfg, "chat")
	require.NoError(t, err)
	assert.False(t, found)

	store = &recordingStore{manifest: &Manifest{ID: "run-a", State: StateCleaned}}
	found, err = Teardown(context.Background(), store, cleaner, &cfg, "chat")
	require.NoError(t, err)
	assert.True(t, found)
	assert.False(t, store.cleaned)

	cleaner = &recordingCleaner{keyspace: "chat", err: errors.New("truncate")}
	store = &recordingStore{manifest: &Manifest{ID: "run-a", State: StateSeeded}}
	_, err = Teardown(context.Background(), store, cleaner, &cfg, "chat")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "truncate Cassandra table")
}

func TestSoakRunMongoContracts_AreRunScopedAndDurable(t *testing.T) {
	filter := userFilter("site-a")
	projection := userProjection()
	assert.Equal(t, "site-a", bsonValue(t, filter, "siteId"))
	assert.Equal(t, bson.D{{Key: "$ne", Value: false}}, bsonValue(t, filter, "active"))
	assert.Equal(t, bson.M{
		"_id": 1, "account": 1, "siteId": 1, "active": 1, "roles": 1,
		"engName": 1, "chineseName": 1,
	}, documentMap(projection))

	assert.Equal(t, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gte", Value: "run-a:"}, {Key: "$lt", Value: "run-a;"},
	}}}, ownershipIDFilter("run-a", ""))
	assert.Equal(t, bson.D{{Key: "_id", Value: bson.D{
		{Key: "$gt", Value: "run-a:000001"}, {Key: "$lt", Value: "run-a;"},
	}}}, ownershipIDFilter("run-a", "run-a:000001"))

	concern := manifestWriteConcern()
	assert.Equal(t, writeconcern.WCMajority, concern.W)
	require.NotNil(t, concern.Journal)
	assert.True(t, *concern.Journal)

	at := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	assert.Equal(t, bson.D{{Key: "$max", Value: bson.D{
		{Key: "lastHeartbeatAt", Value: at}, {Key: "updatedAt", Value: at},
	}}}, heartbeatUpdate(at))

	manifestFields := documentMap(manifestProjection())
	for _, field := range []string{
		"_id", "state", "siteId", "mongoDatabase", "cassandraKeyspace",
		"configDigest", "borrowedUserCount", "activeUserCount", "activeUserIds",
		"roomCount", "subscriptionCount", "startedAt", "updatedAt", "seededAt",
		"cleanedAt", "firstStartedAt", "deadline", "completedAt",
		"configuredDuration", "restartCount", "runMode", "lastStoppedAt",
		"lastHeartbeatAt",
	} {
		assert.Equal(t, 1, manifestFields[field], field)
	}
	assert.Len(t, manifestFields, 23)
}

func TestSoakRunHelpers_HandleEmptyAndExpiredInputs(t *testing.T) {
	now := time.Now().UTC()
	assert.False(t, ActiveManifest(nil, time.Minute, now))
	assert.False(t, ActiveManifest(&Manifest{State: StateSeeded}, time.Minute, now))
	assert.True(t, ActiveManifest(&Manifest{State: StateRunning}, time.Minute, now))
	expired := now.Add(-2 * time.Minute)
	assert.False(t, ActiveManifest(&Manifest{
		State: StateRunning, LastHeartbeatAt: &expired,
	}, time.Minute, now))
	assert.Nil(t, ChunkRoomIDs(nil, 2))
	assert.Nil(t, ChunkRoomIDs([]string{"room"}, 0))
	chunks := ChunkRoomIDs(make([]string, 4501), 2000)
	require.Len(t, chunks, 3)
	assert.Len(t, chunks[0], 2000)
	assert.Len(t, chunks[1], 2000)
	assert.Len(t, chunks[2], 501)
	keys, err := BuildRoomKeys(nil)
	require.NoError(t, err)
	assert.Empty(t, keys)
	first, err := BuildRoomKeys([]model.Room{{ID: "room-1"}})
	require.NoError(t, err)
	second, err := BuildRoomKeys([]model.Room{{ID: "room-1"}})
	require.NoError(t, err)
	require.Len(t, first["room-1"].PrivateKey, 32)
	require.Len(t, second["room-1"].PrivateKey, 32)
	assert.False(t, bytes.Equal(first["room-1"].PrivateKey, second["room-1"].PrivateKey))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.ErrorIs(t, wait(ctx, time.Hour), context.Canceled)
	require.NoError(t, wait(context.Background(), 0))
}

func TestSoakRunMongo_RejectsInvalidArgumentsBeforeDatabaseAccess(t *testing.T) {
	store := NewMongo(nil)
	ctx := context.Background()
	_, err := store.BorrowUsers(ctx, "site-a", 0)
	require.Error(t, err)
	_, err = store.CountCreatedRooms(ctx, "")
	require.Error(t, err)
	_, _, err = store.RoomIDByName(ctx, "", "")
	require.Error(t, err)
	_, _, err = store.RoomName(ctx, "")
	require.Error(t, err)
	_, err = store.IsRoomMember(ctx, "", "")
	require.Error(t, err)
	_, _, err = store.SubscriptionMuted(ctx, "", "")
	require.Error(t, err)
	_, _, err = store.SubscriptionLastSeen(ctx, "", "")
	require.Error(t, err)
	require.Error(t, store.AppendOwnedRooms(ctx, "", []string{"room"}))
	require.NoError(t, store.AppendOwnedRooms(ctx, "run-a", nil))
}

type recordingStore struct {
	users              []model.User
	manifest           *Manifest
	manifests          []Manifest
	borrowCalls        int
	pages              [][]string
	cursors            []string
	nextPage           int
	deleted            [][]string
	ownershipDeleted   bool
	cleaned            bool
	findErr            error
	borrowErr          error
	conflictIDs        []string
	conflictErr        error
	resetErr           error
	putErr             error
	ownershipErr       error
	roomsErr           error
	subscriptionsErr   error
	pageErr            error
	deleteErr          error
	deleteOwnershipErr error
	markErr            error
}

func (s *recordingStore) FindManifest(context.Context, string) (*Manifest, error) {
	return s.manifest, s.findErr
}

func (s *recordingStore) BorrowUsers(context.Context, string, int) ([]model.User, error) {
	s.borrowCalls++
	return s.users, s.borrowErr
}

func (s *recordingStore) FindConflictingRoomIDs(context.Context, string, []string) ([]string, error) {
	return s.conflictIDs, s.conflictErr
}

func (s *recordingStore) ResetOwned(context.Context, string) error { return s.resetErr }

func (s *recordingStore) PutManifest(_ context.Context, manifest *Manifest) error {
	if s.putErr != nil {
		return s.putErr
	}
	s.manifests = append(s.manifests, *manifest)
	return nil
}

func (s *recordingStore) InsertOwnedRooms(context.Context, string, []model.Room) error {
	return s.roomsErr
}

func (s *recordingStore) InsertOwnedSubscriptions(context.Context, string, []model.Subscription) error {
	return s.subscriptionsErr
}

func (s *recordingStore) ReplaceOwnershipChunks(context.Context, string, [][]string) error {
	return s.ownershipErr
}

func (s *recordingStore) NextOwnershipPage(
	_ context.Context, _ string, _ string, _ int,
) (*OwnershipPage, error) {
	if s.pageErr != nil {
		return nil, s.pageErr
	}
	if s.nextPage >= len(s.pages) {
		return nil, nil
	}
	roomIDs := s.pages[s.nextPage]
	cursor := fmt.Sprintf("chunk-%06d", s.nextPage+1)
	if len(s.cursors) > s.nextPage {
		cursor = s.cursors[s.nextPage]
	}
	s.nextPage++
	return &OwnershipPage{Cursor: cursor, RoomIDs: roomIDs}, nil
}

func (s *recordingStore) DeleteOwnedRoomBatch(
	_ context.Context, _ string, roomIDs []string,
) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deleted = append(s.deleted, append([]string(nil), roomIDs...))
	return nil
}

func (s *recordingStore) DeleteOwnership(context.Context, string) error {
	if s.deleteOwnershipErr != nil {
		return s.deleteOwnershipErr
	}
	s.ownershipDeleted = true
	return nil
}

func (s *recordingStore) MarkCleaned(context.Context, string) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.cleaned = true
	return nil
}

type recordingKeys struct {
	roomIDs []string
	err     error
}

func (s *recordingKeys) Set(
	_ context.Context, roomID string, _ roomkeystore.RoomKeyPair,
) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.roomIDs = append(s.roomIDs, roomID)
	return 1, nil
}

type recordingCleaner struct {
	keyspace string
	tables   []string
	err      error
}

func (c *recordingCleaner) Keyspace() string { return c.keyspace }

func (c *recordingCleaner) Truncate(_ context.Context, table string) error {
	if c.err != nil {
		return c.err
	}
	c.tables = append(c.tables, table)
	return nil
}

func validSeedInput() SeedInput {
	return SeedInput{
		RunID: "run-a", RunMode: "duration", SiteID: "site-a",
		MongoDatabase: "chat", CassandraKeyspace: "chat", ConfigDigest: "digest",
		HeartbeatStaleAfter: time.Minute, Seed: 42,
		Topology: topology.BuildConfig{
			RunID: "run-a", MaxUsers: 10, ActiveUsers: 6, RoomCount: 5,
			ChannelRatio: 0.4, ChannelMembers: 3,
		},
	}
}

func makeUsers(count int, siteID string) []model.User {
	users := make([]model.User, count)
	for i := range users {
		users[i] = model.User{
			ID: fmt.Sprintf("u-%05d", i), Account: fmt.Sprintf("user-%05d", i),
			SiteID: siteID, Roles: []model.UserRole{model.UserRoleUser},
		}
	}
	return users
}

func sequenceIDs() *topology.IdentitySource {
	var room, subscription int
	return &topology.IdentitySource{
		NewChannelRoomID: func() string {
			room++
			return fmt.Sprintf("channel-%03d", room)
		},
		NewSubscriptionID: func() string {
			subscription++
			return fmt.Sprintf("subscription-%05d", subscription)
		},
	}
}

func userIDs(users []model.User) []string {
	ids := make([]string, len(users))
	for i := range users {
		ids[i] = users[i].ID
	}
	return ids
}

func sortedKeys(document bson.M) []string {
	keys := make([]string, 0, len(document))
	for key := range document {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func bsonValue(t *testing.T, document bson.D, key string) any {
	t.Helper()
	for _, element := range document {
		if element.Key == key {
			return element.Value
		}
	}
	t.Fatalf("key %q not found", key)
	return nil
}

func documentMap(document bson.D) bson.M {
	result := make(bson.M, len(document))
	for _, element := range document {
		result[element.Key] = element.Value
	}
	return result
}

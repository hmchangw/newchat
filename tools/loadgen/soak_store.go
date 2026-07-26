package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
)

const (
	soakManifestCollection  = "loadgen_soak_runs"
	soakOwnershipCollection = "loadgen_soak_ownership"
	soakInsertBatchSize     = 1000
	soakOwnershipChunkSize  = 2000
)

//go:generate mockgen -destination=mock_soak_store_test.go -package=main . soakSeedStore,soakLifecycleStore

type soakSeedStore interface {
	FindManifest(ctx context.Context, runID string) (*soakManifest, error)
	BorrowUsers(ctx context.Context, siteID string, limit int) ([]model.User, error)
	FindConflictingRoomIDs(ctx context.Context, runID string, roomIDs []string) ([]string, error)
	ResetOwned(ctx context.Context, runID string) error
	PutManifest(ctx context.Context, manifest *soakManifest) error
	InsertOwnedRooms(ctx context.Context, runID string, rooms []model.Room) error
	InsertOwnedSubscriptions(ctx context.Context, runID string, subscriptions []model.Subscription) error
	ReplaceOwnershipChunks(ctx context.Context, runID string, chunks [][]string) error
}

type mongoSoakStore struct {
	db *mongo.Database
}

var (
	_ soakSeedStore      = (*mongoSoakStore)(nil)
	_ soakTeardownStore  = (*mongoSoakStore)(nil)
	_ soakLifecycleStore = (*mongoSoakStore)(nil)
)

func soakUserFilter(siteID string) bson.D {
	return bson.D{
		{Key: "siteId", Value: siteID},
		{Key: "active", Value: bson.D{{Key: "$ne", Value: false}}},
		{Key: "_id", Value: bson.D{
			{Key: "$type", Value: "string"},
			{Key: "$ne", Value: ""},
		}},
		{Key: "account", Value: bson.D{
			{Key: "$type", Value: "string"},
			{Key: "$regex", Value: `^(?!p_)(?!.*\.bot$)[^.*>\s]+$`},
		}},
		{Key: "roles", Value: bson.D{
			{Key: "$nin", Value: bson.A{model.UserRoleBot, model.UserRoleAdmin}},
		}},
	}
}

func soakUserProjection() bson.D {
	return bson.D{
		{Key: "_id", Value: 1},
		{Key: "account", Value: 1},
		{Key: "siteId", Value: 1},
		{Key: "active", Value: 1},
		{Key: "roles", Value: 1},
		{Key: "engName", Value: 1},
		{Key: "chineseName", Value: 1},
	}
}

func (s *mongoSoakStore) BorrowUsers(
	ctx context.Context,
	siteID string,
	limit int,
) ([]model.User, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("borrowed soak user limit must be greater than zero")
	}
	opts := options.Find().
		SetProjection(soakUserProjection()).
		SetLimit(int64(limit))
	cursor, err := s.db.Collection("users").Find(ctx, soakUserFilter(siteID), opts)
	if err != nil {
		return nil, fmt.Errorf("find borrowed soak users: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var users []model.User
	if err := cursor.All(ctx, &users); err != nil {
		return nil, fmt.Errorf("decode borrowed soak users: %w", err)
	}
	return users, nil
}

func (s *mongoSoakStore) ResetOwned(ctx context.Context, runID string) error {
	after := ""
	for {
		page, err := s.NextOwnershipPage(
			ctx,
			runID,
			after,
			soakOwnershipChunkSize,
		)
		if err != nil {
			return fmt.Errorf("page prior soak ownership: %w", err)
		}
		if page == nil {
			break
		}
		if page.Cursor == "" || page.Cursor <= after {
			return fmt.Errorf("prior soak ownership cursor did not advance")
		}
		if err := s.DeleteOwnedRoomBatch(ctx, runID, page.RoomIDs); err != nil {
			return fmt.Errorf("delete prior soak room artifacts: %w", err)
		}
		after = page.Cursor
	}
	if err := s.DeleteOwnership(ctx, runID); err != nil {
		return fmt.Errorf("delete prior soak ownership: %w", err)
	}
	return nil
}

func (s *mongoSoakStore) FindConflictingRoomIDs(
	ctx context.Context,
	runID string,
	roomIDs []string,
) ([]string, error) {
	var conflicts []string
	for _, batch := range chunkSoakRoomIDs(roomIDs, soakInsertBatchSize) {
		cursor, err := s.db.Collection("rooms").Find(
			ctx,
			bson.D{
				{Key: "_id", Value: bson.D{{Key: "$in", Value: batch}}},
				{Key: "soakRunId", Value: bson.D{{Key: "$ne", Value: runID}}},
			},
			options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
		)
		if err != nil {
			return nil, fmt.Errorf("find conflicting soak room IDs: %w", err)
		}
		var found []struct {
			ID string `bson:"_id"`
		}
		if err := cursor.All(ctx, &found); err != nil {
			_ = cursor.Close(ctx)
			return nil, fmt.Errorf("decode conflicting soak room IDs: %w", err)
		}
		if err := cursor.Close(ctx); err != nil {
			return nil, fmt.Errorf("close conflicting soak room cursor: %w", err)
		}
		for i := range found {
			conflicts = append(conflicts, found[i].ID)
		}
	}
	return conflicts, nil
}

func (s *mongoSoakStore) PutManifest(ctx context.Context, manifest *soakManifest) error {
	_, err := s.db.Collection(soakManifestCollection).ReplaceOne(
		ctx,
		bson.D{{Key: "_id", Value: manifest.ID}},
		manifest,
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("put soak manifest %q: %w", manifest.ID, err)
	}
	return nil
}

func (s *mongoSoakStore) GetManifest(
	ctx context.Context,
	runID string,
) (*soakManifest, error) {
	var manifest soakManifest
	err := s.db.Collection(soakManifestCollection).FindOne(
		ctx,
		bson.D{{Key: "_id", Value: runID}},
		options.FindOne().SetProjection(soakManifestProjection()),
	).Decode(&manifest)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, errSoakManifestNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get soak manifest %q: %w", runID, err)
	}
	return &manifest, nil
}

func (s *mongoSoakStore) TouchHeartbeat(
	ctx context.Context,
	runID string,
	at time.Time,
) error {
	heartbeat := at.UTC()
	result, err := s.db.Collection(soakManifestCollection).UpdateOne(
		ctx,
		bson.D{
			{Key: "_id", Value: runID},
			{Key: "state", Value: soakManifestRunning},
		},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "lastHeartbeatAt", Value: heartbeat},
			{Key: "updatedAt", Value: heartbeat},
		}}},
	)
	if err != nil {
		return fmt.Errorf("touch soak manifest %q heartbeat: %w", runID, err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("touch soak manifest %q heartbeat: run is not active", runID)
	}
	return nil
}

func (s *mongoSoakStore) LoadTopology(
	ctx context.Context,
	runID string,
	siteID string,
) (soakTopology, error) {
	manifest, err := s.FindManifest(ctx, runID)
	if err != nil {
		return soakTopology{}, fmt.Errorf("load soak topology manifest: %w", err)
	}
	if manifest == nil {
		return soakTopology{}, fmt.Errorf(
			"load soak topology for run %q: manifest not found",
			runID,
		)
	}

	var rooms []model.Room
	var subscriptions []model.Subscription
	after := ""
	for {
		page, pageErr := s.NextOwnershipPage(
			ctx,
			runID,
			after,
			soakOwnershipChunkSize,
		)
		if pageErr != nil {
			return soakTopology{}, fmt.Errorf("page soak topology ownership: %w", pageErr)
		}
		if page == nil {
			break
		}
		if page.Cursor == "" || page.Cursor <= after {
			return soakTopology{}, fmt.Errorf("soak topology ownership cursor did not advance")
		}
		pageRooms, pageSubscriptions, loadErr := s.loadOwnedTopologyPage(
			ctx,
			runID,
			siteID,
			page.RoomIDs,
		)
		if loadErr != nil {
			return soakTopology{}, loadErr
		}
		rooms = append(rooms, pageRooms...)
		subscriptions = append(subscriptions, pageSubscriptions...)
		after = page.Cursor
	}
	if len(rooms) == 0 {
		return soakTopology{}, fmt.Errorf(
			"load soak topology for run %q: no owned rooms",
			runID,
		)
	}
	if len(subscriptions) == 0 {
		return soakTopology{}, fmt.Errorf(
			"load soak topology for run %q: no owned subscriptions",
			runID,
		)
	}

	usersByID := make(map[string]model.User)
	for i := range subscriptions {
		user := subscriptions[i].User
		if user.ID == "" || user.Account == "" {
			continue
		}
		usersByID[user.ID] = model.User{
			ID: user.ID, Account: user.Account, SiteID: siteID,
		}
	}
	activeUsers := make([]model.User, 0, len(manifest.ActiveUserIDs))
	for _, userID := range manifest.ActiveUserIDs {
		user, ok := usersByID[userID]
		if !ok {
			return soakTopology{}, fmt.Errorf(
				"load soak topology for run %q: active user %q has no subscription",
				runID,
				userID,
			)
		}
		activeUsers = append(activeUsers, user)
	}
	if len(activeUsers) != manifest.ActiveUserCount {
		return soakTopology{}, fmt.Errorf(
			"load soak topology for run %q: active user count mismatch",
			runID,
		)
	}
	return soakTopology{
		ActiveUsers: activeUsers, Rooms: rooms, Subscriptions: subscriptions,
	}, nil
}

func (s *mongoSoakStore) loadOwnedTopologyPage(
	ctx context.Context,
	runID string,
	siteID string,
	roomIDs []string,
) ([]model.Room, []model.Subscription, error) {
	roomCursor, err := s.db.Collection("rooms").Find(
		ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: roomIDs}}},
			{Key: "soakRunId", Value: runID},
			{Key: "siteId", Value: siteID},
		},
		options.Find().
			SetProjection(bson.D{
				{Key: "_id", Value: 1},
				{Key: "name", Value: 1},
				{Key: "type", Value: 1},
				{Key: "siteId", Value: 1},
				{Key: "userCount", Value: 1},
				{Key: "uids", Value: 1},
				{Key: "accounts", Value: 1},
				{Key: "createdAt", Value: 1},
				{Key: "updatedAt", Value: 1},
			}).
			SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("find owned soak rooms by ID: %w", err)
	}
	var rooms []model.Room
	if err := roomCursor.All(ctx, &rooms); err != nil {
		_ = roomCursor.Close(ctx)
		return nil, nil, fmt.Errorf("decode owned soak rooms: %w", err)
	}
	if err := roomCursor.Close(ctx); err != nil {
		return nil, nil, fmt.Errorf("close owned soak room cursor: %w", err)
	}

	subscriptionCursor, err := s.db.Collection("subscriptions").Find(
		ctx,
		bson.D{
			{Key: "roomId", Value: bson.D{{Key: "$in", Value: roomIDs}}},
			{Key: "soakRunId", Value: runID},
			{Key: "siteId", Value: siteID},
		},
		options.Find().
			SetProjection(bson.D{
				{Key: "_id", Value: 1},
				{Key: "u", Value: 1},
				{Key: "roomId", Value: 1},
				{Key: "siteId", Value: 1},
				{Key: "roles", Value: 1},
				{Key: "name", Value: 1},
				{Key: "roomType", Value: 1},
				{Key: "isSubscribed", Value: 1},
				{Key: "joinedAt", Value: 1},
			}).
			SetSort(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("find owned soak subscriptions by room ID: %w", err)
	}
	var subscriptions []model.Subscription
	if err := subscriptionCursor.All(ctx, &subscriptions); err != nil {
		_ = subscriptionCursor.Close(ctx)
		return nil, nil, fmt.Errorf("decode owned soak subscriptions: %w", err)
	}
	if err := subscriptionCursor.Close(ctx); err != nil {
		return nil, nil, fmt.Errorf("close owned soak subscription cursor: %w", err)
	}
	return rooms, subscriptions, nil
}

func (s *mongoSoakStore) HasWrappedDEK(
	ctx context.Context,
	roomID string,
) (bool, error) {
	var record struct {
		WrappedDEK []byte `bson:"wrappedDEK"`
	}
	err := s.db.Collection(atrest.CollectionName).FindOne(
		ctx,
		bson.D{
			{Key: "_id", Value: roomID},
			{Key: "wrappedDEK", Value: bson.D{{Key: "$exists", Value: true}}},
		},
		options.FindOne().SetProjection(bson.D{
			{Key: "_id", Value: 0},
			{Key: "wrappedDEK", Value: 1},
		}),
	).Decode(&record)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("find wrapped DEK for room %q: %w", roomID, err)
	}
	return len(record.WrappedDEK) > 0, nil
}

func soakManifestProjection() bson.D {
	return bson.D{
		{Key: "_id", Value: 1},
		{Key: "state", Value: 1},
		{Key: "runMode", Value: 1},
		{Key: "siteId", Value: 1},
		{Key: "mongoDatabase", Value: 1},
		{Key: "cassandraKeyspace", Value: 1},
		{Key: "configDigest", Value: 1},
		{Key: "borrowedUserCount", Value: 1},
		{Key: "activeUserCount", Value: 1},
		{Key: "activeUserIds", Value: 1},
		{Key: "roomCount", Value: 1},
		{Key: "subscriptionCount", Value: 1},
		{Key: "startedAt", Value: 1},
		{Key: "updatedAt", Value: 1},
		{Key: "seededAt", Value: 1},
		{Key: "cleanedAt", Value: 1},
		{Key: "firstStartedAt", Value: 1},
		{Key: "deadline", Value: 1},
		{Key: "completedAt", Value: 1},
		{Key: "lastStoppedAt", Value: 1},
		{Key: "lastHeartbeatAt", Value: 1},
		{Key: "configuredDuration", Value: 1},
		{Key: "restartCount", Value: 1},
	}
}

func (s *mongoSoakStore) InsertOwnedRooms(
	ctx context.Context,
	runID string,
	rooms []model.Room,
) error {
	docs := make([]any, len(rooms))
	for i := range rooms {
		docs[i] = ownedSoakRoom{Room: rooms[i], SoakRunID: runID}
	}
	if err := insertSoakBatches(ctx, s.db.Collection("rooms"), docs); err != nil {
		return fmt.Errorf("insert owned soak rooms: %w", err)
	}
	return nil
}

func (s *mongoSoakStore) InsertOwnedSubscriptions(
	ctx context.Context,
	runID string,
	subscriptions []model.Subscription,
) error {
	docs := make([]any, len(subscriptions))
	for i := range subscriptions {
		docs[i] = ownedSoakSubscription{
			Subscription: subscriptions[i],
			SoakRunID:    runID,
		}
	}
	if err := insertSoakBatches(ctx, s.db.Collection("subscriptions"), docs); err != nil {
		return fmt.Errorf("insert owned soak subscriptions: %w", err)
	}
	return nil
}

func (s *mongoSoakStore) ReplaceOwnershipChunks(
	ctx context.Context,
	runID string,
	chunks [][]string,
) error {
	collection := s.db.Collection(soakOwnershipCollection)
	if _, err := collection.DeleteMany(ctx, soakOwnershipIDFilter(runID, "")); err != nil {
		return fmt.Errorf("delete prior ownership chunks for run %q: %w", runID, err)
	}
	docs := make([]any, len(chunks))
	for i := range chunks {
		docs[i] = soakOwnershipChunk{
			ID:        fmt.Sprintf("%s:%06d", runID, i),
			SoakRunID: runID,
			RoomIDs:   append([]string(nil), chunks[i]...),
		}
	}
	if err := insertSoakBatches(ctx, collection, docs); err != nil {
		return fmt.Errorf("insert ownership chunks for run %q: %w", runID, err)
	}
	return nil
}

func insertSoakBatches(ctx context.Context, collection *mongo.Collection, docs []any) error {
	for start := 0; start < len(docs); start += soakInsertBatchSize {
		end := min(start+soakInsertBatchSize, len(docs))
		if _, err := collection.InsertMany(ctx, docs[start:end]); err != nil {
			return fmt.Errorf("insert batch into %s: %w", collection.Name(), err)
		}
	}
	return nil
}

func (s *mongoSoakStore) FindManifest(
	ctx context.Context,
	runID string,
) (*soakManifest, error) {
	var manifest soakManifest
	err := s.db.Collection(soakManifestCollection).
		FindOne(
			ctx,
			bson.D{{Key: "_id", Value: runID}},
			options.FindOne().SetProjection(soakManifestProjection()),
		).
		Decode(&manifest)
	if err == nil {
		return &manifest, nil
	}
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	return nil, fmt.Errorf("find soak manifest %q: %w", runID, err)
}

func (s *mongoSoakStore) NextOwnershipPage(
	ctx context.Context,
	runID string,
	after string,
	limit int,
) (*soakOwnershipPage, error) {
	var chunk soakOwnershipChunk
	err := s.db.Collection(soakOwnershipCollection).FindOne(
		ctx,
		soakOwnershipIDFilter(runID, after),
		options.FindOne().
			SetProjection(bson.D{
				{Key: "_id", Value: 1},
				{Key: "roomIds", Value: 1},
			}).
			SetSort(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&chunk)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find ownership chunk after %q: %w", after, err)
	}
	if limit <= 0 || len(chunk.RoomIDs) > limit {
		return nil, fmt.Errorf(
			"ownership chunk %q contains %d room IDs, limit=%d",
			chunk.ID,
			len(chunk.RoomIDs),
			limit,
		)
	}
	return &soakOwnershipPage{
		Cursor:  chunk.ID,
		RoomIDs: chunk.RoomIDs,
	}, nil
}

func soakOwnershipIDFilter(runID string, after string) bson.D {
	lowerOperator := "$gte"
	lower := runID + ":"
	if after != "" {
		lowerOperator = "$gt"
		lower = after
	}
	return bson.D{{
		Key: "_id",
		Value: bson.D{
			{Key: lowerOperator, Value: lower},
			{Key: "$lt", Value: runID + ";"},
		},
	}}
}

func (s *mongoSoakStore) DeleteOwnedRoomBatch(
	ctx context.Context,
	runID string,
	roomIDs []string,
) error {
	cursor, err := s.db.Collection("rooms").Find(
		ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: roomIDs}}},
			{Key: "soakRunId", Value: runID},
		},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return fmt.Errorf("resolve rooms owned by soak run %q: %w", runID, err)
	}
	var ownedRooms []struct {
		ID string `bson:"_id"`
	}
	if err := cursor.All(ctx, &ownedRooms); err != nil {
		_ = cursor.Close(ctx)
		return fmt.Errorf("decode rooms owned by soak run %q: %w", runID, err)
	}
	if err := cursor.Close(ctx); err != nil {
		return fmt.Errorf("close owned soak room cursor: %w", err)
	}
	ownedRoomIDs := make([]string, len(ownedRooms))
	for i := range ownedRooms {
		ownedRoomIDs[i] = ownedRooms[i].ID
	}
	if len(ownedRoomIDs) == 0 {
		return nil
	}

	roomFilter := bson.D{{
		Key:   "roomId",
		Value: bson.D{{Key: "$in", Value: ownedRoomIDs}},
	}}
	if err := s.deleteOwnedThreads(ctx, runID, roomFilter); err != nil {
		return err
	}
	if _, err := s.db.Collection(atrest.CollectionName).DeleteMany(
		ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ownedRoomIDs}}}},
	); err != nil {
		return fmt.Errorf("delete wrapped DEKs for soak run %q: %w", runID, err)
	}
	ownedFilter := bson.D{
		{Key: "soakRunId", Value: runID},
		{Key: "roomId", Value: bson.D{{Key: "$in", Value: ownedRoomIDs}}},
	}
	if _, err := s.db.Collection("subscriptions").DeleteMany(ctx, ownedFilter); err != nil {
		return fmt.Errorf("delete subscriptions for soak run %q: %w", runID, err)
	}
	roomOwnedFilter := bson.D{
		{Key: "soakRunId", Value: runID},
		{Key: "_id", Value: bson.D{{Key: "$in", Value: ownedRoomIDs}}},
	}
	if _, err := s.db.Collection("rooms").DeleteMany(ctx, roomOwnedFilter); err != nil {
		return fmt.Errorf("delete rooms for soak run %q: %w", runID, err)
	}
	return nil
}

func (s *mongoSoakStore) deleteOwnedThreads(
	ctx context.Context,
	runID string,
	roomFilter bson.D,
) error {
	cursor, err := s.db.Collection("thread_rooms").Find(
		ctx,
		roomFilter,
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return fmt.Errorf("find thread rooms for soak run %q: %w", runID, err)
	}

	threadRoomIDs := make([]string, 0, soakInsertBatchSize)
	deleteThreadSubscriptions := func() error {
		if len(threadRoomIDs) == 0 {
			return nil
		}
		_, deleteErr := s.db.Collection("thread_subscriptions").DeleteMany(
			ctx,
			bson.D{{
				Key:   "threadRoomId",
				Value: bson.D{{Key: "$in", Value: threadRoomIDs}},
			}},
		)
		if deleteErr != nil {
			return fmt.Errorf(
				"delete thread subscriptions for soak run %q: %w",
				runID,
				deleteErr,
			)
		}
		threadRoomIDs = threadRoomIDs[:0]
		return nil
	}

	for cursor.Next(ctx) {
		var threadRoom struct {
			ID string `bson:"_id"`
		}
		if err := cursor.Decode(&threadRoom); err != nil {
			_ = cursor.Close(ctx)
			return fmt.Errorf("decode thread room for soak run %q: %w", runID, err)
		}
		threadRoomIDs = append(threadRoomIDs, threadRoom.ID)
		if len(threadRoomIDs) == soakInsertBatchSize {
			if err := deleteThreadSubscriptions(); err != nil {
				_ = cursor.Close(ctx)
				return err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		_ = cursor.Close(ctx)
		return fmt.Errorf("iterate thread rooms for soak run %q: %w", runID, err)
	}
	if err := deleteThreadSubscriptions(); err != nil {
		_ = cursor.Close(ctx)
		return err
	}
	if err := cursor.Close(ctx); err != nil {
		return fmt.Errorf("close thread room cursor for soak run %q: %w", runID, err)
	}
	if _, err := s.db.Collection("thread_rooms").DeleteMany(ctx, roomFilter); err != nil {
		return fmt.Errorf("delete thread rooms for soak run %q: %w", runID, err)
	}
	return nil
}

func (s *mongoSoakStore) DeleteOwnership(ctx context.Context, runID string) error {
	if _, err := s.db.Collection(soakOwnershipCollection).DeleteMany(
		ctx,
		soakOwnershipIDFilter(runID, ""),
	); err != nil {
		return fmt.Errorf("delete ownership for soak run %q: %w", runID, err)
	}
	return nil
}

func (s *mongoSoakStore) MarkCleaned(ctx context.Context, runID string) error {
	now := time.Now().UTC()
	result, err := s.db.Collection(soakManifestCollection).UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: runID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "state", Value: soakManifestCleaned},
			{Key: "updatedAt", Value: now},
			{Key: "cleanedAt", Value: now},
		}}},
	)
	if err != nil {
		return fmt.Errorf("mark soak manifest %q cleaned: %w", runID, err)
	}
	if result.MatchedCount != 1 {
		return fmt.Errorf("mark soak manifest %q cleaned: manifest not found", runID)
	}
	return nil
}

type ownedSoakRoom struct {
	model.Room `bson:",inline"`
	SoakRunID  string `bson:"soakRunId"`
}

type ownedSoakSubscription struct {
	model.Subscription `bson:",inline"`
	SoakRunID          string `bson:"soakRunId"`
}

type soakOwnershipChunk struct {
	ID        string   `bson:"_id"`
	SoakRunID string   `bson:"soakRunId"`
	RoomIDs   []string `bson:"roomIds"`
}

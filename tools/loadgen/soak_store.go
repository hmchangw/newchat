package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const (
	soakManifestCollection  = "loadgen_soak_runs"
	soakOwnershipCollection = "loadgen_soak_ownership"
	soakInsertBatchSize     = 1000
	soakOwnershipChunkSize  = 2000
)

//go:generate mockgen -destination=mock_soak_store_test.go -package=main . soakSeedStore,soakLifecycleStore,soakSendLifecycle

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
	// The member lane draws every candidate from BorrowedUsers, so a topology
	// without it builds a pool with no candidates and takes the run down before
	// it sends anything. Rebuilding from the subscribed users is enough and
	// needs no extra persistence: a candidate only has to be a real account in
	// this run's topology, and the pool already excludes each room's own
	// members. Order is the subscription order, which is sorted by _id, so a
	// replacement process sees the same candidate ring.
	borrowedUsers := make([]model.User, 0, len(usersByID))
	seen := make(map[string]struct{}, len(usersByID))
	for i := range subscriptions {
		user := subscriptions[i].User
		if user.ID == "" {
			continue
		}
		if _, duplicate := seen[user.ID]; duplicate {
			continue
		}
		// Index through the map result: a subscription user with an ID but no
		// account never enters usersByID, and appending the zero value would put
		// an empty account in the candidate ring — every member_add against it
		// is rejected and lands in the ledger as a real failure.
		candidate, known := usersByID[user.ID]
		if !known {
			continue
		}
		seen[user.ID] = struct{}{}
		borrowedUsers = append(borrowedUsers, candidate)
	}
	if len(borrowedUsers) == 0 {
		return soakTopology{}, fmt.Errorf(
			"load soak topology for run %q: no borrowed users",
			runID,
		)
	}

	return soakTopology{
		BorrowedUsers: borrowedUsers, ActiveUsers: activeUsers,
		Rooms: rooms, Subscriptions: subscriptions,
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
	ownedRoomIDs := make([]string, len(rooms))
	for i := range rooms {
		ownedRoomIDs[i] = rooms[i].ID
	}
	if len(ownedRoomIDs) == 0 {
		return rooms, nil, nil
	}

	subscriptionCursor, err := s.db.Collection("subscriptions").Find(
		ctx,
		bson.D{
			// room-service writes create/member-lane subscriptions without
			// loadgen's private soakRunId marker. The rooms query above has
			// already narrowed this scope to rooms owned by the run.
			{Key: "roomId", Value: bson.D{{Key: "$in", Value: ownedRoomIDs}}},
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
				// The room pool seeds its next mute toggle and its read-receipt
				// baseline from these. Dropping them makes a restarted process
				// expect the opposite mute state and report a correctness
				// violation it caused itself.
				{Key: "muted", Value: 1},
				{Key: "lastSeenAt", Value: 1},
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
	if _, err := s.db.Collection("subscriptions").DeleteMany(ctx, roomFilter); err != nil {
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

// soakRoomStateStore is declared by its consumers in soak_roommember.go.
var _ soakRoomStateStore = (*mongoSoakStore)(nil)

// primary routes a read at the replica-set primary. The shared soak client
// connects with SecondaryPreferred, and a lagging secondary would report a
// completed write as missing — turning replication lag into a false data-loss
// claim during exactly the failures this run is measuring.
func (s *mongoSoakStore) primary(name string) *mongo.Collection {
	return mongoutil.CollectionWithReadPreference(s.db.Collection(name), readpref.Primary())
}

// soakCreatedRoomPrefix is the name every room the create lane makes starts
// with. It is what separates them from the far larger seeded population, which
// shares the same ownership records.
func soakCreatedRoomPrefix(runID string) string {
	return fmt.Sprintf("soak-%s-created-", runID)
}

// CountCreatedRooms totals the rooms the create lane has already made. The
// budget is a per-run cap, so a replacement process must start from what the
// run already spent rather than from a full allowance.
//
// It counts by name prefix rather than by ownership, because the seeded rooms
// carry the same ownership marker and outnumber the budget many times over —
// counting those would zero the allowance the moment any process restarted.
func (s *mongoSoakStore) CountCreatedRooms(ctx context.Context, runID string) (int, error) {
	if runID == "" {
		return 0, fmt.Errorf("count created soak rooms requires a run ID")
	}
	// The name prefix already embeds the run ID, so it alone scopes the count.
	// Requiring soakRunId as well would exclude exactly the rooms that matter:
	// room-service creates the room, and the marker is only written afterwards
	// by AppendOwnedRooms. A room stranded between the two has spent budget but
	// would be invisible here, letting a crash loop re-spend the whole
	// allowance on every restart.
	total, err := s.primary("rooms").CountDocuments(ctx, bson.D{
		{Key: "name", Value: bson.D{
			{Key: "$regex", Value: "^" + regexp.QuoteMeta(soakCreatedRoomPrefix(runID))},
		}},
	})
	if err != nil {
		return 0, fmt.Errorf("count created soak rooms for run %q: %w", runID, err)
	}
	return int(total), nil
}

// RoomIDByName resolves a room the create lane made from the name it chose
// before sending. The room ID is server-generated, so the name is the only
// identifier the run can journal ahead of the request — and therefore the only
// way a replacement process can find, verify and take ownership of a room whose
// reply was lost.
func (s *mongoSoakStore) RoomIDByName(
	ctx context.Context,
	siteID, name string,
) (string, bool, error) {
	if siteID == "" || name == "" {
		return "", false, fmt.Errorf("resolve soak room by name requires a site and name")
	}
	var document struct {
		ID string `bson:"_id"`
	}
	err := s.primary("rooms").FindOne(
		ctx,
		bson.D{
			{Key: "name", Value: name},
			{Key: "siteId", Value: siteID},
		},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("resolve soak room named %q: %w", name, err)
	}
	return document.ID, true, nil
}

func (s *mongoSoakStore) RoomName(ctx context.Context, roomID string) (string, bool, error) {
	if roomID == "" {
		return "", false, fmt.Errorf("read soak room name requires a room ID")
	}
	var document struct {
		Name string `bson:"name"`
	}
	err := s.primary("rooms").FindOne(
		ctx,
		bson.D{{Key: "_id", Value: roomID}},
		options.FindOne().SetProjection(bson.D{
			{Key: "_id", Value: 0},
			{Key: "name", Value: 1},
		}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read soak room name for %q: %w", roomID, err)
	}
	return document.Name, true, nil
}

func (s *mongoSoakStore) IsRoomMember(
	ctx context.Context,
	roomID, account string,
) (bool, error) {
	if roomID == "" || account == "" {
		return false, fmt.Errorf("read soak room membership requires a room ID and account")
	}
	err := s.primary("room_members").FindOne(
		ctx,
		bson.D{
			{Key: "rid", Value: roomID},
			{Key: "member.account", Value: account},
		},
		options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read soak room membership for %q: %w", roomID, err)
	}
	return true, nil
}

func (s *mongoSoakStore) SubscriptionMuted(
	ctx context.Context,
	roomID, account string,
) (bool, bool, error) {
	if roomID == "" || account == "" {
		return false, false, fmt.Errorf("read soak mute state requires a room ID and account")
	}
	var document struct {
		Muted bool `bson:"muted"`
	}
	err := s.primary("subscriptions").FindOne(
		ctx,
		bson.D{
			{Key: "roomId", Value: roomID},
			{Key: "u.account", Value: account},
		},
		options.FindOne().SetProjection(bson.D{
			{Key: "_id", Value: 0},
			{Key: "muted", Value: 1},
		}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("read soak mute state for %q: %w", roomID, err)
	}
	return document.Muted, true, nil
}

// SubscriptionLastSeen reads the authoritative read cursor. mark-read only ever
// moves it forward, so comparing two server-written values proves the write
// landed without trusting loadgen's clock.
func (s *mongoSoakStore) SubscriptionLastSeen(
	ctx context.Context,
	roomID, account string,
) (time.Time, bool, error) {
	if roomID == "" || account == "" {
		return time.Time{}, false, fmt.Errorf("read soak read cursor requires a room ID and account")
	}
	var document struct {
		LastSeenAt *time.Time `bson:"lastSeenAt"`
	}
	err := s.primary("subscriptions").FindOne(
		ctx,
		bson.D{
			{Key: "roomId", Value: roomID},
			{Key: "u.account", Value: account},
		},
		options.FindOne().SetProjection(bson.D{
			{Key: "_id", Value: 0},
			{Key: "lastSeenAt", Value: 1},
		}),
	).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read soak read cursor for %q: %w", roomID, err)
	}
	if document.LastSeenAt == nil {
		return time.Time{}, true, nil
	}
	return document.LastSeenAt.UTC(), true, nil
}

// AppendOwnedRooms takes ownership of rooms created during the run. Teardown
// only deletes rooms carrying this run's marker and only reaches rooms listed
// in an ownership chunk, so both are needed or the create lane would leave its
// rooms behind forever.
func (s *mongoSoakStore) AppendOwnedRooms(
	ctx context.Context,
	runID string,
	roomIDs []string,
) error {
	if runID == "" {
		return fmt.Errorf("append owned soak rooms requires a run ID")
	}
	if len(roomIDs) == 0 {
		return nil
	}
	// Teardown needs both the marker and a chunk that lists the room, so a crash
	// between these two writes leaks the room either way round. The marker goes
	// first because it is the auditable half: a marked room can still be found
	// by its run ID, while a chunk entry for an unmarked room is invisible to
	// teardown's per-room ownership check. The caller counts the failure as
	// untracked so the leak is reported rather than silent.
	// Only claim a room that is unowned or already ours. Teardown deletes every
	// room carrying this run's marker, so overwriting another run's marker would
	// enlist its rooms for deletion.
	marked, err := s.db.Collection("rooms").UpdateMany(
		ctx,
		bson.D{
			{Key: "_id", Value: bson.D{{Key: "$in", Value: roomIDs}}},
			{Key: "$or", Value: bson.A{
				bson.D{{Key: "soakRunId", Value: bson.D{{Key: "$exists", Value: false}}}},
				bson.D{{Key: "soakRunId", Value: runID}},
			}},
		},
		bson.D{{Key: "$set", Value: bson.D{{Key: "soakRunId", Value: runID}}}},
	)
	if err != nil {
		return fmt.Errorf("mark rooms owned by soak run %q: %w", runID, err)
	}
	// The guard cannot fire for a room this run just created, so a short match is
	// an anomaly worth naming. The chunk is still written: teardown re-checks
	// ownership per room, so listing an unclaimed room is inert, while dropping
	// the chunk would strand the rooms that were marked.
	if int(marked.MatchedCount) < len(roomIDs) {
		slog.Warn("some created rooms could not be claimed by this soak run",
			"runId", runID, "requested", len(roomIDs), "matched", marked.MatchedCount)
	}

	collection := s.db.Collection(soakOwnershipCollection)
	for start := 0; start < len(roomIDs); start += soakOwnershipChunkSize {
		end := min(start+soakOwnershipChunkSize, len(roomIDs))
		if err := s.insertOwnershipChunk(ctx, collection, runID, roomIDs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// insertOwnershipChunk retries past the duplicate-key race between two
// concurrent appends that computed the same next index. Reconciliation runs
// from several lane slots at once, so the read-then-insert is genuinely
// concurrent and a lost race would strand the room outside teardown's reach.
func (s *mongoSoakStore) insertOwnershipChunk(
	ctx context.Context,
	collection *mongo.Collection,
	runID string,
	roomIDs []string,
) error {
	const maxAttempts = 8
	for attempt := range maxAttempts {
		next, err := s.nextOwnershipChunkIndex(ctx, runID)
		if err != nil {
			return err
		}
		_, err = collection.InsertOne(ctx, soakOwnershipChunk{
			ID:        fmt.Sprintf("%s:%06d", runID, next+attempt),
			SoakRunID: runID,
			RoomIDs:   append([]string(nil), roomIDs...),
		})
		if err == nil {
			return nil
		}
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("append ownership chunk for run %q: %w", runID, err)
		}
	}
	return fmt.Errorf(
		"append ownership chunk for run %q: %d concurrent appends contended for the same index",
		runID, maxAttempts,
	)
}

// nextOwnershipChunkIndex keeps appended chunk IDs sorting after existing ones
// so the teardown pager's strictly-advancing cursor still reaches them.
func (s *mongoSoakStore) nextOwnershipChunkIndex(
	ctx context.Context,
	runID string,
) (int, error) {
	// The index has to come from the primary. A lagging secondary returns a
	// stale highest chunk, so every retry recomputes the same taken index and
	// the insert loop exhausts its attempts against contention that is not
	// there — leaving the created room outside teardown's reach.
	var chunk soakOwnershipChunk
	err := s.primary(soakOwnershipCollection).FindOne(
		ctx,
		soakOwnershipIDFilter(runID, ""),
		options.FindOne().
			SetProjection(bson.D{{Key: "_id", Value: 1}}).
			SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&chunk)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read latest ownership chunk for run %q: %w", runID, err)
	}
	index, parseErr := strconv.Atoi(strings.TrimPrefix(chunk.ID, runID+":"))
	if parseErr != nil {
		return 0, fmt.Errorf("parse ownership chunk index %q: %w", chunk.ID, parseErr)
	}
	return index + 1, nil
}

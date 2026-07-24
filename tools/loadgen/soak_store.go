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

//go:generate mockgen -destination=mock_soak_store_test.go -package=main . soakSeedStore

type soakSeedStore interface {
	BorrowUsers(ctx context.Context, siteID string, limit int) ([]model.User, error)
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
	_ soakSeedStore     = (*mongoSoakStore)(nil)
	_ soakTeardownStore = (*mongoSoakStore)(nil)
)

func soakUserFilter(siteID string) bson.D {
	return bson.D{
		{Key: "siteId", Value: siteID},
		{Key: "deactivated", Value: bson.D{{Key: "$ne", Value: true}}},
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
		{Key: "deactivated", Value: 1},
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
	filter := bson.D{{Key: "soakRunId", Value: runID}}
	for _, collection := range []string{"subscriptions", "rooms", soakOwnershipCollection} {
		if _, err := s.db.Collection(collection).DeleteMany(ctx, filter); err != nil {
			return fmt.Errorf("delete %s owned by soak run %q: %w", collection, runID, err)
		}
	}
	return nil
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
	if _, err := collection.DeleteMany(ctx, bson.D{{Key: "soakRunId", Value: runID}}); err != nil {
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
		FindOne(ctx, bson.D{{Key: "_id", Value: runID}}).
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
	filter := bson.D{{Key: "soakRunId", Value: runID}}
	if after != "" {
		filter = append(filter, bson.E{
			Key:   "_id",
			Value: bson.D{{Key: "$gt", Value: after}},
		})
	}
	var chunk soakOwnershipChunk
	err := s.db.Collection(soakOwnershipCollection).FindOne(
		ctx,
		filter,
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

func (s *mongoSoakStore) DeleteOwnedRoomBatch(
	ctx context.Context,
	runID string,
	roomIDs []string,
) error {
	roomFilter := bson.D{{Key: "roomId", Value: bson.D{{Key: "$in", Value: roomIDs}}}}
	for _, collection := range []string{"thread_subscriptions", "thread_rooms"} {
		if _, err := s.db.Collection(collection).DeleteMany(ctx, roomFilter); err != nil {
			return fmt.Errorf("delete %s for soak run %q: %w", collection, runID, err)
		}
	}
	if _, err := s.db.Collection(atrest.CollectionName).DeleteMany(
		ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: roomIDs}}}},
	); err != nil {
		return fmt.Errorf("delete wrapped DEKs for soak run %q: %w", runID, err)
	}
	ownedFilter := bson.D{
		{Key: "soakRunId", Value: runID},
		{Key: "roomId", Value: bson.D{{Key: "$in", Value: roomIDs}}},
	}
	if _, err := s.db.Collection("subscriptions").DeleteMany(ctx, ownedFilter); err != nil {
		return fmt.Errorf("delete subscriptions for soak run %q: %w", runID, err)
	}
	roomOwnedFilter := bson.D{
		{Key: "soakRunId", Value: runID},
		{Key: "_id", Value: bson.D{{Key: "$in", Value: roomIDs}}},
	}
	if _, err := s.db.Collection("rooms").DeleteMany(ctx, roomOwnedFilter); err != nil {
		return fmt.Errorf("delete rooms for soak run %q: %w", runID, err)
	}
	return nil
}

func (s *mongoSoakStore) DeleteOwnership(ctx context.Context, runID string) error {
	if _, err := s.db.Collection(soakOwnershipCollection).DeleteMany(
		ctx,
		bson.D{{Key: "soakRunId", Value: runID}},
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

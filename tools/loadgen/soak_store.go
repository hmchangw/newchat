package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

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

var _ soakSeedStore = (*mongoSoakStore)(nil)

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

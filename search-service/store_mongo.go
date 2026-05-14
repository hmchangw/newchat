package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

// mongoStore is the Mongo-backed implementation of MongoStore.
type mongoStore struct {
	apps          *mongo.Collection
	subscriptions *mongo.Collection
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		apps:          db.Collection("apps"),
		subscriptions: db.Collection("subscriptions"),
	}
}

func (s *mongoStore) SearchAppsByName(
	ctx context.Context,
	query, account string,
	assistantEnabled *bool,
	offset, limit int,
) ([]model.App, error) {
	pipeline := buildSearchAppsPipeline(query, account, assistantEnabled, offset, limit)
	cur, err := s.apps.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate apps: %w", err)
	}
	defer cur.Close(ctx)

	results := make([]model.App, 0)
	if err := cur.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode apps: %w", err)
	}
	return results, nil
}

// HydrateRooms fetches the caller's subscription documents for the
// given room IDs from the `subscriptions` collection and projects them into
// []model.SearchRoom. Documents are matched by `u.account` (caller's
// account) and `roomId` (one of the provided IDs). Missing subscriptions are
// silently omitted. The bson projection maps directly onto model.SearchRoom
// via the `bson:"..."` tags on that struct.
func (s *mongoStore) HydrateRooms(
	ctx context.Context,
	account string,
	roomIDs []string,
) ([]model.SearchRoom, error) {
	if len(roomIDs) == 0 {
		return []model.SearchRoom{}, nil
	}

	filter := bson.D{
		{Key: "u.account", Value: account},
		{Key: "roomId", Value: bson.D{{Key: "$in", Value: roomIDs}}},
	}

	cur, err := s.subscriptions.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find subscriptions: %w", err)
	}
	defer cur.Close(ctx)

	results := make([]model.SearchRoom, 0)
	if err := cur.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}

	// Reorder results to match the input roomIDs order (Mongo $in does
	// not preserve input order; the caller depends on ES relevance ranking
	// surviving the hydration step). Subscriptions missing from Mongo
	// (e.g. the user left between the ES query and this lookup) are
	// silently omitted.
	byRoomID := make(map[string]model.SearchRoom, len(results))
	for _, sub := range results {
		byRoomID[sub.RoomID] = sub
	}
	ordered := make([]model.SearchRoom, 0, len(results))
	for _, roomID := range roomIDs {
		if sub, ok := byRoomID[roomID]; ok {
			ordered = append(ordered, sub)
		}
	}
	return ordered, nil
}

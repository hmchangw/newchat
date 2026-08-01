package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/mongoutil"
)

// roomDoc is the projected room shape: identity plus the denormalized counter.
//
//nolint:unused // wired up once teams-room-inspector/main.go (task 5) constructs a mongoStore
type roomDoc struct {
	ID        string `bson:"_id"`
	UserCount int    `bson:"userCount"`
}

// subCountDoc is one $group output row: a room id and its subscription count.
//
//nolint:unused // wired up once teams-room-inspector/main.go (task 5) constructs a mongoStore
type subCountDoc struct {
	RoomID string `bson:"_id"`
	Count  int    `bson:"count"`
}

// mongoStore reads this site's rooms and subscriptions.
//
//nolint:unused // wired up once teams-room-inspector/main.go (task 5) constructs a mongoStore
type mongoStore struct {
	rooms *mongoutil.Collection[roomDoc]
	subs  *mongoutil.Collection[subCountDoc]
}

//nolint:unused // called from teams-room-inspector/main.go (task 5); until then only the tag-gated integration test exercises it
func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		rooms: mongoutil.NewCollection[roomDoc](db.Collection("rooms")),
		subs:  mongoutil.NewCollection[subCountDoc](db.Collection("subscriptions")),
	}
}

// RoomStates answers with two queries and no $lookup: a projected find over
// rooms, and a $group over subscriptions that counts server-side instead of
// shipping documents.
//
//nolint:unused // called through RoomStore once teams-room-inspector/main.go (task 5) constructs a mongoStore
func (s *mongoStore) RoomStates(ctx context.Context, roomIDs []string) (map[string]RoomState, error) {
	out := make(map[string]RoomState, len(roomIDs))
	if len(roomIDs) == 0 {
		return out, nil
	}

	rooms, err := s.rooms.FindMany(ctx, bson.M{"_id": bson.M{"$in": roomIDs}},
		mongoutil.WithProjection(bson.M{"_id": 1, "userCount": 1}))
	if err != nil {
		return nil, fmt.Errorf("find rooms: %w", err)
	}
	for _, r := range rooms {
		out[r.ID] = RoomState{RoomID: r.ID, Exists: true, UserCount: r.UserCount}
	}

	counts, err := s.subs.Aggregate(ctx, bson.A{
		bson.M{"$match": bson.M{"roomId": bson.M{"$in": roomIDs}}},
		bson.M{"$group": bson.M{"_id": "$roomId", "count": bson.M{"$sum": 1}}},
	})
	if err != nil {
		return nil, fmt.Errorf("count subscriptions by room: %w", err)
	}
	for _, c := range counts {
		st := out[c.RoomID]
		st.RoomID = c.RoomID
		st.SubscriptionCount = c.Count
		out[c.RoomID] = st
	}
	return out, nil
}

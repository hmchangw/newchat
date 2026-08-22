package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	roomsCollection         = "rooms"
	subscriptionsCollection = "subscriptions"
	// inChunkSize bounds each $in so a large Del- set does not build one
	// unbounded query document.
	inChunkSize = 500
)

// delNameRegex anchors on the marker so the scan can ride the name index rather
// than collection-scanning.
var delNameRegex = bson.Regex{Pattern: "^" + delPrefix}

// mongoStore reads rooms and subscriptions. Every query is a find with an
// explicit projection — the audit never writes.
type mongoStore struct {
	rooms *mongo.Collection
	subs  *mongo.Collection
}

func newMongoStore(db *mongo.Database) *mongoStore {
	return &mongoStore{
		rooms: db.Collection(roomsCollection),
		subs:  db.Collection(subscriptionsCollection),
	}
}

// delRooms returns rooms whose name carries the soft-delete marker, capped by
// limit (0 = uncapped).
func (s *mongoStore) delRooms(ctx context.Context, limit int) ([]roomRef, error) {
	opt := options.Find().SetProjection(bson.M{"_id": 1, "name": 1, "type": 1})
	if limit > 0 {
		opt = opt.SetLimit(int64(limit))
	}
	cur, err := s.rooms.Find(ctx, bson.M{"name": delNameRegex}, opt)
	if err != nil {
		return nil, fmt.Errorf("find Del- rooms: %w", err)
	}
	var out []roomRef
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode Del- rooms: %w", err)
	}
	return out, nil
}

// subsForRooms returns every subscription pointing at one of roomIDs.
func (s *mongoStore) subsForRooms(ctx context.Context, roomIDs []string) ([]subRef, error) {
	var out []subRef
	for _, ids := range chunk(roomIDs, inChunkSize) {
		batch, err := s.findSubs(ctx, bson.M{"roomId": bson.M{"$in": ids}}, 0)
		if err != nil {
			return nil, err
		}
		out = append(out, batch...)
	}
	return out, nil
}

// delNamedSubs returns subscriptions whose own denormalized name carries the
// marker — the reverse direction of the audit.
func (s *mongoStore) delNamedSubs(ctx context.Context, limit int) ([]subRef, error) {
	return s.findSubs(ctx, bson.M{"name": delNameRegex}, limit)
}

func (s *mongoStore) findSubs(ctx context.Context, filter bson.M, limit int) ([]subRef, error) {
	opt := options.Find().SetProjection(bson.M{"_id": 1, "name": 1, "roomId": 1, "roomType": 1, "siteId": 1})
	if limit > 0 {
		opt = opt.SetLimit(int64(limit))
	}
	cur, err := s.subs.Find(ctx, filter, opt)
	if err != nil {
		return nil, fmt.Errorf("find subscriptions: %w", err)
	}
	var out []subRef
	if err := cur.All(ctx, &out); err != nil {
		return nil, fmt.Errorf("decode subscriptions: %w", err)
	}
	return out, nil
}

// roomsByID resolves roomIDs to their room docs; a room with no local doc is
// simply absent from the map (cross-site or hard-deleted).
func (s *mongoStore) roomsByID(ctx context.Context, roomIDs []string) (map[string]roomRef, error) {
	out := make(map[string]roomRef, len(roomIDs))
	for _, ids := range chunk(roomIDs, inChunkSize) {
		cur, err := s.rooms.Find(ctx, bson.M{"_id": bson.M{"$in": ids}},
			options.Find().SetProjection(bson.M{"_id": 1, "name": 1, "type": 1}))
		if err != nil {
			return nil, fmt.Errorf("find rooms by id: %w", err)
		}
		var batch []roomRef
		if err := cur.All(ctx, &batch); err != nil {
			return nil, fmt.Errorf("decode rooms by id: %w", err)
		}
		for _, room := range batch {
			out[room.ID] = room
		}
	}
	return out, nil
}

// collect runs the four read phases and returns the audit input.
func collect(ctx context.Context, s *mongoStore, roomLimit, subLimit, sampleCap int) (auditInput, error) {
	rooms, err := s.delRooms(ctx, roomLimit)
	if err != nil {
		return auditInput{}, fmt.Errorf("collect Del- rooms: %w", err)
	}
	roomIDs := make([]string, 0, len(rooms))
	for _, room := range rooms {
		roomIDs = append(roomIDs, room.ID)
	}
	subs, err := s.subsForRooms(ctx, roomIDs)
	if err != nil {
		return auditInput{}, fmt.Errorf("collect subscriptions for Del- rooms: %w", err)
	}
	named, err := s.delNamedSubs(ctx, subLimit)
	if err != nil {
		return auditInput{}, fmt.Errorf("collect Del--named subscriptions: %w", err)
	}
	resolved, err := s.roomsByID(ctx, distinctRoomIDs(named))
	if err != nil {
		return auditInput{}, fmt.Errorf("resolve rooms for Del--named subscriptions: %w", err)
	}
	return auditInput{
		delRooms:     rooms,
		subsForRooms: subs,
		delNamedSubs: named,
		roomsForSubs: resolved,
		sampleCap:    sampleCap,
	}, nil
}

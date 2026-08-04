package mongorepo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const roomsCollection = "rooms"

// RoomRepo reads room metadata from MongoDB.
type RoomRepo struct {
	rooms *mongoutil.Collection[model.Room]
}

func NewRoomRepo(db *mongo.Database) *RoomRepo {
	return &RoomRepo{
		rooms: mongoutil.NewCollection[model.Room](db.Collection(roomsCollection)),
	}
}

// GetMinUserLastSeenAt returns (nil, nil) when the room is missing OR the
// field is unset — both mean "no read floor".
func (r *RoomRepo) GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error) {
	room, err := r.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		mongoutil.WithProjection(bson.M{"minUserLastSeenAt": 1, "_id": 0}),
	)
	if err != nil {
		return nil, fmt.Errorf("get room %s minUserLastSeenAt: %w", roomID, err)
	}
	if room == nil {
		return nil, nil
	}
	return room.MinUserLastSeenAt, nil
}

// GetRoomTimes returns lastMsgAt (zero time when unset) and createdAt for the given room.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist.
func (r *RoomRepo) GetRoomTimes(ctx context.Context, roomID string) (lastMsgAt, createdAt time.Time, err error) {
	room, err := r.rooms.FindByID(ctx, roomID, mongoutil.WithProjection(bson.M{"lastMsgAt": 1, "createdAt": 1, "_id": 0}))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get room times for %s: %w", roomID, err)
	}
	if room == nil {
		return time.Time{}, time.Time{}, fmt.Errorf("get room times for %s: %w", roomID, mongo.ErrNoDocuments)
	}
	if room.LastMsgAt != nil {
		lastMsgAt = *room.LastMsgAt
	}
	return lastMsgAt, room.CreatedAt, nil
}

// RoomTimes holds the two room timestamps GetRoomTimesByIDs projects.
type RoomTimes struct {
	LastMsgAt time.Time
	CreatedAt time.Time
}

// GetRoomTimesByIDs batches GetRoomTimes across ids into a single $in query,
// returning a map keyed by room ID. Rooms absent from Mongo are simply absent
// from the map (not an error). Empty ids returns an empty, non-nil map with no query.
func (r *RoomRepo) GetRoomTimesByIDs(ctx context.Context, ids []string) (map[string]RoomTimes, error) {
	out := make(map[string]RoomTimes, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rooms, err := r.rooms.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		mongoutil.WithProjection(bson.M{"_id": 1, "lastMsgAt": 1, "createdAt": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("get room times for %d rooms: %w", len(ids), err)
	}
	for i := range rooms {
		room := &rooms[i]
		var lastMsgAt time.Time
		if room.LastMsgAt != nil {
			lastMsgAt = *room.LastMsgAt
		}
		out[room.ID] = RoomTimes{LastMsgAt: lastMsgAt, CreatedAt: room.CreatedAt}
	}
	return out, nil
}

// GetRoomUserCount returns the room's userCount via a projected findOne.
// Returns mongo.ErrNoDocuments wrapped when the room does not exist —
// callers treat that as an infrastructure error (reaching this call already
// implies the caller is subscribed to the room).
func (r *RoomRepo) GetRoomUserCount(ctx context.Context, roomID string) (int, error) {
	room, err := r.rooms.FindByID(ctx, roomID, mongoutil.WithProjection(bson.M{"userCount": 1, "_id": 0}))
	if err != nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, err)
	}
	if room == nil {
		return 0, fmt.Errorf("get room %s userCount: %w", roomID, mongo.ErrNoDocuments)
	}
	return room.UserCount, nil
}

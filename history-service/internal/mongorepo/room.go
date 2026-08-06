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

// RoomNameType is the projected name/type row returned by GetRoomsNameType.
type RoomNameType struct {
	Name string
	Type model.RoomType
}

// GetRoomsNameType returns name/type for each existing room in roomIDs;
// absent IDs are simply missing from the map (not an error). Backs the
// forwarded-message room enrichment on history read paths.
func (r *RoomRepo) GetRoomsNameType(ctx context.Context, roomIDs []string) (map[string]RoomNameType, error) {
	out := make(map[string]RoomNameType, len(roomIDs))
	if len(roomIDs) == 0 {
		return out, nil
	}
	rooms, err := r.rooms.FindMany(ctx,
		bson.M{"_id": bson.M{"$in": roomIDs}},
		mongoutil.WithProjection(bson.M{"name": 1, "type": 1}),
	)
	if err != nil {
		return nil, fmt.Errorf("get rooms name/type: %w", err)
	}
	for i := range rooms {
		out[rooms[i].ID] = RoomNameType{Name: rooms[i].Name, Type: rooms[i].Type}
	}
	return out, nil
}

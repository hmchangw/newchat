package mongorepo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
)

const roomsCollection = "rooms"

type RoomRepo struct {
	rooms *Collection[model.Room]
}

func NewRoomRepo(db *mongo.Database) *RoomRepo {
	return &RoomRepo{
		rooms: NewCollection[model.Room](db.Collection(roomsCollection)),
	}
}

// GetMinUserLastSeenAt returns (nil, nil) when the room is missing OR the
// field is unset — both mean "no read floor".
func (r *RoomRepo) GetMinUserLastSeenAt(ctx context.Context, roomID string) (*time.Time, error) {
	room, err := r.rooms.FindOne(ctx,
		bson.M{"_id": roomID},
		WithProjection(bson.M{"minUserLastSeenAt": 1, "_id": 0}),
	)
	if err != nil {
		return nil, fmt.Errorf("get room %s minUserLastSeenAt: %w", roomID, err)
	}
	if room == nil {
		return nil, nil
	}
	return room.MinUserLastSeenAt, nil
}

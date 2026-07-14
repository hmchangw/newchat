package mongorepo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const threadRoomsCollection = "thread_rooms"

// threadRoomSort: newest activity first, stable secondary sort matching the compound indexes.
var threadRoomSort = bson.D{{Key: "lastMsgAt", Value: -1}, {Key: "threadParentCreatedAt", Value: 1}}

type ThreadRoomRepo struct {
	threadRooms *mongoutil.Collection[model.ThreadRoom]
}

func NewThreadRoomRepo(db *mongo.Database) *ThreadRoomRepo {
	return &ThreadRoomRepo{
		threadRooms: mongoutil.NewCollection[model.ThreadRoom](db.Collection(threadRoomsCollection)),
	}
}

// EnsureIndexes creates the compound indexes required by the thread-list queries. Idempotent.
func (r *ThreadRoomRepo) EnsureIndexes(ctx context.Context) error {
	col := r.threadRooms.Raw().Indexes()

	indexes := []mongo.IndexModel{
		// GetThreadRooms: all threads
		{Keys: bson.D{
			{Key: "roomId", Value: 1},
			{Key: "lastMsgAt", Value: -1},
			{Key: "threadParentCreatedAt", Value: 1},
		}},
		// GetFollowingThreadRooms: threads the user has replied to
		{Keys: bson.D{
			{Key: "roomId", Value: 1},
			{Key: "replyAccounts", Value: 1},
			{Key: "lastMsgAt", Value: -1},
			{Key: "threadParentCreatedAt", Value: 1},
		}},
	}

	if _, err := col.CreateMany(ctx, indexes, options.CreateIndexes()); err != nil {
		return fmt.Errorf("ensure thread_rooms indexes: %w", err)
	}
	// The thread_subscriptions indexes are owned by ThreadSubscriptionRepo.
	return nil
}

func (r *ThreadRoomRepo) GetThreadRooms(ctx context.Context, roomID string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[model.ThreadRoom], error) {
	page, err := r.threadRooms.AggregatePaged(ctx, allThreadsPipeline(roomID, accessSince), req)
	if err != nil {
		return mongoutil.OffsetPage[model.ThreadRoom]{}, fmt.Errorf("querying thread rooms: %w", err)
	}
	return page, nil
}

func (r *ThreadRoomRepo) GetFollowingThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[model.ThreadRoom], error) {
	page, err := r.threadRooms.AggregatePaged(ctx, followingThreadsPipeline(roomID, account, accessSince), req)
	if err != nil {
		return mongoutil.OffsetPage[model.ThreadRoom]{}, fmt.Errorf("querying following thread rooms: %w", err)
	}
	return page, nil
}

// Unread = subscribed AND lastMsgAt > lastSeenAt.
func (r *ThreadRoomRepo) GetUnreadThreadRooms(ctx context.Context, roomID, account string, accessSince *time.Time, req mongoutil.OffsetPageRequest) (mongoutil.OffsetPage[model.ThreadRoom], error) {
	page, err := r.threadRooms.AggregatePaged(ctx, unreadThreadsPipeline(roomID, account, accessSince), req)
	if err != nil {
		return mongoutil.OffsetPage[model.ThreadRoom]{}, fmt.Errorf("querying unread thread rooms: %w", err)
	}
	return page, nil
}

// GetMinThreadUserLastSeenAt reads thread_rooms.minUserLastSeenAt for threadRoomID.
// Returns (nil, nil) when the field is unset or the document is missing.
func (r *ThreadRoomRepo) GetMinThreadUserLastSeenAt(ctx context.Context, threadRoomID string) (*time.Time, error) {
	tr, err := r.threadRooms.FindOne(ctx,
		bson.M{"_id": threadRoomID},
		mongoutil.WithProjection(bson.M{"minUserLastSeenAt": 1, "_id": 0}),
	)
	if err != nil {
		return nil, fmt.Errorf("get thread room %s minUserLastSeenAt: %w", threadRoomID, err)
	}
	if tr == nil {
		return nil, nil
	}
	return tr.MinUserLastSeenAt, nil
}

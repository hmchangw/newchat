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

const threadSubscriptionsCollection = "thread_subscriptions"

// ThreadSubRow is the flat projection a user-thread-inbox page yields: the
// user's thread-subscription state joined with the thread room's activity and
// parent pointers, plus the owning room's name/type. It feeds the leaf
// handler's message hydration.
type ThreadSubRow struct {
	ThreadRoomID    string         `bson:"_id"` // remapped from thread_subscriptions.threadRoomId
	RoomID          string         `bson:"roomId"`
	SiteID          string         `bson:"siteId"`
	RoomName        string         `bson:"roomName"`
	RoomType        model.RoomType `bson:"roomType"`
	ParentMessageID string         `bson:"parentMessageId"`
	LastMsgID       string         `bson:"lastMsgId"`
	LastMsgAt       time.Time      `bson:"lastMsgAt"`
	LastSeenAt      *time.Time     `bson:"lastSeenAt"`
	HasMention      bool           `bson:"hasMention"`
}

// ThreadSubscriptionRepo owns the thread_subscriptions collection and the
// user-driven thread-inbox query.
type ThreadSubscriptionRepo struct {
	subs *mongoutil.Collection[ThreadSubRow]
}

func NewThreadSubscriptionRepo(db *mongo.Database) *ThreadSubscriptionRepo {
	return &ThreadSubscriptionRepo{
		subs: mongoutil.NewCollection[ThreadSubRow](db.Collection(threadSubscriptionsCollection)),
	}
}

// EnsureIndexes creates the (userAccount) index this service's thread-inbox query
// needs. The (threadRoomId, userAccount) unique index is owned by room-service —
// verified, not created here. Idempotent.
func (r *ThreadSubscriptionRepo) EnsureIndexes(ctx context.Context) error {
	// thread_subscriptions.{threadRoomId,userAccount} (unique) is owned by room-service; verify + warn only, never create.
	mongoutil.WarnMissingIndexes(ctx, r.subs.Raw(), "threadRoomId_1_userAccount_1")
	// Fronts ListUserThreadSubscriptions' userAccount $match.
	if _, err := r.subs.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userAccount", Value: 1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (userAccount) index: %w", err)
	}
	return nil
}

// ListUserThreadSubscriptions returns the account's thread subscriptions on this
// site, newest activity first, strictly after the (lastMsgAt, threadRoomId)
// cursor (nil cursorLastMsgAt = first page). It fetches limit+1 rows to report
// hasMore, then trims to limit.
func (r *ThreadSubscriptionRepo) ListUserThreadSubscriptions(
	ctx context.Context, account string, cursorLastMsgAt *time.Time,
	cursorThreadRoomID string, limit int,
) ([]ThreadSubRow, bool, error) {
	rows, err := r.subs.Aggregate(ctx, userThreadSubscriptionsPipeline(account, cursorLastMsgAt, cursorThreadRoomID, limit))
	if err != nil {
		return nil, false, fmt.Errorf("querying user thread subscriptions: %w", err)
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	return rows, hasMore, nil
}

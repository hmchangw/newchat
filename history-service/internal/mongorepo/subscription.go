package mongorepo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const subscriptionsCollection = "subscriptions"

type SubscriptionRepo struct {
	subscriptions *mongoutil.Collection[model.Subscription]
}

func NewSubscriptionRepo(db *mongo.Database) *SubscriptionRepo {
	return &SubscriptionRepo{
		subscriptions: mongoutil.NewCollection[model.Subscription](db.Collection(subscriptionsCollection)),
	}
}

// subscriptionReadProjection is the field set GetSubscription returns — the
// union of every Subscription field its call sites read: canBypassLargeRoomPin
// (roles, u.account) and the PinnedBy participant (u.id, u.account).
// model.Subscription carries ~30 fields including the unbounded threadUnread
// list, and this sits on the pin/unpin path uncached, so the rest is decode
// cost for nothing. _id is excluded because no call site reads sub.ID. Keep in
// sync with the Subscription field reads in service/pin.go; the unit tests in
// subscription_unit_test.go and the projection-field integration test guard drift.
var subscriptionReadProjection = bson.M{"u": 1, "roles": 1, "_id": 0}

// GetSubscription returns a call-site-projected subscription for a user in a
// room (see subscriptionReadProjection), or (nil, nil) when not subscribed.
func (r *SubscriptionRepo) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	return r.subscriptions.FindOne(ctx,
		bson.M{"u.account": account, "roomId": roomID},
		mongoutil.WithProjection(subscriptionReadProjection),
	)
}

// GetHistorySharedSince returns (nil, true, nil) = full access; (&t, true, nil) = restricted; (nil, false, nil) = not subscribed.
func (r *SubscriptionRepo) GetHistorySharedSince(ctx context.Context, account, roomID string) (*time.Time, bool, error) {
	sub, err := r.subscriptions.FindOne(ctx,
		bson.M{"u.account": account, "roomId": roomID},
		mongoutil.WithProjection(bson.M{"historySharedSince": 1, "_id": 0}),
	)
	if err != nil {
		return nil, false, err
	}
	if sub == nil {
		return nil, false, nil
	}
	return sub.HistorySharedSince, true, nil
}

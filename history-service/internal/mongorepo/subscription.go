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

// subscriptionReadProjection trims the ~30-field doc to what service/pin.go reads.
// The excluded top-level _id is not sub.User.ID — that is u._id, inside u.
var subscriptionReadProjection = bson.M{"u": 1, "roles": 1, "_id": 0}

// GetSubscription returns a projected subscription, or (nil, nil) when not subscribed.
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

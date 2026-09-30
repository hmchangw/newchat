package mongorepo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
)

const subscriptionsCollection = "subscriptions"

type SubscriptionRepo struct {
	subscriptions *mongoutil.Collection[model.Subscription]
}

// NewSubscriptionRepo fronts the subscriptions collection. readPref pins this
// collection's reads: these are access-control reads, and the service-wide
// default is secondaryPreferred — a lagging secondary would let the sub-cache's
// evict-then-reload re-cache a stale pre-revocation grant, so callers pass strict
// primary here (read-your-writes, and fail-closed on a primary outage).
func NewSubscriptionRepo(db *mongo.Database, readPref *readpref.ReadPref) *SubscriptionRepo {
	coll := db.Collection(subscriptionsCollection, options.Collection().SetReadPreference(readPref))
	return &SubscriptionRepo{
		subscriptions: mongoutil.NewCollection[model.Subscription](coll),
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

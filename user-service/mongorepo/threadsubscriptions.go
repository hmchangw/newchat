package mongorepo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/pipelines"
)

const threadSubscriptionsCollection = "thread_subscriptions"

// maxThreadSubscriptions caps the badge read at the newest N thread-subs,
// bounding both the read and the downstream per-site fan-out.
const maxThreadSubscriptions = 500

// ThreadSubscriptionRepo reads the local (home-site) thread_subscriptions replica.
// Typed to the join result since the only read aggregates against subscriptions.
type ThreadSubscriptionRepo struct {
	threadSubs *mongoutil.Collection[model.ThreadUnreadRow]
}

// NewThreadSubscriptionRepo builds a ThreadSubscriptionRepo over db. The badge/tally
// read stays on the primary so it reflects a just-changed thread subscription.
func NewThreadSubscriptionRepo(db *mongo.Database) *ThreadSubscriptionRepo {
	return &ThreadSubscriptionRepo{
		threadSubs: mongoutil.NewCollection[model.ThreadUnreadRow](db.Collection(threadSubscriptionsCollection)),
	}
}

// EnsureIndexes creates the (userAccount, createdAt desc) index backing
// ListByAccount's newest-first read. Idempotent; independent of other owners.
func (r *ThreadSubscriptionRepo) EnsureIndexes(ctx context.Context) error {
	if _, err := r.threadSubs.Raw().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userAccount", Value: 1}, {Key: "createdAt", Value: -1}},
	}); err != nil {
		return fmt.Errorf("ensure thread_subscriptions (userAccount,createdAt) index: %w", err)
	}
	return nil
}

// ListByAccount returns the account's newest maxThreadSubscriptions accessible
// thread-subs (across every site), newest-first, each carrying its room type.
func (r *ThreadSubscriptionRepo) ListByAccount(ctx context.Context, account string) ([]model.ThreadUnreadRow, error) {
	// $lookup justification: the join reads three facts off the account's
	// subscription row — membership (access gate), unsubscribed-app status, and
	// roomType (DM tally) — and applies the gate BEFORE the limit so an
	// inaccessible thread can't crowd out a live one. Both keys are indexed.
	match := bson.M{"userAccount": account}
	pipeline := bson.A{
		bson.M{"$match": match},
		bson.M{"$sort": bson.D{{Key: "createdAt", Value: -1}}},
		bson.M{"$lookup": bson.M{
			"from": subscriptionsCollection,
			"let":  bson.M{"rid": "$roomId"},
			"pipeline": bson.A{
				bson.M{"$match": bson.M{
					"$expr":     bson.M{"$eq": bson.A{"$roomId", "$$rid"}},
					"u.account": account,
					// Keep everything but an app room the user unsubscribed from:
					// a botDM facing a human or p_admin is not an app room, and its
					// isSubscribed=false is structural, not a user choice.
					"$nor": bson.A{pipelines.UnsubscribedAppFilter()},
				}},
				bson.M{"$project": bson.M{"_id": 0, "roomType": 1, "name": 1}},
			},
			"as": "sub",
		}},
		// No surviving subscription ⇒ drop the thread.
		bson.M{"$match": bson.M{"sub": bson.M{"$ne": bson.A{}}}},
		bson.M{"$limit": int64(maxThreadSubscriptions)},
		bson.M{"$project": bson.M{
			"_id":          0,
			"threadRoomId": 1,
			"roomId":       1,
			"siteId":       1,
			"lastSeenAt":   1,
			"hasMention":   1,
			"roomType":     bson.M{"$arrayElemAt": bson.A{"$sub.roomType", 0}},
			// name is the counterpart account on dm/botDM rows; the caller needs
			// it to resolve the effective room type for the DM unread tally.
			"name": bson.M{"$arrayElemAt": bson.A{"$sub.name", 0}},
		}},
	}
	return r.threadSubs.Aggregate(ctx, pipeline)
}

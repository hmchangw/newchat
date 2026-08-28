package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type MongoStore struct {
	subTier *subauthcache.Tier // owns the subscription L2 + its own breaker
	// metaTier is built once, for the same reason its fetch guard always was: a
	// tier's closures escape to the heap, so constructing one per GetRoomMeta
	// would allocate on the message-send hot path.
	metaTier *roommetacache.L2Tier
}

// NewMongoStore wires the subscription and room-meta reads behind two
// independent circuit breakers. Keeping them separate is load-bearing: a warm
// room-meta L2 hit must not reset the subscription breaker's failure count, or
// cold subscription misses would never trip fast-fail during a Mongo outage.
func NewMongoStore(db *mongo.Database, valkey valkeyutil.Client, metaTTL, subTTL time.Duration, subBreaker, metaBreaker *circuitbreaker.Breaker) *MongoStore {
	rooms := db.Collection("rooms")
	return &MongoStore{
		subTier: subauthcache.NewTier(valkey, db.Collection("subscriptions"), subTTL,
			subBreaker, cachemetrics.For("subauth", "l2")),
		metaTier: roommetacache.NewL2Tier(valkey, rooms, metaTTL,
			metaBreaker, cachemetrics.For("roommeta", "l2")),
	}
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	auth, subscribed, err := s.subTier.Resolve(ctx, roomID, account)
	if err != nil {
		return nil, fmt.Errorf("get subscription for %s in %s: %w", account, roomID, err)
	}
	if !subscribed {
		return nil, fmt.Errorf("user %s not subscribed to room %s: %w", account, roomID, errNotSubscribed)
	}
	return &model.Subscription{
		User:  model.SubscriptionUser{ID: auth.ID, Account: auth.Account},
		Roles: auth.Roles,
	}, nil
}

// GetRoomMeta fences only the Mongo fetch behind the meta breaker, never the L2
// read in front of it: an open breaker must still serve cached rooms, since the
// L2 is the only tier that can answer during the outage that opened it.
//
// The "a missing room is not a Mongo failure" rule rides on the breaker itself
// (see metaBreakerFailure), so the guard is just its Do.
func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	meta, err := s.metaTier.Get(ctx, roomID)
	if err != nil {
		return roommetacache.Meta{}, fmt.Errorf("get room meta for %s: %w", roomID, err)
	}
	return meta, nil
}

// metaBreakerFailure is the failure predicate the room-meta breaker must be
// constructed with. A room that does not exist is a healthy answer from a
// healthy Mongo; counting it would let a burst of requests for deleted or
// mistyped rooms open the breaker and degrade every other room's meta read.
var metaBreakerFailure = mongoutil.BreakerFailure()

// subBreakerFailure is the failure predicate the subscription breaker must be
// constructed with. Unlike metaBreakerFailure it is not about not-founds:
// subauthcache's loader already turns mongo.ErrNoDocuments into "not
// subscribed" before the breaker sees it. What this buys is the context.Canceled
// exemption — a cancelled caller is evidence about the caller, not about Mongo,
// and counting it would let cancellations fence a healthy database. Note the
// asymmetry it preserves: context.DeadlineExceeded still counts, because that is
// how an unreachable MongoDB reports itself.
var subBreakerFailure = mongoutil.BreakerFailure()

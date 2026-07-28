package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
	"github.com/hmchangw/chat/pkg/subauthcache"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type MongoStore struct {
	subscriptions *mongo.Collection
	rooms         *mongo.Collection
	valkey        valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL       time.Duration
	subTTL        time.Duration
	subBreaker    *circuitbreaker.Breaker // guards the subscription-authz Mongo loader
	metaBreaker   *circuitbreaker.Breaker // guards the room-meta read-through, independently
	metaRec       roommetacache.Recorder
	subRec        subauthcache.Recorder
}

// NewMongoStore wires the subscription and room-meta reads behind two
// independent circuit breakers. Keeping them separate is load-bearing: a warm
// room-meta L2 hit must not reset the subscription breaker's failure count, or
// cold subscription misses would never trip fast-fail during a Mongo outage.
func NewMongoStore(db *mongo.Database, valkey valkeyutil.Client, metaTTL, subTTL time.Duration, subBreaker, metaBreaker *circuitbreaker.Breaker) *MongoStore {
	return &MongoStore{
		subscriptions: db.Collection("subscriptions"),
		rooms:         db.Collection("rooms"),
		valkey:        valkey,
		metaTTL:       metaTTL,
		subTTL:        subTTL,
		subBreaker:    subBreaker,
		metaBreaker:   metaBreaker,
		metaRec:       cachemetrics.For("roommeta", "l2"),
		subRec:        cachemetrics.For("subauth", "l2"),
	}
}

func (s *MongoStore) GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error) {
	loader := func(ctx context.Context, roomID, account string) (subauthcache.SubAuth, bool, error) {
		var (
			auth       subauthcache.SubAuth
			subscribed bool
		)
		err := s.subBreaker.Do(func() error {
			var e error
			auth, subscribed, e = subauthcache.FetchFromMongo(ctx, s.subscriptions, roomID, account)
			return e
		})
		return auth, subscribed, err
	}
	auth, subscribed, err := subauthcache.ReadThrough(ctx, s.valkey, loader, roomID, account, s.subTTL, s.subRec)
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

func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	var meta roommetacache.Meta
	err := s.metaBreaker.Do(func() error {
		var e error
		meta, e = roommetacache.ReadThrough(ctx, s.valkey, s.rooms, roomID, s.metaTTL, s.metaRec)
		return e
	})
	if err != nil {
		return roommetacache.Meta{}, fmt.Errorf("get room meta for %s: %w", roomID, err)
	}
	return meta, nil
}

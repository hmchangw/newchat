package main

import (
	"context"
	"errors"
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
	rooms       *mongo.Collection
	valkey      valkeyutil.Client // nil disables the L2 tier (pure Mongo)
	metaTTL     time.Duration
	metaBreaker *circuitbreaker.Breaker // guards the room-meta read-through, independently
	metaRec     roommetacache.Recorder
	subTier     *subauthcache.Tier // owns the subscription L2 + its own breaker
	// metaOpts is built once: the guard is a method value, so passing it inline
	// on every GetRoomMeta would heap-allocate it on the message-send hot path.
	metaOpts []roommetacache.ReadThroughOption
}

// NewMongoStore wires the subscription and room-meta reads behind two
// independent circuit breakers. Keeping them separate is load-bearing: a warm
// room-meta L2 hit must not reset the subscription breaker's failure count, or
// cold subscription misses would never trip fast-fail during a Mongo outage.
func NewMongoStore(db *mongo.Database, valkey valkeyutil.Client, metaTTL, subTTL time.Duration, subBreaker, metaBreaker *circuitbreaker.Breaker) *MongoStore {
	s := &MongoStore{
		rooms:       db.Collection("rooms"),
		valkey:      valkey,
		metaTTL:     metaTTL,
		metaBreaker: metaBreaker,
		metaRec:     cachemetrics.For("roommeta", "l2"),
		subTier: subauthcache.NewTier(valkey, db.Collection("subscriptions"), subTTL,
			subBreaker, cachemetrics.For("subauth", "l2")),
	}
	s.metaOpts = []roommetacache.ReadThroughOption{roommetacache.WithFetchGuard(metaBreaker.Do)}
	return s
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
// (see MetaBreakerFailure), so the guard is just its Do.
func (s *MongoStore) GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error) {
	meta, err := roommetacache.ReadThrough(ctx, s.valkey, s.rooms, roomID, s.metaTTL, s.metaRec, s.metaOpts...)
	if err != nil {
		return roommetacache.Meta{}, fmt.Errorf("get room meta for %s: %w", roomID, err)
	}
	return meta, nil
}

// MetaBreakerFailure is the failure predicate the room-meta breaker must be
// constructed with. A room that does not exist is a healthy answer from a
// healthy Mongo; counting it would let a burst of requests for deleted or
// mistyped rooms open the breaker and degrade every other room's meta read.
func MetaBreakerFailure(err error) bool {
	return err != nil && !errors.Is(err, mongo.ErrNoDocuments)
}

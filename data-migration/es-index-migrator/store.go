package main

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
)

// MessageSource streams a room's Cassandra message history in [from, to)
// to fn, one row at a time. fn must not retain row beyond its call —
// implementations reuse row's underlying buffers across calls where
// possible to avoid materializing a whole room's history in memory at
// once. A non-nil error from fn aborts the stream and is returned as-is.
type MessageSource interface {
	StreamMessages(ctx context.Context, siteID, roomID string, from, to time.Time, fn func(cassandra.Message) error) error
}

// SubscriptionSource reads a site's current MongoDB subscriptions collection.
type SubscriptionSource interface {
	// RoomIDs returns the distinct room IDs any of the site's subscriptions
	// reference — the candidate set for the messages backfill (Collection 1).
	RoomIDs(ctx context.Context, siteID string) ([]string, error)
	// Subscriptions returns every current subscription for the site — the
	// full source for the spotlight and user-room backfills (Collections
	// 2 and 3). Every row is an active membership (subscriptions are
	// hard-deleted on leave, never soft-flagged — see Global Constraints).
	Subscriptions(ctx context.Context, siteID string) ([]model.Subscription, error)
}

// ESStore is the narrow slice of searchengine.SearchEngine the flusher
// needs. Defined here (the consumer), not in pkg/searchengine, per
// CLAUDE.md's interface convention.
type ESStore interface {
	Bulk(ctx context.Context, actions []searchengine.BulkAction) ([]searchengine.BulkResult, error)
}

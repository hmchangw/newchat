package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore
//go:generate mockgen -destination=mock_keystore_test.go -package=main . RoomKeyProvider
//go:generate mockgen -destination=mock_parentfetcher_test.go -package=main . ParentFetcher

// Store defines data access operations for the broadcast worker.
type Store interface {
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error)
	// ListSubscriptions returns the room's subscriptions straight from Mongo,
	// as whole documents: callers read fields beyond the fan-out projection, and
	// a partial projection would hand them a model.Subscription whose unselected
	// fields are silently zero. Deliberately NOT cache-fronted — unlike the
	// roomsubcache path it replaced, DM fan-out now fails when Mongo is down.
	ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error)
	GetThreadFollowers(ctx context.Context, parentMessageID string) (map[string]struct{}, error)
	// GetHistorySharedSince returns each account's room-subscription historySharedSince
	// (nil when unrestricted; absent from the map when the account has no subscription
	// in the room — key-presence encodes membership).
	GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
}

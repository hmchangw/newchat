package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore
//go:generate mockgen -destination=mock_keystore_test.go -package=main . RoomKeyProvider

// Store defines data access operations for the broadcast worker.
type Store interface {
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error)
	FetchAndUpdateRoom(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool) (*model.Room, error)
	SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string) error
}

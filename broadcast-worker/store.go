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
	ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error)
	GetThreadFollowers(ctx context.Context, parentMessageID string) (map[string]struct{}, error)
	// UpdateRoomLastMessage records the room's newest message; pvw (nil for
	// system messages) is the denormalized preview to persist alongside,
	// previewAsOf its canonical-event-Timestamp watermark.
	UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, pvw *model.PreviewMessage, previewAsOf int64) error
	// SetRoomPreviewMessage persists a post-mutation (edit/delete) preview,
	// watermark-guarded so redeliveries and races cannot regress a newer one.
	SetRoomPreviewMessage(ctx context.Context, roomID string, pvw *model.PreviewMessage, asOf int64) error
	// ClearRoomPreviewMessage removes the stored preview after a mutation left
	// the room with no eligible survivor, watermark-guarded like Set.
	ClearRoomPreviewMessage(ctx context.Context, roomID string, asOf int64) error
	// AppNameByAccount returns the app display name for a bot account
	// (assistant.name), or ("", nil) when no app matches.
	AppNameByAccount(ctx context.Context, botAccount string) (string, error)
	// SetSubscriptionMentions flags accounts as mentioned, unless a given account
	// already read past msgCreatedAt (lastSeenAt >= msgCreatedAt) — otherwise an
	// async mention write can clobber a read-clear that happened first (#467).
	SetSubscriptionMentions(ctx context.Context, roomID string, accounts []string, msgCreatedAt time.Time) error
	// GetHistorySharedSince returns each account's room-subscription historySharedSince
	// (nil when unrestricted; absent from the map when the account has no subscription
	// in the room — key-presence encodes membership).
	GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
	// AdvanceSubscriptionLastSeen advances the sender's own lastSeenAt: sending
	// implies they've seen up to their own message, keeping the room read-floor
	// from counting the sender against it (#396).
	AdvanceSubscriptionLastSeen(ctx context.Context, roomID, account string, at time.Time) error
}

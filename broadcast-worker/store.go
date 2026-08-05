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
	UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool) error
	// SetRoomLastMessage walks a room's denormalized last-message fields back after a delete.
	// Direct write (not coalesced) — deletes are rare. lastMsgID/lastMsgAt nil clear those fields
	// (room emptied); setMentionAll gates lastMentionAllAt (set to mentionAllAt, cleared when nil,
	// left untouched when setMentionAll is false).
	SetRoomLastMessage(ctx context.Context, roomID string, lastMsgID *string, lastMsgAt *time.Time, setMentionAll bool, mentionAllAt *time.Time) error
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

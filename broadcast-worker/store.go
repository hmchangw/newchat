package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore
//go:generate mockgen -destination=mock_keystore_test.go -package=main . RoomKeyProvider
//go:generate mockgen -destination=mock_parentfetcher_test.go -package=main . ParentFetcher
//go:generate mockgen -destination=mock_lastmsgfetcher_test.go -package=main . LastMessageFetcher

// Store defines data access operations for the broadcast worker.
type Store interface {
	GetRoom(ctx context.Context, roomID string) (*model.Room, error)
	GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error)
	ListSubscriptions(ctx context.Context, roomID string) ([]model.Subscription, error)
	GetThreadFollowers(ctx context.Context, parentMessageID string) (map[string]struct{}, error)
	// UpdateRoomLastMessage advances the room's lastMsgId/lastMsgAt/lastMsg
	// pointer to a newly created message. preview is the denormalized lastMsg
	// document (Msg already blanked by the caller for encrypted rooms).
	UpdateRoomLastMessage(ctx context.Context, roomID, msgID string, msgAt time.Time, mentionAll bool, preview *model.LastMessagePreview) error
	// RewindRoomLastMessage rewinds the room's last-message state after a
	// delete (guarded UpdateOnes; non-matching guards are benign no-ops — a
	// concurrent newer message won). pointer is the newest surviving message
	// of ANY type (system notices included) and rewinds lastMsgId/lastMsgAt;
	// survivor is the newest surviving non-system message and rewinds the
	// lastMsg preview — they differ when a system notice sits on top.
	// pointer == nil clears the fields like a fresh room (survivor is nil
	// then too).
	RewindRoomLastMessage(ctx context.Context, roomID, deletedMsgID string, pointer *model.LastMessagePointer, survivor *model.LastMessagePreview, updatedAt time.Time) error
	// SetRoomLastMessageEdited patches lastMsg.msg/lastMsg.encMsg/lastMsg.editedAt
	// after an edit, but ONLY if the room still points at editedMsgID (guarded
	// UpdateOne; non-matching pointer is a benign no-op). Exactly one of
	// newMsg/encMsg is set: encrypted rooms pass the content ciphertext with
	// newMsg=="", plaintext rooms pass encMsg==nil, which $unsets lastMsg.encMsg
	// so a stale ciphertext never survives a plaintext patch.
	SetRoomLastMessageEdited(ctx context.Context, roomID, editedMsgID, newMsg string, encMsg json.RawMessage, editedAt time.Time) error
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

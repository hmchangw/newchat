package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store,ThreadStore
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore

// Store defines Cassandra persistence operations for the message worker.
type Store interface {
	SaveMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error
	SaveThreadMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string, threadRoomID string) (*int, error)
	GetMessageSender(ctx context.Context, messageID string) (*cassParticipant, error)
	// GetQuotedParentSnapshot re-projects the authoritative quoted-parent snapshot
	// for messageID from messages_by_id (decrypting the body when the store has a
	// cipher). The bool is false (nil error) when the row is absent. Used to
	// correct an untrusted degraded-mode placeholder snapshot before the durable
	// write; MessageLink is left empty (the caller preserves the gatekeeper-built link).
	GetQuotedParentSnapshot(ctx context.Context, messageID string) (*cassandra.QuotedParentMessage, bool, error)
	// GetMessageCreatedAt returns the authoritative createdAt for a message from
	// messages_by_id. The bool is false (nil error) when the row is absent.
	GetMessageCreatedAt(ctx context.Context, messageID string) (time.Time, bool, error)
	UpdateParentMessageThreadRoomID(ctx context.Context, parentMessageID, roomID string, parentCreatedAt time.Time, threadRoomID string) error
}

// ThreadStore defines MongoDB operations for thread room and subscription management.
type ThreadStore interface {
	CreateThreadRoom(ctx context.Context, room *model.ThreadRoom) error
	GetThreadRoomByParentMessageID(ctx context.Context, parentMessageID string) (*model.ThreadRoom, error)
	InsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	// MarkThreadSubscriptionMention flags sub as mentioned, unless the account
	// already read past sub.CreatedAt (the mentioning message's time) — otherwise
	// an async mention write can clobber a read-clear that happened first (#467).
	MarkThreadSubscriptionMention(ctx context.Context, sub *model.ThreadSubscription) error
	// UpdateThreadRoomLastMessage bumps the last-message pointer and $addToSet-merges
	// the supplied accounts (replier + parent author on the subsequent-reply path) into
	// replyAccounts in one write.
	UpdateThreadRoomLastMessage(ctx context.Context, threadRoomID, lastMsgID string, replyAccounts []string, lastMsgAt time.Time) error
	// AddReplyAccounts $addToSet-merges accounts into thread_rooms.replyAccounts.
	// Used by paths that don't already update lastMsg (first-reply parent author,
	// mention-only subscribers) so the field mirrors thread_subscriptions membership.
	AddReplyAccounts(ctx context.Context, threadRoomID string, accounts []string) error
	// GetHistorySharedSince returns each account's room-subscription historySharedSince
	// (nil when unrestricted; absent from the map when the account has no subscription
	// in the room — key-presence encodes membership).
	GetHistorySharedSince(ctx context.Context, roomID string, accounts []string) (map[string]*time.Time, error)
	// AdvanceThreadSubscriptionLastSeen advances the replier's own lastSeenAt: replying
	// implies they've seen up to their own reply, keeping the thread read-floor
	// (minUserLastSeenAt) from counting the replier against it (#396).
	AdvanceThreadSubscriptionLastSeen(ctx context.Context, threadRoomID, account string, at time.Time) error
}

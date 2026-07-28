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
	// EnsureThreadRoom returns the thread room for room.ParentMessageID, atomically creating
	// it from room when absent — a single round trip via an upserting FindOneAndUpdate, so the
	// hot subsequent-reply path never attempts (and fails) an insert against the unique index.
	// created is true iff this call inserted the room (i.e. this is the first reply).
	EnsureThreadRoom(ctx context.Context, room *model.ThreadRoom) (stored *model.ThreadRoom, created bool, err error)
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
	// UpsertThreadSubscriptionAdvancingLastSeen creates sub's (threadRoomId, userAccount)
	// subscription when missing and advances its lastSeenAt to at via $max, in a single
	// write. It merges UpsertThreadSubscription + AdvanceThreadSubscriptionLastSeen for the
	// replier on the hot path: replying implies the replier has seen up to their own reply
	// (#396), so the new sub is seeded with lastSeenAt=at and an existing one is moved
	// forward (never backward).
	UpsertThreadSubscriptionAdvancingLastSeen(ctx context.Context, sub *model.ThreadSubscription, at time.Time) error
}

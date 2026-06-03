package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store,ThreadStore
//go:generate mockgen -destination=mock_userstore_test.go -package=main github.com/hmchangw/chat/pkg/userstore UserStore

// Store defines Cassandra persistence operations for the message worker.
type Store interface {
	SaveMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string) error
	SaveThreadMessage(ctx context.Context, msg *model.Message, sender *cassParticipant, siteID string, threadRoomID string) error
	GetMessageSender(ctx context.Context, messageID string) (*cassParticipant, error)
	UpdateParentMessageThreadRoomID(ctx context.Context, parentMessageID, roomID string, parentCreatedAt time.Time, threadRoomID string) error
}

// ThreadStore defines MongoDB operations for thread room and subscription management.
type ThreadStore interface {
	CreateThreadRoom(ctx context.Context, room *model.ThreadRoom) error
	GetThreadRoomByParentMessageID(ctx context.Context, parentMessageID string) (*model.ThreadRoom, error)
	InsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	UpsertThreadSubscription(ctx context.Context, sub *model.ThreadSubscription) error
	MarkThreadSubscriptionMention(ctx context.Context, sub *model.ThreadSubscription) error
	// UpdateThreadRoomLastMessage bumps the last-message pointer and $addToSet-merges
	// the supplied accounts (replier + parent author on the subsequent-reply path) into
	// replyAccounts in one write.
	UpdateThreadRoomLastMessage(ctx context.Context, threadRoomID, lastMsgID string, replyAccounts []string, lastMsgAt time.Time) error
	// AddReplyAccounts $addToSet-merges accounts into thread_rooms.replyAccounts.
	// Used by paths that don't already update lastMsg (first-reply parent author,
	// mention-only subscribers) so the field mirrors thread_subscriptions membership.
	AddReplyAccounts(ctx context.Context, threadRoomID string, accounts []string) error
}

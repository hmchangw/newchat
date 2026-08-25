package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/roommetacache"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store,ParentMessageFetcher

var (
	// errNotSubscribed: matched by identity (errors.Is); forbidden/not_subscribed survives to the client.
	errNotSubscribed = errcode.Forbidden("not subscribed", errcode.WithReason(errcode.MessageNotSubscribed))

	// errLargeRoomPostRestricted: returned when a sender without bypass privileges
	// (owner/admin/bot) posts to a room whose userCount exceeds the threshold.
	errLargeRoomPostRestricted = errcode.Forbidden(
		"posting is restricted to owners and admins in this room",
		errcode.WithReason(errcode.MessageLargeRoomPostRestricted))
)

type Store interface {
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetRoomMeta(ctx context.Context, roomID string) (roommetacache.Meta, error)
	// ThreadRoomExists reports whether a thread already exists for parentMessageID.
	// Read only when parent resolution has already failed, to tell "this thread is
	// resolvable from Mongo" from "nothing can resolve it".
	ThreadRoomExists(ctx context.Context, parentMessageID string) (bool, error)
}

// ParentMessageFetcher resolves a quoted parent message into a snapshot
// suitable for embedding on the new message's canonical event. Implementations
// should treat any failure (not found, RPC timeout, forbidden, etc.) as a
// reason to return an error — the handler soft-fails on every error and ships
// the message without the quote.
type ParentMessageFetcher interface {
	FetchQuotedParent(ctx context.Context, account, roomID, siteID, messageID string) (*cassandra.QuotedParentMessage, error)
}

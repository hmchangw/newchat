package main

import (
	"context"
	"errors"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/model/cassandra"
)

//go:generate mockgen -destination=mock_store_test.go -package=main . Store,ParentMessageFetcher

// errNotSubscribed is returned when the user is not subscribed to the room.
var errNotSubscribed = errors.New("not subscribed")

// codedError pairs a stable wire code with a user-safe message. Returned by
// validation paths that want the reply to carry a machine-readable code.
type codedError struct {
	Code    string
	Message string
}

func (e *codedError) Error() string { return e.Message }

// codeLargeRoomPostRestricted is the wire code emitted when a non-bypass
// sender hits the cap. Shared between the error sentinel and the slog
// "reason" field so log queries and the wire payload stay aligned.
const codeLargeRoomPostRestricted = "large_room_post_restricted"

// errLargeRoomPostRestricted is returned when a sender without bypass
// privileges (owner, admin, or bot account) attempts to post a top-level
// message in a room whose userCount exceeds the configured threshold.
var errLargeRoomPostRestricted = &codedError{
	Code:    codeLargeRoomPostRestricted,
	Message: "posting is restricted to owners and admins in this room",
}

type Store interface {
	GetSubscription(ctx context.Context, account, roomID string) (*model.Subscription, error)
	GetRoomUserCount(ctx context.Context, roomID string) (int, error)
}

// ParentMessageFetcher resolves a quoted parent message into a snapshot
// suitable for embedding on the new message's canonical event. Implementations
// should treat any failure (not found, RPC timeout, forbidden, etc.) as a
// reason to return an error — the handler soft-fails on every error and ships
// the message without the quote.
type ParentMessageFetcher interface {
	FetchQuotedParent(ctx context.Context, account, roomID, siteID, messageID string) (*cassandra.QuotedParentMessage, error)
}

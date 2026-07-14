package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsUserStore reads the externally-populated teams_user collection and
// advances per-user watermarks.
type TeamsUserStore interface {
	// ListUsers returns every teams_user projected to exactly the fields the
	// sync needs (_id, siteID, account, from).
	ListUsers(ctx context.Context) ([]model.TeamsUser, error)
	// SetFrom advances one user's watermark after that user fully succeeded.
	SetFrom(ctx context.Context, userID string, from time.Time) error
}

// TeamsChatStore upserts synced chats keyed on _id. oneOnOne chats are
// insert-only; for other chat types createdDateTime and siteID are
// $setOnInsert-only while the mutable fields are refreshed.
type TeamsChatStore interface {
	UpsertChats(ctx context.Context, chats []model.TeamsChat, now time.Time) error
}

// chatsFetcher is the Graph surface the sync consumes (interface defined in
// the consumer per repo convention; satisfied by msgraph.ChatsReader).
type chatsFetcher interface {
	ListUserChats(ctx context.Context, userID string, from, to time.Time) ([]msgraph.Chat, error)
}

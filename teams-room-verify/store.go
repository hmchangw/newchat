package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// VerifiedRef identifies a chat to clear plus the updatedAt it was read at.
// MarkVerified uses it as a compare-and-set token: a chat whose updatedAt
// changed since it was listed (teams-chat-member-sync re-resolved its roster)
// is left flagged and re-verified next run against its new member list.
type VerifiedRef struct {
	ID        string
	UpdatedAt time.Time
}

// TeamsChatStore reads chats awaiting verification and clears the flag once
// their site confirms the room and subscriptions exist. Satisfied by
// *mongoStore, whose reads and writes go to separate clients (secondary-read,
// primary-write).
type TeamsChatStore interface {
	// ListChatsNeedingVerify returns every teams_chat with needVerify=true,
	// projected to the fields verification needs (_id, members, siteId, updatedAt).
	ListChatsNeedingVerify(ctx context.Context) ([]model.TeamsChat, error)
	// MarkVerified clears needVerify for each ref whose updatedAt still matches.
	MarkVerified(ctx context.Context, refs []VerifiedRef) error
}

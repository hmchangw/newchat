package main

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// errSuperseded indicates the chat's updatedAt no longer matches the value
// observed when the chat was read for sync (teams-chat-sync rewrote the chat
// concurrently). The write is skipped and needMemberSync stays true, so the
// chat is retried next run against the fresher membership.
var errSuperseded = errors.New("teams_chat superseded by a concurrent update")

// ChatToSync is a chat needing member sync: its id plus the updatedAt
// observed at read time, used for the optimistic conditional write.
type ChatToSync struct {
	ID        string
	UpdatedAt time.Time
}

// TeamsChatStore reads chats needing member sync and writes back the resolved
// member list. Satisfied by *mongoStore. ListChatsToSync uses the read client;
// SetMembersSynced the write client.
type TeamsChatStore interface {
	// ListChatsToSync returns the id and updatedAt of every teams_chat with
	// needMemberSync=true.
	ListChatsToSync(ctx context.Context) ([]ChatToSync, error)
	// SetMembersSynced conditionally updates the chat only when its
	// updatedAt still equals seenUpdatedAt: $set {members,
	// needCreateRoom:true, needMemberSync:false, updatedAt:now}. Returns
	// errSuperseded (matchable via errors.Is) when the chat was rewritten
	// concurrently, leaving needMemberSync=true for retry.
	SetMembersSynced(ctx context.Context, chatID string, seenUpdatedAt time.Time, members []model.TeamsChatMember, now time.Time) error
}

// TeamsUserStore resolves userIds to accounts from teams_user (read client),
// for members whose Graph UPN was absent. Satisfied by *mongoStore.
type TeamsUserStore interface {
	// AccountsByIDs returns userId->account for the ids present in teams_user;
	// ids without a record are absent from the map.
	AccountsByIDs(ctx context.Context, ids []string) (map[string]string, error)
}

// membersFetcher is the Graph surface the sync consumes (interface defined in
// the consumer per repo convention; satisfied by msgraph.ChatMembersReader).
type membersFetcher interface {
	ListChatMembers(ctx context.Context, chatID string) ([]msgraph.ChatMemberDetail, error)
}

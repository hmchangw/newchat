package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsChatStore reads chats needing member sync and writes back the resolved
// member list. Satisfied by *mongoStore. ListChatsToSync uses the read client;
// SetMembersSynced the write client.
type TeamsChatStore interface {
	// ListChatsToSync returns the _id of every teams_chat with
	// needMemberSync=true.
	ListChatsToSync(ctx context.Context) ([]string, error)
	// SetMembersSynced replaces the chat's members and hands it to the next
	// stage: $set {members, needCreateRoom:true, needMemberSync:false,
	// updatedAt:now}.
	SetMembersSynced(ctx context.Context, chatID string, members []model.TeamsChatMember, now time.Time) error
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

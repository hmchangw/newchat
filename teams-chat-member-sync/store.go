package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/msgraph"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// ChatToSync is a chat needing member sync: its id plus the updatedAt
// observed at read time, used for the optimistic conditional write.
type ChatToSync struct {
	ID        string
	UpdatedAt time.Time
}

// ChatMembersUpdate is one chat's resolved member list ready for a batched
// conditional write: the id, the updatedAt observed at read time (the
// optimistic-concurrency token), and the members to store.
type ChatMembersUpdate struct {
	ChatID        string
	SeenUpdatedAt time.Time
	Members       []model.TeamsChatMember
}

// TeamsChatStore reads chats needing member sync and writes back the resolved
// member lists. Satisfied by *mongoStore. ListChatsToSync uses the read client;
// SetMembersSyncedBatch the write client.
type TeamsChatStore interface {
	// ListChatsToSync returns the id and updatedAt of every teams_chat with
	// needMemberSync=true.
	ListChatsToSync(ctx context.Context) ([]ChatToSync, error)
	// SetMembersSyncedBatch conditionally updates every chat in updates with
	// one unordered bulk write: each chat is updated only while its updatedAt
	// still equals its SeenUpdatedAt: $set {members, needCreateRoom:true,
	// needMemberSync:false, updatedAt:now}. It returns the number of chats
	// matched (updated); the remainder len(updates)-matched were rewritten
	// concurrently (superseded) and keep needMemberSync=true for retry next
	// run. A bulk write cannot attribute per-chat outcomes, so superseded
	// chats are reported as a count, not by id.
	SetMembersSyncedBatch(ctx context.Context, updates []ChatMembersUpdate, now time.Time) (int64, error)
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

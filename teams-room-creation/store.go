package main

import (
	"context"
	"time"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// RoomCreatedRef identifies a chat to clear plus the updatedAt it was read at.
// MarkRoomsCreated uses it as a compare-and-set token: a chat whose updatedAt
// changed since it was listed (teams-chat-member-sync re-resolved its roster and
// re-set needCreateRoom) is left flagged and re-published next run, so a
// concurrent membership update is never dropped.
type RoomCreatedRef struct {
	ID        string
	UpdatedAt time.Time
}

// TeamsChatStore reads chats flagged for room creation and clears the flag
// once their room-creation event is durably published. Satisfied by
// *mongoStore, whose reads and writes go to separate clients (secondary-read,
// primary-write).
type TeamsChatStore interface {
	// ListChatsNeedingRoom returns every teams_chat with needCreateRoom=true,
	// projected to the fields the event and the compare-and-set clear need
	// (_id, name, members, createdDateTime, siteId, updatedAt).
	ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error)
	// MarkRoomsCreated clears needCreateRoom for each ref whose updatedAt still
	// matches — a compare-and-set so a chat re-flagged by member-sync since it
	// was listed is left for the next run.
	MarkRoomsCreated(ctx context.Context, refs []RoomCreatedRef) error
}

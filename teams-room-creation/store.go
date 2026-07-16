package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

//go:generate mockgen -source=store.go -destination=mock_store_test.go -package=main

// TeamsChatStore reads chats flagged for room creation and clears the flag
// once their room-creation event is durably published. Satisfied by
// *mongoStore, whose reads and writes go to separate clients (secondary-read,
// primary-write).
type TeamsChatStore interface {
	// ListChatsNeedingRoom returns every teams_chat with needCreateRoom=true,
	// projected to the fields the event needs (_id, name, members,
	// createdDateTime, siteId).
	ListChatsNeedingRoom(ctx context.Context) ([]model.TeamsChat, error)
	// MarkRoomsCreated clears needCreateRoom for the given chat ids.
	MarkRoomsCreated(ctx context.Context, ids []string) error
}

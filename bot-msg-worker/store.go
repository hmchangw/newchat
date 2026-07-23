package main

import (
	"context"

	"github.com/hmchangw/chat/pkg/model"
)

// Store is the narrow Cassandra write surface bot-msg-worker calls.
type Store interface {
	// SaveMessage inserts into messages_by_room + messages_by_id; idempotent on the compound PK.
	SaveMessage(ctx context.Context, msg *model.Message, siteID string) error

	// SaveThreadMessage inserts into messages_by_id + thread_messages_by_thread, mirroring to messages_by_room when msg.TShow is true; idempotent on the compound PK.
	SaveThreadMessage(ctx context.Context, msg *model.Message, siteID, threadRoomID string) error
}

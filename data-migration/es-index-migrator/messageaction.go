package main

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/searchengine"
	"github.com/hmchangw/chat/pkg/searchindex"
)

// buildMessageAction builds the ES bulk action for one Cassandra message
// row. Always ActionIndex — a historical scan never deletes; deleted rows
// are filtered out before reaching this function (messagesource_cassandra.go).
//
//nolint:gocritic // hugeParam: msg is passed by value; struct copy is acceptable for bulk action construction
func buildMessageAction(msg cassandra.Message, indexPrefix string) (searchengine.BulkAction, error) {
	if msg.MessageID == "" {
		return searchengine.BulkAction{}, fmt.Errorf("build message action: empty message id for room %s", msg.RoomID)
	}
	if msg.CreatedAt.IsZero() {
		return searchengine.BulkAction{}, fmt.Errorf("build message action: zero createdAt for message %s", msg.MessageID)
	}

	doc, docErr := searchindex.NewMessageDoc(searchindex.MessageFields{
		MessageID:             msg.MessageID,
		RoomID:                msg.RoomID,
		SiteID:                msg.SiteID,
		UserID:                msg.Sender.ID,
		UserAccount:           msg.Sender.Account,
		Content:               msg.Msg,
		CreatedAt:             msg.CreatedAt,
		EditedAt:              msg.EditedAt,
		UpdatedAt:             msg.UpdatedAt,
		ThreadParentID:        msg.ThreadParentID,
		ThreadParentCreatedAt: msg.ThreadParentCreatedAt,
		TShow:                 msg.TShow,
		Attachments:           msg.Attachments,
		Card:                  msg.Card,
	})
	if docErr != nil {
		// Not fatal — the doc is still fully usable, just missing the skipped
		// attachment(s). Logged so silent data loss across a bulk backfill is observable.
		slog.Warn("message doc built with skipped attachments", "error", docErr,
			"messageId", msg.MessageID, "roomId", msg.RoomID)
	}

	body, err := json.Marshal(doc)
	if err != nil {
		return searchengine.BulkAction{}, fmt.Errorf("marshal message doc for %s: %w", msg.MessageID, err)
	}

	return searchengine.BulkAction{
		Action:  searchengine.ActionIndex,
		Index:   searchindex.MessageIndexName(indexPrefix, msg.CreatedAt),
		DocID:   msg.MessageID,
		Version: msg.CreatedAt.UnixMilli(),
		Doc:     body,
	}, nil
}

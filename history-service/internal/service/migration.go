package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// MigrationEditMessage applies a migrated content edit and republishes the canonical .updated event
// (X-Migration: live). Server-internal/account-less: the transformer supplies the Cassandra locator,
// so no access gate runs. Idempotent — per-table UPDATEs are keyed and dedup is enforced downstream.
//
// Edit-before-insert race: INSERTs migrate asynchronously while edits arrive synchronously. An edit
// reaching us first finds no row (ErrMessageNotFound) → mapped to NotFound (4xx) so the transformer Naks/retries, not 5xx.
func (s *HistoryService) MigrationEditMessage(c *natsrouter.Context, siteID string, req model.MigrationEditRequest) (*model.MigrationAck, error) { //nolint:gocritic // hugeParam: req is passed by value to satisfy the natsrouter.Register handler signature
	c.WithLogValues("room_id", req.RoomID, "message_id", req.MessageID)

	locator := &models.Message{
		MessageID: req.MessageID,
		RoomID:    req.RoomID,
		CreatedAt: req.CreatedAt,
	}
	if err := s.msgWriter.UpdateMessageContent(c, locator, req.Content, req.EditedAt); err != nil {
		// Edit-before-insert race: the row isn't persisted yet, so the UPDATE matched nothing.
		// Map to NotFound (4xx, retryable Nak) so this benign race doesn't log as internal/5xx.
		if errors.Is(err, cassrepo.ErrMessageNotFound) {
			return nil, errcode.NotFound("message not yet persisted, retry")
		}
		return nil, fmt.Errorf("migration edit message %s: %w", req.MessageID, err)
	}

	editedAt := req.EditedAt
	evt := model.MessageEvent{
		Event: model.EventUpdated,
		Message: model.Message{
			ID:        req.MessageID,
			RoomID:    req.RoomID,
			CreatedAt: req.CreatedAt,
			Content:   req.Content,
			EditedAt:  &editedAt,
			UpdatedAt: &editedAt,
		},
		SiteID: siteID,
		// Event-level Timestamp is publish-time, not the source editedAt (the domain timestamp inside Message).
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	s.publishMigrationBestEffort(c, subject.MsgCanonicalUpdated(siteID), &evt)

	return &model.MigrationAck{OK: true}, nil
}

// MigrationDeleteMessage applies a migrated delete and republishes the canonical .deleted event
// (X-Migration: live). Serves both hard deletes and soft deletes; the request carries only the id,
// and the row is resolved via GetMessageByID (messages_by_id PK is (message_id, created_at)).
//
// Delete-before-insert race: SoftDeleteMessage CAS-updates an existing row and never creates one, so
// a delete reaching us before the async insert would silently lose. We gate on existence first:
// absent ⇒ retryable error (Nak); already-deleted ⇒ idempotent {OK:true}; present-not-deleted ⇒ delete+publish.
func (s *HistoryService) MigrationDeleteMessage(c *natsrouter.Context, siteID string, req model.MigrationDeleteRequest) (*model.MigrationAck, error) { //nolint:gocritic // hugeParam: req is passed by value to satisfy the natsrouter.Register handler signature
	c.WithLogValues("message_id", req.MessageID)

	// Resolve the full row by id alone — messages_by_id PK is (message_id, created_at), so the lookup
	// recovers roomId + createdAt without the caller supplying them.
	msg, err := s.msgReader.GetMessageByID(c, req.MessageID)
	if err != nil {
		return nil, fmt.Errorf("migration delete %q: %w", req.MessageID, err)
	}
	if msg == nil {
		// Insert not yet persisted (or never migrated) — surface a retryable error
		// so the transformer Naks/retries until message-worker persists the insert.
		return nil, errcode.NotFound("message not yet persisted, retry")
	}

	// Already soft-deleted on an earlier delivery — ack without re-applying.
	if msg.Deleted {
		return &model.MigrationAck{OK: true}, nil
	}

	if _, _, _, err := s.msgWriter.SoftDeleteMessage(c, msg, req.DeletedAt); err != nil {
		return nil, fmt.Errorf("migration delete message %s: %w", req.MessageID, err)
	}

	deletedAt := req.DeletedAt
	evt := model.MessageEvent{
		Event: model.EventDeleted,
		Message: model.Message{
			ID:        msg.MessageID,
			RoomID:    msg.RoomID,
			CreatedAt: msg.CreatedAt,
			UpdatedAt: &deletedAt,
		},
		SiteID: siteID,
		// Event-level Timestamp is publish-time, not the source deletedAt (the domain timestamp inside Message).
		Timestamp: time.Now().UTC().UnixMilli(),
	}
	s.publishMigrationBestEffort(c, subject.MsgCanonicalDeleted(siteID), &evt)

	return &model.MigrationAck{OK: true}, nil
}

// publishMigrationBestEffort publishes a canonical event with the X-Migration header; failures are
// logged and swallowed (Cassandra is the source of truth).
func (s *HistoryService) publishMigrationBestEffort(c *natsrouter.Context, subj string, evt *model.MessageEvent) {
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Warn("migration canonical marshal failed",
			"error", err, "subject", subj, "messageID", evt.Message.ID, "room_id", evt.Message.RoomID,
			"request_id", natsutil.RequestIDFromContext(c))
		return
	}
	if err := s.publisher.PublishMigration(c, subj, payload, natsutil.CanonicalDedupID(evt)); err != nil {
		slog.Warn("migration canonical publish failed",
			"error", err, "subject", subj, "messageID", evt.Message.ID, "room_id", evt.Message.RoomID,
			"request_id", natsutil.RequestIDFromContext(c))
	}
}

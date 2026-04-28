package service

import (
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	defaultPageSize     = 20
	surroundingPageSize = 50
	maxPageSize         = 100
	maxContentBytes     = 20 * 1024 // 20 KB; mirrors message-gatekeeper's content cap
)

func (s *HistoryService) LoadHistory(c *natsrouter.Context, req models.LoadHistoryRequest) (*models.LoadHistoryResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	before := millisToTime(req.Before)
	if before.IsZero() {
		before = time.Now().UTC()
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	pageReq, err := parsePageRequest("", limit)
	if err != nil {
		return nil, err
	}

	var page cassrepo.Page[models.Message]
	if accessSince == nil {
		page, err = s.messages.GetMessagesBefore(c, roomID, before, pageReq)
	} else {
		page, err = s.messages.GetMessagesBetweenDesc(c, roomID, *accessSince, before, pageReq)
	}
	if err != nil {
		slog.Error("loading history", "error", err, "roomID", roomID)
		return nil, natsrouter.ErrInternal("failed to load message history")
	}

	return &models.LoadHistoryResponse{Messages: page.Data}, nil
}

func (s *HistoryService) LoadNextMessages(c *natsrouter.Context, req models.LoadNextMessagesRequest) (*models.LoadNextMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	after := millisToTime(req.After)

	// Lower bound = max(after, accessSince). Zero means no lower bound.
	lowerBound := timeMax(after, derefTime(accessSince))

	limit := req.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	pageReq, err := parsePageRequest(req.Cursor, limit)
	if err != nil {
		return nil, err
	}

	var page cassrepo.Page[models.Message]
	if lowerBound.IsZero() {
		page, err = s.messages.GetAllMessagesAsc(c, roomID, pageReq)
	} else {
		page, err = s.messages.GetMessagesAfter(c, roomID, lowerBound, pageReq)
	}
	if err != nil {
		slog.Error("loading next messages", "error", err, "roomID", roomID)
		return nil, natsrouter.ErrInternal("failed to load messages")
	}

	return &models.LoadNextMessagesResponse{
		Messages:   page.Data,
		NextCursor: page.NextCursor,
		HasNext:    page.HasNext,
	}, nil
}

func (s *HistoryService) LoadSurroundingMessages(c *natsrouter.Context, req models.LoadSurroundingMessagesRequest) (*models.LoadSurroundingMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	centralMsg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}
	if accessSince != nil && centralMsg.CreatedAt.Before(*accessSince) {
		return nil, natsrouter.ErrForbidden("message is outside access window")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = surroundingPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	// Split limit-1 (excluding central message) across before and after.
	// before gets the larger half on odd splits.
	remaining := limit - 1
	if remaining <= 0 {
		return &models.LoadSurroundingMessagesResponse{
			Messages: []models.Message{*centralMsg},
		}, nil
	}
	beforeCount := (remaining + 1) / 2
	afterCount := remaining / 2

	beforePageReq, err := parsePageRequest("", beforeCount)
	if err != nil {
		return nil, err
	}
	afterPageReq, err := parsePageRequest("", afterCount)
	if err != nil {
		return nil, err
	}

	// Before-page: messages older than central, newest-first.
	var beforePage cassrepo.Page[models.Message]
	if accessSince == nil {
		beforePage, err = s.messages.GetMessagesBefore(c, roomID, centralMsg.CreatedAt, beforePageReq)
	} else {
		beforePage, err = s.messages.GetMessagesBetweenDesc(c, roomID, *accessSince, centralMsg.CreatedAt, beforePageReq)
	}
	if err != nil {
		slog.Error("loading surrounding messages", "error", err, "roomID", roomID, "direction", "before")
		return nil, natsrouter.ErrInternal("failed to load surrounding messages")
	}

	// After-page: messages newer than central, oldest-first.
	afterPage, err := s.messages.GetMessagesAfter(c, roomID, centralMsg.CreatedAt, afterPageReq)
	if err != nil {
		slog.Error("loading surrounding messages", "error", err, "roomID", roomID, "direction", "after")
		return nil, natsrouter.ErrInternal("failed to load surrounding messages")
	}

	// Assemble: reverse before-page (DESC→ASC) + central + after-page (already ASC).
	messages := make([]models.Message, 0, len(beforePage.Data)+1+len(afterPage.Data))
	for i := len(beforePage.Data) - 1; i >= 0; i-- {
		messages = append(messages, beforePage.Data[i])
	}
	messages = append(messages, *centralMsg)
	messages = append(messages, afterPage.Data...)

	return &models.LoadSurroundingMessagesResponse{
		Messages:   messages,
		MoreBefore: beforePage.HasNext,
		MoreAfter:  afterPage.HasNext,
	}, nil
}

func (s *HistoryService) GetMessageByID(c *natsrouter.Context, req models.GetMessageByIDRequest) (*models.Message, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	if accessSince != nil && msg.CreatedAt.Before(*accessSince) {
		return nil, natsrouter.ErrForbidden("message is outside access window")
	}

	return msg, nil
}

// EditMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit.
// Sender-only auth. Writes to all applicable Cassandra tables via
// UpdateMessageContent, then publishes a best-effort MessageEditedEvent to
// chat.room.{roomID}.event for live fan-out.
func (s *HistoryService) EditMessage(c *natsrouter.Context, req models.EditMessageRequest) (*models.EditMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	// 1. Subscription gate — non-subscribers cannot probe messageID -> roomID mappings.
	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	// 2. Hydrate. findMessage returns ErrNotFound for missing IDs and for
	// messages that belong to a different room (same error, no leak).
	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// A soft-deleted message must not be editable — that would emit a
	// message_edited event after message_deleted, which downstream consumers
	// can't reconcile. Same ErrNotFound as wrong-room to keep the leak
	// surface symmetric.
	if msg.Deleted {
		return nil, natsrouter.ErrNotFound("message not found")
	}

	// 3. Sender gate.
	if !canModify(msg, account) {
		return nil, natsrouter.ErrForbidden("only the sender can edit")
	}

	// 4. Content validation.
	if strings.TrimSpace(req.NewMsg) == "" {
		return nil, natsrouter.ErrBadRequest("newMsg must not be empty")
	}
	if len(req.NewMsg) > maxContentBytes {
		return nil, natsrouter.ErrBadRequest("newMsg exceeds maximum size")
	}

	// 5. Persist.
	editedAt := time.Now().UTC()
	if err := s.messages.UpdateMessageContent(c, msg, req.NewMsg, editedAt); err != nil {
		slog.Error("edit: update content", "error", err, "messageID", req.MessageID)
		return nil, natsrouter.ErrInternal("failed to edit message")
	}

	// 6. Publish live event (best-effort — publish failure is logged, not returned).
	editedAtMs := editedAt.UnixMilli()
	evt := models.MessageEditedEvent{
		Type:      "message_edited",
		Timestamp: editedAtMs,
		RoomID:    roomID,
		MessageID: req.MessageID,
		NewMsg:    req.NewMsg,
		EditedBy:  account,
		EditedAt:  editedAtMs,
	}
	if payload, err := json.Marshal(evt); err == nil {
		if pubErr := s.publisher.Publish(c, subject.RoomEvent(roomID), payload); pubErr != nil {
			slog.Warn("edit: publish event failed", "error", pubErr, "messageID", req.MessageID)
		}
	} else {
		slog.Warn("edit: marshal event failed", "error", err, "messageID", req.MessageID)
	}

	return &models.EditMessageResponse{
		MessageID: req.MessageID,
		EditedAt:  editedAtMs,
	}, nil
}

// DeleteMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete.
// Sender-only auth. Soft-deletes (deleted = true, updated_at = ?) across all
// applicable Cassandra tables via SoftDeleteMessage, including tcount
// decrement on the parent for thread replies. On already-deleted messages the
// handler short-circuits and returns success without repeating the UPDATEs or
// publishing a duplicate event — this prevents tcount drift on caller retry.
func (s *HistoryService) DeleteMessage(c *natsrouter.Context, req models.DeleteMessageRequest) (*models.DeleteMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	// 1. Subscription gate.
	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	// 2. Hydrate. findMessage does the roomID-match check and ErrNotFound handling.
	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// 3. Sender gate.
	if !canModify(msg, account) {
		return nil, natsrouter.ErrForbidden("only the sender can delete")
	}

	// 4. Already-deleted short-circuit. Echo the current updated_at as the
	// DeletedAt. Prevents tcount double-decrement on caller retry and avoids
	// duplicate message_deleted events.
	if msg.Deleted {
		var deletedAtMs int64
		if msg.UpdatedAt != nil {
			deletedAtMs = msg.UpdatedAt.UnixMilli()
		}
		return &models.DeleteMessageResponse{
			MessageID: req.MessageID,
			DeletedAt: deletedAtMs,
		}, nil
	}

	// 5. Persist via LWT. The repo's CAS gates the mirror-table UPDATEs and
	// parent-tcount decrement so two concurrent deletes can't double-decrement
	// the parent.
	deletedAt := time.Now().UTC()
	actualDeletedAt, applied, err := s.messages.SoftDeleteMessage(c, msg, deletedAt)
	if err != nil {
		slog.Error("delete: soft-delete", "error", err, "messageID", req.MessageID)
		return nil, natsrouter.ErrInternal("failed to delete message")
	}
	if !applied {
		// A concurrent delete won the CAS. Skip the publish — the winning
		// goroutine has emitted (or will emit) the message_deleted event —
		// and return the timestamp actually persisted.
		return &models.DeleteMessageResponse{
			MessageID: req.MessageID,
			DeletedAt: actualDeletedAt.UnixMilli(),
		}, nil
	}

	// 6. Publish live event (best-effort).
	deletedAtMs := actualDeletedAt.UnixMilli()
	evt := models.MessageDeletedEvent{
		Type:      "message_deleted",
		Timestamp: deletedAtMs,
		RoomID:    roomID,
		MessageID: req.MessageID,
		DeletedBy: account,
		DeletedAt: deletedAtMs,
	}
	if payload, err := json.Marshal(evt); err == nil {
		if pubErr := s.publisher.Publish(c, subject.RoomEvent(roomID), payload); pubErr != nil {
			slog.Warn("delete: publish event failed", "error", pubErr, "messageID", req.MessageID)
		}
	} else {
		slog.Warn("delete: marshal event failed", "error", err, "messageID", req.MessageID)
	}

	return &models.DeleteMessageResponse{
		MessageID: req.MessageID,
		DeletedAt: deletedAtMs,
	}, nil
}

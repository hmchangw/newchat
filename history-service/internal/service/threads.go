package service

import (
	"fmt"
	"log/slog"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// NATS: chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread
func (s *HistoryService) GetThreadMessages(c *natsrouter.Context, req models.GetThreadMessagesRequest) (*models.GetThreadMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	if req.ThreadMessageID == "" {
		return nil, natsrouter.ErrBadRequest("threadMessageId is required")
	}

	// Access check before fetch — prevents probing message IDs without room membership.
	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.ThreadMessageID)
	if err != nil {
		return nil, err
	}

	if msg.ThreadParentID != "" {
		return nil, natsrouter.ErrBadRequest("threadMessageId must be a top-level message, not a reply")
	}

	if accessSince != nil && msg.CreatedAt.Before(*accessSince) {
		return nil, natsrouter.ErrForbidden("thread is outside access window")
	}

	// Empty ThreadRoomID: no replies yet, or stamp skipped due to missing event fields.
	if msg.ThreadRoomID == "" {
		return &models.GetThreadMessagesResponse{Messages: []models.Message{}, HasNext: false}, nil
	}

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

	page, err := s.msgReader.GetThreadMessages(c, roomID, msg.ThreadRoomID, pageReq)
	if err != nil {
		slog.Error("loading thread messages", "error", err, "roomID", roomID, "threadRoomID", msg.ThreadRoomID)
		return nil, natsrouter.ErrInternal("failed to load thread messages")
	}

	redactUnavailableQuotes(page.Data, accessSince)
	return &models.GetThreadMessagesResponse{
		Messages:   page.Data,
		NextCursor: page.NextCursor,
		HasNext:    page.HasNext,
	}, nil
}

// Empty filter defaults to "all" so clients can omit the field.
func validateThreadFilter(filter models.ThreadFilter) (models.ThreadFilter, error) {
	switch filter {
	case "", models.ThreadFilterAll:
		return models.ThreadFilterAll, nil
	case models.ThreadFilterFollowing, models.ThreadFilterUnread:
		return filter, nil
	default:
		return "", natsrouter.ErrBadRequest(fmt.Sprintf("invalid thread filter: %q", filter))
	}
}

// NATS: chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent
func (s *HistoryService) GetThreadParentMessages(c *natsrouter.Context, req models.GetThreadParentMessagesRequest) (*models.GetThreadParentMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	filter, err := validateThreadFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	pageReq := mongorepo.NewOffsetPageRequest(req.Offset, req.Limit)

	var threadPage mongorepo.OffsetPage[pkgmodel.ThreadRoom]
	switch filter {
	case models.ThreadFilterAll:
		threadPage, err = s.threadRooms.GetThreadRooms(c, roomID, accessSince, pageReq)
	case models.ThreadFilterFollowing:
		threadPage, err = s.threadRooms.GetFollowingThreadRooms(c, roomID, account, accessSince, pageReq)
	case models.ThreadFilterUnread:
		threadPage, err = s.threadRooms.GetUnreadThreadRooms(c, roomID, account, accessSince, pageReq)
	default:
		slog.Error("unhandled thread filter", "filter", filter)
		return nil, natsrouter.ErrInternal("unhandled thread filter")
	}
	if err != nil {
		slog.Error("loading thread rooms from MongoDB", "error", err, "roomID", roomID, "filter", filter)
		return nil, natsrouter.ErrInternal("failed to load thread parent messages")
	}

	if len(threadPage.Data) == 0 {
		return &models.GetThreadParentMessagesResponse{ParentMessages: []models.Message{}, Total: threadPage.Total}, nil
	}

	seenIDs := make(map[string]struct{}, len(threadPage.Data))
	parentIDs := make([]string, 0, len(threadPage.Data))
	for i := range threadPage.Data {
		id := threadPage.Data[i].ParentMessageID
		if _, dup := seenIDs[id]; dup {
			continue
		}
		seenIDs[id] = struct{}{}
		parentIDs = append(parentIDs, id)
	}

	cassMessages, err := s.msgReader.GetMessagesByIDs(c, parentIDs)
	if err != nil {
		slog.Error("hydrating thread parent messages from Cassandra", "error", err, "roomID", roomID)
		return nil, natsrouter.ErrInternal("failed to load thread parent messages")
	}

	msgByID := make(map[string]models.Message, len(cassMessages))
	for i := range cassMessages {
		msgByID[cassMessages[i].MessageID] = cassMessages[i]
	}

	// Iterate parentIDs (deduplicated, MongoDB sort order preserved) rather than
	// threadPage.Data to avoid emitting the same parent twice when MongoDB returns
	// duplicate thread rooms for one parent. accessSince re-checked here:
	// MongoDB's threadParentCreatedAt can be zero when absent from the original event.
	parentMessages := make([]models.Message, 0, len(parentIDs))
	for _, id := range parentIDs {
		msg, ok := msgByID[id]
		if !ok {
			continue
		}
		if msg.RoomID != roomID {
			slog.Warn("thread parent message belongs to unexpected room", "messageID", id, "gotRoom", msg.RoomID, "wantRoom", roomID)
			continue
		}
		if accessSince != nil && msg.CreatedAt.Before(*accessSince) {
			continue
		}
		parentMessages = append(parentMessages, msg)
	}

	redactUnavailableQuotes(parentMessages, accessSince)
	return &models.GetThreadParentMessagesResponse{ParentMessages: parentMessages, Total: threadPage.Total}, nil
}

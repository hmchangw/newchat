package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
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
	c.WithLogValues("account", account, "room_id", roomID)
	now := time.Now().UTC()

	// The access check and room-times resolve are independent Mongo reads; run
	// them concurrently so the worst-case latency is one RTT, not two. Access
	// errors take precedence so a "not subscribed" 403 isn't masked by a
	// transient room-times error.
	accessSince, lastMsgAt, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	before := millisToTime(req.Before)
	if before.IsZero() {
		before = now
	}
	// Cap before at lastMsgAt+1ms so year-dead rooms become 1-bucket reads instead of walking from now.
	if !lastMsgAt.IsZero() && before.After(lastMsgAt) {
		before = lastMsgAt.Add(time.Millisecond)
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

	// Issue both the message-page read and the MinUserLastSeenAt read in parallel; receipt failures are non-fatal.
	var (
		page          cassrepo.Page[models.Message]
		lastSeenFloor *time.Time
	)
	g, gctx := errgroup.WithContext(c)
	g.Go(func() error {
		var pErr error
		if accessSince == nil {
			// Clamp createdAt to historyFloor so a client hint can't push the walk further back than configured.
			historyFloor := now.Add(-s.historyFloor)
			walkFloor := createdAt
			if walkFloor.IsZero() || walkFloor.Before(historyFloor) {
				walkFloor = historyFloor
			}
			page, pErr = s.msgReader.GetMessagesBefore(gctx, roomID, before, walkFloor, pageReq)
		} else {
			page, pErr = s.msgReader.GetMessagesBetweenDesc(gctx, roomID, *accessSince, before, pageReq)
		}
		return pErr
	})
	g.Go(s.readFloorInto(gctx, roomID, &lastSeenFloor))
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("loading history: %w", err)
	}

	minMs := millisPtr(lastSeenFloor)

	redactUnavailableQuotes(page.Data, accessSince)
	setDecodedAttachments(c, page.Data)
	return &models.LoadHistoryResponse{
		Messages:          page.Data,
		MinUserLastSeenAt: minMs,
	}, nil
}

func (s *HistoryService) LoadNextMessages(c *natsrouter.Context, req models.LoadNextMessagesRequest) (*models.LoadNextMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)
	now := time.Now().UTC()

	accessSince, lastMsgAt, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)

	after := millisToTime(req.After)

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

	// Page read + MinUserLastSeenAt read in parallel; the receipt read is non-fatal.
	var (
		page          cassrepo.Page[models.Message]
		lastSeenFloor *time.Time
	)
	g, gctx := errgroup.WithContext(c)
	g.Go(func() error {
		var pErr error
		if lowerBound.IsZero() {
			page, pErr = s.msgReader.GetAllMessagesAsc(gctx, roomID, floor, ceiling, pageReq)
		} else {
			page, pErr = s.msgReader.GetMessagesAfter(gctx, roomID, lowerBound, ceiling, pageReq)
		}
		return pErr
	})
	g.Go(s.readFloorInto(gctx, roomID, &lastSeenFloor))
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("loading next messages: %w", err)
	}

	minMs := millisPtr(lastSeenFloor)

	redactUnavailableQuotes(page.Data, accessSince)
	setDecodedAttachments(c, page.Data)
	return &models.LoadNextMessagesResponse{
		Messages:          page.Data,
		NextCursor:        page.NextCursor,
		HasNext:           page.HasNext,
		MinUserLastSeenAt: minMs,
	}, nil
}

func (s *HistoryService) LoadSurroundingMessages(c *natsrouter.Context, req models.LoadSurroundingMessagesRequest) (*models.LoadSurroundingMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	centralMsg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}
	if accessSince != nil && centralMsg.CreatedAt.Before(*accessSince) {
		return nil, errcode.Forbidden("message is outside access window", errcode.WithReason(errcode.MessageOutsideAccessWindow))
	}

	now := time.Now().UTC()
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(c, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)

	limit := req.Limit
	if limit <= 0 {
		limit = surroundingPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	remaining := limit - 1 // before gets the larger half on odd splits
	if remaining <= 0 {
		only := *centralMsg
		redactUnavailableQuote(&only, accessSince)
		decodeMessageAttachments(c, &only)
		// Serial best-effort read — this path issues no page reads to parallelise against.
		return &models.LoadSurroundingMessagesResponse{
			Messages:          []models.Message{only},
			MinUserLastSeenAt: s.minUserLastSeenMillis(c, roomID),
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

	var (
		beforePage    cassrepo.Page[models.Message]
		afterPage     cassrepo.Page[models.Message]
		lastSeenFloor *time.Time
	)
	g, gctx := errgroup.WithContext(c)
	g.Go(func() error {
		var berr error
		if accessSince == nil {
			beforePage, berr = s.msgReader.GetMessagesBefore(gctx, roomID, centralMsg.CreatedAt, floor, beforePageReq)
		} else {
			beforePage, berr = s.msgReader.GetMessagesBetweenDesc(gctx, roomID, *accessSince, centralMsg.CreatedAt, beforePageReq)
		}
		if berr != nil {
			return fmt.Errorf("loading surrounding messages (before): %w", berr)
		}
		return nil
	})
	g.Go(func() error {
		var aerr error
		afterPage, aerr = s.msgReader.GetMessagesAfter(gctx, roomID, centralMsg.CreatedAt, ceiling, afterPageReq)
		if aerr != nil {
			return fmt.Errorf("loading surrounding messages (after): %w", aerr)
		}
		return nil
	})
	g.Go(s.readFloorInto(gctx, roomID, &lastSeenFloor))
	if err := g.Wait(); err != nil {
		// errgroup error already carries the (before|after) direction.
		return nil, err
	}

	minMs := millisPtr(lastSeenFloor)

	// Assemble in ASC order: reverse the DESC before-page, append central, then after-page.
	messages := make([]models.Message, 0, len(beforePage.Data)+1+len(afterPage.Data))
	for i := len(beforePage.Data) - 1; i >= 0; i-- {
		messages = append(messages, beforePage.Data[i])
	}
	messages = append(messages, *centralMsg)
	messages = append(messages, afterPage.Data...)

	redactUnavailableQuotes(messages, accessSince)
	setDecodedAttachments(c, messages)
	return &models.LoadSurroundingMessagesResponse{
		Messages:          messages,
		MoreBefore:        beforePage.HasNext,
		MoreAfter:         afterPage.HasNext,
		MinUserLastSeenAt: minMs,
	}, nil
}

// millisPtr converts a read-floor time to UTC millis; nil in → nil out.
func millisPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	ms := t.UTC().UnixMilli()
	return &ms
}

// readFloorInto returns an errgroup task that best-effort loads the room read-floor
// into *dst. A read error logs and leaves *dst nil — messages still return.
func (s *HistoryService) readFloorInto(ctx context.Context, roomID string, dst **time.Time) func() error {
	return func() error {
		t, err := s.rooms.GetMinUserLastSeenAt(ctx, roomID)
		if err != nil {
			slog.Warn("loading minUserLastSeenAt", "error", err, "room_id", roomID)
			return nil
		}
		*dst = t
		return nil
	}
}

// minUserLastSeenMillis reads the room read-floor as UTC millis; best-effort — a read error logs and yields nil.
// Serial counterpart to readFloorInto for paths with no page read to parallelise against.
func (s *HistoryService) minUserLastSeenMillis(ctx context.Context, roomID string) *int64 {
	var t *time.Time
	_ = s.readFloorInto(ctx, roomID, &t)()
	return millisPtr(t)
}

func (s *HistoryService) GetMessageByID(c *natsrouter.Context, req models.GetMessageByIDRequest) (*models.Message, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	if accessSince != nil && msg.CreatedAt.Before(*accessSince) {
		return nil, errcode.Forbidden("message is outside access window", errcode.WithReason(errcode.MessageOutsideAccessWindow))
	}

	redactUnavailableQuote(msg, accessSince)
	decodeMessageAttachments(c, msg)
	return msg, nil
}

// maxGetByIDsBatchSize caps the number of IDs per msg.get.ids request.
const maxGetByIDsBatchSize = 100

// GetMessagesByIDs handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.get.ids.
// Returns messages in input order; IDs not found or outside the access window are silently omitted.
func (s *HistoryService) GetMessagesByIDs(c *natsrouter.Context, req models.GetMessagesByIDsRequest) (*models.GetMessagesByIDsResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	if len(req.MessageIDs) == 0 {
		return nil, errcode.BadRequest("messageIds must not be empty")
	}
	if len(req.MessageIDs) > maxGetByIDsBatchSize {
		return nil, errcode.BadRequest("too many messageIds")
	}

	fetched, err := s.msgReader.GetMessagesByIDs(c, req.MessageIDs)
	if err != nil {
		return nil, fmt.Errorf("fetching messages by IDs: %w", err)
	}

	kept := fetched[:0]
	for i := range fetched {
		// Scope to the subject's room — fetch is by ID alone, so drop any cross-room match.
		if fetched[i].RoomID != roomID {
			continue
		}
		if accessSince != nil && fetched[i].CreatedAt.Before(*accessSince) {
			continue
		}
		kept = append(kept, fetched[i])
	}

	redactUnavailableQuotes(kept, accessSince)
	setDecodedAttachments(c, kept)
	return &models.GetMessagesByIDsResponse{Messages: kept}, nil
}

// EditMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.edit.
// Cassandra is the source of truth; canonical publish failures are logged and swallowed.
func (s *HistoryService) EditMessage(c *natsrouter.Context, siteID string, req models.EditMessageRequest) (*models.EditMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	// Editing a soft-deleted message would emit updated after deleted, which consumers can't reconcile.
	if msg.Deleted {
		return nil, errcode.NotFound("message not found")
	}

	if !canModify(msg, account) {
		return nil, errcode.Forbidden("only the sender can edit")
	}

	if strings.TrimSpace(req.NewMsg) == "" {
		return nil, errcode.BadRequest("newMsg must not be empty")
	}
	if len(req.NewMsg) > maxContentBytes {
		return nil, errcode.BadRequest("newMsg exceeds maximum size")
	}

	editedAt := time.Now().UTC()
	if err := s.msgWriter.UpdateMessageContent(c, msg, req.NewMsg, editedAt); err != nil {
		// A TOCTOU between findMessage and the CAS edit (or a concurrent
		// hard-delete / soft-delete) surfaces as ErrMessageNotFound from
		// the repo. Map it to 4xx so it doesn't pollute 5xx telemetry —
		// it's a benign race, not a server fault.
		if errors.Is(err, cassrepo.ErrMessageNotFound) {
			return nil, errcode.NotFound("message not found")
		}
		return nil, fmt.Errorf("editing message %s: %w", req.MessageID, err)
	}

	editedAtMs := editedAt.UnixMilli()

	// search-sync-worker reindexes the FULL doc, so attachments/card must ride
	// along or edits wipe them. Mentions omitted: broadcast-worker re-resolves.
	canonicalEvt := model.MessageEvent{
		Event: model.EventUpdated,
		Message: model.Message{
			ID:                           msg.MessageID,
			RoomID:                       msg.RoomID,
			UserID:                       msg.Sender.ID,
			UserAccount:                  msg.Sender.Account,
			Content:                      req.NewMsg,
			Attachments:                  msg.Attachments,
			Card:                         msg.Card,
			CreatedAt:                    msg.CreatedAt,
			EditedAt:                     &editedAt,
			UpdatedAt:                    &editedAt,
			ThreadParentMessageID:        msg.ThreadParentID,
			ThreadParentMessageCreatedAt: msg.ThreadParentCreatedAt,
			TShow:                        msg.TShow,
		},
		SiteID:    siteID,
		Timestamp: editedAtMs,
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalUpdated(siteID), &canonicalEvt)

	return &models.EditMessageResponse{
		MessageID: req.MessageID,
		EditedAt:  editedAtMs,
	}, nil
}

// DeleteMessage handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.delete.
// Already-deleted messages short-circuit to prevent tcount drift and duplicate canonical events on retry.
func (s *HistoryService) DeleteMessage(c *natsrouter.Context, siteID string, req models.DeleteMessageRequest) (*models.DeleteMessageResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	if _, err := s.getAccessSince(c, account, roomID); err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.MessageID)
	if err != nil {
		return nil, err
	}

	if !canModify(msg, account) {
		return nil, errcode.Forbidden("only the sender can delete")
	}

	// Already-deleted short-circuit: echo the current updated_at as the DeletedAt.
	// Prevents tcount double-decrement on caller retry and avoids duplicate events.
	// countAndSetParentTcount already wrote the correct tcount on the first delete,
	// so no re-publish is needed — the tcount is durable in Cassandra.
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

	deletedAt := time.Now().UTC()
	actualDeletedAt, applied, newTcount, newThreadLastMsgAt, err := s.msgWriter.SoftDeleteMessage(c, msg, deletedAt)
	if err != nil {
		return nil, fmt.Errorf("deleting message %s: %w", req.MessageID, err)
	}
	if !applied {
		// Concurrent delete won the CAS — skip publish to avoid a duplicate event.
		return &models.DeleteMessageResponse{
			MessageID: req.MessageID,
			DeletedAt: actualDeletedAt.UnixMilli(),
		}, nil
	}

	deletedAtMs := actualDeletedAt.UnixMilli()

	canonicalEvt := model.MessageEvent{
		Event: model.EventDeleted,
		Message: model.Message{
			ID:                           msg.MessageID,
			RoomID:                       msg.RoomID,
			UserID:                       msg.Sender.ID,
			UserAccount:                  msg.Sender.Account,
			Content:                      msg.Msg,
			CreatedAt:                    msg.CreatedAt,
			UpdatedAt:                    &actualDeletedAt,
			ThreadParentMessageID:        msg.ThreadParentID,
			ThreadParentMessageCreatedAt: msg.ThreadParentCreatedAt,
			TShow:                        msg.TShow,
		},
		SiteID:             siteID,
		Timestamp:          deletedAtMs,
		NewTCount:          newTcount,
		NewThreadLastMsgAt: newThreadLastMsgAt,
	}
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalDeleted(siteID), &canonicalEvt)

	return &models.DeleteMessageResponse{
		MessageID: req.MessageID,
		DeletedAt: deletedAtMs,
	}, nil
}

// publishCanonicalBestEffort publishes a canonical event; failures are logged and swallowed (Cassandra is source of truth).
func (s *HistoryService) publishCanonicalBestEffort(c *natsrouter.Context, subj string, evt *model.MessageEvent) {
	payload, err := json.Marshal(evt)
	if err != nil {
		slog.Warn("canonical marshal failed",
			"error", err, "subject", subj, "messageID", evt.Message.ID, "room_id", evt.Message.RoomID)
		return
	}
	if err := s.publisher.Publish(c, subj, payload, natsutil.CanonicalDedupID(evt)); err != nil {
		slog.Warn("canonical publish failed",
			"error", err, "subject", subj, "messageID", evt.Message.ID, "room_id", evt.Message.RoomID)
	}
}

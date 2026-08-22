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
	// loadHistoryEnvelope reserves bytes for a paginated response's non-item
	// fields (flags, cursor, minUserLastSeenAt) plus JSON punctuation.
	loadHistoryEnvelope = 256
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
	// Trim last: both passes above change encoded size. Rows are DESC, so
	// dropping the tail leaves the client's next before = oldest kept createdAt.
	kept, trimmed := s.fitPage(page.Data, loadHistoryEnvelope)
	// An empty page must never claim hasNext: this RPC pages by before = oldest
	// returned createdAt, so an empty resumable page (budget-exhausted walk over
	// a long silent gap) would leave the client no way to advance.
	return &models.LoadHistoryResponse{
		Messages:          kept,
		HasNext:           (page.HasNext || trimmed) && len(kept) > 0,
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

// LoadSurroundingMessages centers a window on exactly one of req.MessageID (a
// central message spliced into the middle) or req.Timestamp (a UTC-millis pivot,
// no central message). It validates the exactly-one-of contract, clamps the
// limit, and dispatches to the mode-specific handler.
func (s *HistoryService) LoadSurroundingMessages(c *natsrouter.Context, req models.LoadSurroundingMessagesRequest) (*models.LoadSurroundingMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	hasID := req.MessageID != ""
	hasTS := req.Timestamp != nil
	switch {
	case hasID && hasTS:
		return nil, errcode.BadRequest("provide either messageId or timestamp, not both")
	case !hasID && !hasTS:
		return nil, errcode.BadRequest("messageId or timestamp is required")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = surroundingPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}

	if hasTS {
		return s.loadSurroundingByTimestamp(c, account, roomID, req, limit)
	}
	return s.loadSurroundingByMessageID(c, account, roomID, req, limit)
}

// loadSurroundingByMessageID centers the window on req.MessageID: the central
// message is looked up, spliced into the middle of the result, and the before /
// after reads use strict bounds around its created_at.
func (s *HistoryService) loadSurroundingByMessageID(c *natsrouter.Context, account, roomID string, req models.LoadSurroundingMessagesRequest, limit int) (*models.LoadSurroundingMessagesResponse, error) {
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

	beforeFn := func(ctx context.Context) (cassrepo.Page[models.Message], error) {
		if accessSince == nil {
			return s.msgReader.GetMessagesBefore(ctx, roomID, centralMsg.CreatedAt, floor, beforePageReq)
		}
		return s.msgReader.GetMessagesBetweenDesc(ctx, roomID, *accessSince, centralMsg.CreatedAt, beforePageReq)
	}
	afterFn := func(ctx context.Context) (cassrepo.Page[models.Message], error) {
		return s.msgReader.GetMessagesAfter(ctx, roomID, centralMsg.CreatedAt, ceiling, afterPageReq)
	}

	return s.assembleSurrounding(c, roomID, accessSince, centralMsg, beforeFn, afterFn)
}

// loadSurroundingByTimestamp centers the window on a UTC-millis pivot. There is
// no central message, so limit splits cleanly (before gets the larger half). The
// before read must be inclusive of a message sitting exactly on the pivot
// (created_at <= pivot); since created_at and the pivot are both millisecond-
// precision, `created_at < pivot+1ms` is exactly `created_at <= pivot`, so the
// existing strict reads are reused with the pivot shifted up one millisecond.
// The after read stays strict (created_at > pivot), so an exact-match message
// anchors the read group without being duplicated below.
func (s *HistoryService) loadSurroundingByTimestamp(c *natsrouter.Context, account, roomID string, req models.LoadSurroundingMessagesRequest, limit int) (*models.LoadSurroundingMessagesResponse, error) {
	if *req.Timestamp <= 0 {
		return nil, errcode.BadRequest("timestamp must be positive")
	}
	pivot := time.UnixMilli(*req.Timestamp).UTC()
	// Inclusive upper bound via +1ms — equivalent to created_at <= pivot at
	// millisecond precision. When pivot is the last ms of its bucket window
	// (the only case where pivot+1ms crosses into the next bucket), pivot is
	// also that bucket's maximum time, so "all of the bucket" still equals
	// "created_at <= pivot": the shift stays exact at bucket boundaries.
	beforeUpper := pivot.Add(time.Millisecond)

	now := time.Now().UTC()
	// No findMessage dependency, so the access check and room-times resolve run concurrently.
	accessSince, lastMsgAt, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}
	if accessSince != nil && pivot.Before(*accessSince) {
		return nil, errcode.Forbidden("timestamp is outside access window", errcode.WithReason(errcode.MessageOutsideAccessWindow))
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)

	beforeCount := (limit + 1) / 2
	afterCount := limit / 2

	beforePageReq, err := parsePageRequest("", beforeCount)
	if err != nil {
		return nil, err
	}
	// afterCount == 0 only when limit == 1; skip the read rather than let
	// parsePageRequest(0) balloon the page size to defaultCassPageSize.
	var afterPageReq cassrepo.PageRequest
	if afterCount > 0 {
		afterPageReq, err = parsePageRequest("", afterCount)
		if err != nil {
			return nil, err
		}
	}

	beforeFn := func(ctx context.Context) (cassrepo.Page[models.Message], error) {
		if accessSince == nil {
			return s.msgReader.GetMessagesBefore(ctx, roomID, beforeUpper, floor, beforePageReq)
		}
		return s.msgReader.GetMessagesBetweenDesc(ctx, roomID, *accessSince, beforeUpper, beforePageReq)
	}
	afterFn := func(ctx context.Context) (cassrepo.Page[models.Message], error) {
		if afterCount == 0 {
			return cassrepo.Page[models.Message]{}, nil
		}
		return s.msgReader.GetMessagesAfter(ctx, roomID, pivot, ceiling, afterPageReq)
	}

	return s.assembleSurrounding(c, roomID, accessSince, nil, beforeFn, afterFn)
}

// assembleSurrounding runs the before/after page reads plus the read-floor read
// in parallel, then assembles them ASC: the reversed DESC before-page, the
// optional central message (nil in timestamp-pivot mode), then the after-page.
func (s *HistoryService) assembleSurrounding(
	c *natsrouter.Context,
	roomID string,
	accessSince *time.Time,
	central *models.Message,
	beforeFn, afterFn func(ctx context.Context) (cassrepo.Page[models.Message], error),
) (*models.LoadSurroundingMessagesResponse, error) {
	var (
		beforePage    cassrepo.Page[models.Message]
		afterPage     cassrepo.Page[models.Message]
		lastSeenFloor *time.Time
	)
	g, gctx := errgroup.WithContext(c)
	g.Go(func() error {
		var berr error
		beforePage, berr = beforeFn(gctx)
		if berr != nil {
			return fmt.Errorf("loading surrounding messages (before): %w", berr)
		}
		return nil
	})
	g.Go(func() error {
		var aerr error
		afterPage, aerr = afterFn(gctx)
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

	// Assemble in ASC order: reverse the DESC before-page, append the optional central, then after-page.
	capacity := len(beforePage.Data) + len(afterPage.Data)
	if central != nil {
		capacity++
	}
	messages := make([]models.Message, 0, capacity)
	for i := len(beforePage.Data) - 1; i >= 0; i-- {
		messages = append(messages, beforePage.Data[i])
	}
	if central != nil {
		messages = append(messages, *central)
	}
	messages = append(messages, afterPage.Data...)

	redactUnavailableQuotes(messages, accessSince)
	setDecodedAttachments(c, messages)
	// Trim outward from the pivot so the caller keeps the row they centred on;
	// each end that loses rows sets its own "more" flag.
	pivotIdx := len(beforePage.Data)
	lo, hi := s.fitWindow(messages, pivotIdx, loadHistoryEnvelope)
	return &models.LoadSurroundingMessagesResponse{
		Messages:          messages[lo:hi],
		MoreBefore:        beforePage.HasNext || lo > 0,
		MoreAfter:         afterPage.HasNext || hi < len(messages),
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

	canonicalEvt.PreviewMessage = s.previewAfterMutation(c, msg, roomID, editedAt)
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

	canonicalEvt.PreviewMessage = s.previewAfterMutation(c, msg, roomID, actualDeletedAt)
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalDeleted(siteID), &canonicalEvt)

	return &models.DeleteMessageResponse{
		MessageID: req.MessageID,
		DeletedAt: deletedAtMs,
	}, nil
}

// previewAfterMutation resolves the room's current last-eligible preview (same walk as
// subscription.list) to carry on an edit/delete fan-out. Hidden thread replies (TShow==false)
// never appear in the room timeline, so they're skipped — clients tell those apart via the
// event's threadParentMessageId and drive the room preview themselves. TShow==true replies do
// appear in the room, so they still get a preview. nil for a hidden thread reply, when the room
// has no eligible message, or on a read error.
func (s *HistoryService) previewAfterMutation(c *natsrouter.Context, msg *models.Message, roomID string, at time.Time) *models.PreviewMessage {
	if msg.ThreadParentID != "" && !msg.TShow {
		return nil
	}
	if preview, ok := s.roomLastPreviewMessage(c, roomID, nil, at); ok {
		return &preview
	}
	return nil
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

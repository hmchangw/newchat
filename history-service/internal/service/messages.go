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
	"github.com/hmchangw/chat/pkg/mention"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	defaultPageSize     = 20
	surroundingPageSize = 50
	maxPageSize         = 100
	// pageEnvelope reserves bytes for a paginated response's non-item fields
	// plus JSON punctuation. History and surrounding share one envelope shape.
	pageEnvelope    = 256
	maxContentBytes = 20 * 1024 // 20 KB; mirrors message-gatekeeper's content cap
)

func (s *HistoryService) LoadHistory(c *natsrouter.Context, req models.LoadHistoryRequest) (*models.LoadHistoryResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)
	now := time.Now().UTC()

	// Two independent Mongo reads, run concurrently for one RTT. Access errors take
	// precedence so a "not subscribed" 403 isn't masked by a transient room-times error.
	accessSince, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	before := millisToTime(req.Before)
	if before.IsZero() {
		before = now
	}
	// No cap at lastMsgAt: that pointer lags the Cassandra write it is meant to
	// describe (see walkBounds), so capping here drops rows that already exist.
	// The clock is the only sound bound, and it still has to be applied.
	before = clampToCeiling(before, now)

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
			// Floor only: `before` is the caller's own ceiling, already clamped above.
			_, walkFloor := s.walkBounds(createdAt, now)
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
	s.resolveRemovedMemberNames(c, page.Data)
	// Trim last: both passes above change encoded size. Rows are DESC, so
	// dropping the tail leaves the client's next before = oldest kept createdAt.
	kept, trimmed, err := s.fitPage(c, page.Data, pageEnvelope)
	if err != nil {
		return nil, err
	}
	// An empty page must never claim hasNext: this RPC pages by before = oldest
	// returned createdAt, so an empty resumable page (budget-exhausted walk over
	// a long silent gap) would leave the client no way to advance.
	return &models.LoadHistoryResponse{
		Messages:          kept,
		HasNext:           (page.HasNext || trimmed) && len(kept) > 0,
		MinUserLastSeenAt: minMs,
		SizeLimited:       trimmed,
		IncompleteSince:   s.incompleteSince(c),
	}, nil
}

func (s *HistoryService) LoadNextMessages(c *natsrouter.Context, req models.LoadNextMessagesRequest) (*models.LoadNextMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)
	now := time.Now().UTC()

	accessSince, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	ceiling, floor := s.walkBounds(createdAt, now)

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
	s.resolveRemovedMemberNames(c, page.Data)
	return &models.LoadNextMessagesResponse{
		Messages:          page.Data,
		NextCursor:        page.NextCursor,
		HasNext:           page.HasNext,
		MinUserLastSeenAt: minMs,
		IncompleteSince:   s.incompleteSince(c),
	}, nil
}

// LoadSurroundingMessages centers a window on exactly one of req.MessageID (spliced into
// the middle) or req.Timestamp (a UTC-millis pivot, no central message).
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

// loadSurroundingByMessageID splices req.MessageID into the middle, with strict bounds.
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
	createdAt, err := s.resolveRoomTimesOrError(c, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}

	ceiling, floor := s.walkBounds(createdAt, now)

	remaining := limit - 1 // before gets the larger half on odd splits
	if remaining <= 0 {
		only := *centralMsg
		redactUnavailableQuote(&only, accessSince)
		decodeMessageAttachments(c, &only)
		s.resolveRemovedMemberName(c, &only)
		// Serial best-effort read — this path issues no page reads to parallelise against.
		return &models.LoadSurroundingMessagesResponse{
			Messages:          []models.Message{only},
			MinUserLastSeenAt: s.minUserLastSeenMillis(c, roomID),
			IncompleteSince:   s.incompleteSince(c),
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

// loadSurroundingByTimestamp centers on a UTC-millis pivot. The before read is inclusive
// (via +1ms) and the after read strict, so a message on the pivot is never duplicated.
func (s *HistoryService) loadSurroundingByTimestamp(c *natsrouter.Context, account, roomID string, req models.LoadSurroundingMessagesRequest, limit int) (*models.LoadSurroundingMessagesResponse, error) {
	if *req.Timestamp <= 0 {
		return nil, errcode.BadRequest("timestamp must be positive")
	}
	pivot := time.UnixMilli(*req.Timestamp).UTC()
	// Exact at bucket boundaries too: when pivot is the last ms of its window it is also
	// that bucket's maximum, so "all of the bucket" still equals created_at <= pivot.
	beforeUpper := pivot.Add(time.Millisecond)

	now := time.Now().UTC()
	// Same clamp as LoadHistory: the pivot is client-supplied, and it is this
	// read's DESC upper bound. The ASC read below needs none — its ceiling
	// already comes from walkBounds, so a future pivot simply yields nothing.
	beforeUpper = clampToCeiling(beforeUpper, now)
	// No findMessage dependency, so the access check and room-times resolve run concurrently.
	accessSince, createdAt, err := s.checkAccessAndRoomTimes(c, account, roomID, req.Meta, now)
	if err != nil {
		return nil, err
	}
	if accessSince != nil && pivot.Before(*accessSince) {
		return nil, errcode.Forbidden("timestamp is outside access window", errcode.WithReason(errcode.MessageOutsideAccessWindow))
	}

	ceiling, floor := s.walkBounds(createdAt, now)

	beforeCount := (limit + 1) / 2
	afterCount := limit / 2

	beforePageReq, err := parsePageRequest("", beforeCount)
	if err != nil {
		return nil, err
	}
	// afterCount == 0 only when limit == 1; skip it, or parsePageRequest(0) balloons.
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

// assembleSurrounding reads before/after/read-floor in parallel and assembles them ASC.
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
	s.resolveRemovedMemberNames(c, messages)
	// Trim outward from the pivot so the caller keeps the row they centred on;
	// each end that loses rows sets its own "more" flag.
	lo, hi, narrowed, err := s.fitWindow(c, messages, len(beforePage.Data), pageEnvelope)
	if err != nil {
		return nil, err
	}
	return &models.LoadSurroundingMessagesResponse{
		Messages:          messages[lo:hi],
		MoreBefore:        beforePage.HasNext || lo > 0,
		MoreAfter:         afterPage.HasNext || hi < len(messages),
		MinUserLastSeenAt: minMs,
		SizeLimited:       narrowed,
		IncompleteSince:   s.incompleteSince(c),
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

// readFloorInto is an errgroup task loading the read-floor into *dst; an error leaves nil.
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

// minUserLastSeenMillis is readFloorInto's serial counterpart; best-effort, nil on error.
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
	s.resolveRemovedMemberName(c, msg)
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
	s.resolveRemovedMemberNames(c, kept)
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

	// Re-resolve @mentions from the edited content so the persisted row, the
	// canonical event and search-sync all reflect the post-edit mentions. Fail
	// closed on a lookup error: a partial/empty set would be written over (or
	// clear) the stored mentions, permanently losing them. A retry resolves clean.
	resolved, err := mention.Resolve(c, req.NewMsg, s.users.FindUsersByAccounts)
	if err != nil {
		return nil, fmt.Errorf("resolve edited mentions for %s: %w", req.MessageID, err)
	}

	editedAt := time.Now().UTC()
	if err := s.msgWriter.UpdateMessageContent(c, msg, req.NewMsg, resolved.Participants, editedAt); err != nil {
		// A TOCTOU between findMessage and the CAS edit is a benign race, not a server
		// fault — map it to 4xx so it doesn't pollute 5xx telemetry.
		if errors.Is(err, cassrepo.ErrMessageNotFound) {
			return nil, errcode.NotFound("message not found")
		}
		return nil, fmt.Errorf("editing message %s: %w", req.MessageID, err)
	}

	editedAtMs := editedAt.UnixMilli()

	// search-sync-worker reindexes the FULL doc, so attachments/card must ride
	// along or edits wipe them. Mentions carry the re-resolved set so the stored
	// row, the event and search-sync agree on the post-edit mentions.
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
			Mentions:                     resolved.Participants,
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

	// Resolve, publish, then persist: the Cassandra edit has committed, so a stalled store
	// must not leave the mutation invisible to every canonical consumer.
	pvw := s.previewAfterMutation(c, msg, roomID)
	canonicalEvt.PreviewMessage = pvw.EventPreview()
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalUpdated(siteID), &canonicalEvt)
	s.persistMutatedPreview(c, roomID, msg.MessageID, &pvw, editedAt)

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

	// Echo updated_at rather than re-deleting: prevents tcount double-decrement on retry
	// and duplicate events. The first delete's tcount is already durable in Cassandra.
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

	// Resolve, publish, then persist — see EditMessage.
	pvw := s.previewAfterMutation(c, msg, roomID)
	canonicalEvt.PreviewMessage = pvw.EventPreview()
	s.publishCanonicalBestEffort(c, subject.MsgCanonicalDeleted(siteID), &canonicalEvt)
	s.persistMutatedPreview(c, roomID, msg.MessageID, &pvw, actualDeletedAt)

	return &models.DeleteMessageResponse{
		MessageID: req.MessageID,
		DeletedAt: deletedAtMs,
	}, nil
}

// previewAfterMutation resolves the room's last-eligible preview after an edit or delete,
// skipping hidden thread replies. It does not persist: the caller publishes first.
func (s *HistoryService) previewAfterMutation(c *natsrouter.Context, msg *models.Message, roomID string) previewWalk {
	if msg.ThreadParentID != "" && !msg.TShow {
		return previewWalk{State: previewSkipped}
	}
	return s.roomLastPreviewMessage(c, roomID, nil, time.Now().UTC())
}

// persistMutatedPreview stores what the mutation walk resolved, and repairs the room when
// it could not. Best-effort and bounded throughout: the mutation itself has committed.
//
// The repair is the load-bearing half. The reader serves a stored preview on
// previewForMsgId == lastMsgId, and a mutation never moves lastMsgId, so a body this
// mutation changed but failed to replace keeps reading as current — deleted content stays
// on the room list until the room's next message, which may never come (#226). Whenever
// the write does not land, the freshness key comes off instead, and the next read misses,
// walks and warms back.
//
// It cannot cover every failure: an invalidate is itself a Mongo write, so a Mongo outage
// takes both. It does cover the ones where only the write path broke — a failed seal
// (Vault), a guard that rejected the write, and a walk that never resolved — which is
// every case where the room is repairable at all without a durable retry queue.
//
// Both attempts share ONE budget. The mutation has already committed, so this whole
// function is time the client spends on a request that succeeded; a fresh window for the
// repair would double that wait exactly when Mongo is unwell, since the write it follows
// timed out rather than failed fast. A fast failure — a rejected guard, a failed seal —
// still leaves the repair nearly all of it.
func (s *HistoryService) persistMutatedPreview(c *natsrouter.Context, roomID, msgID string, w *previewWalk, at time.Time) {
	// A hidden thread reply never reaches the room timeline, so no stored preview can
	// describe it: nothing to write, and nothing to withdraw.
	if w.State == previewSkipped {
		return
	}
	// Before the writes, and unconditional: the mutation has already committed in
	// Cassandra, so whatever the cache holds for this room now describes a message that
	// changed -- possibly one that was just deleted. Even a degraded walk, which writes
	// nothing, must not leave that entry servable (#292).
	if s.previewCache != nil {
		s.previewCache.Invalidate(roomID)
	}

	ctx, cancel := context.WithTimeout(c, warmBackTimeout)
	defer cancel()

	applied, err := s.writeMutatedPreview(ctx, roomID, w, at.UnixMilli())
	if err != nil {
		slog.WarnContext(c, "persist mutated room preview failed", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(c), "error", err)
	}
	if applied {
		return
	}
	// Keyed on the mutated message, not on what the walk observed: the freshness key can
	// name a message the body does not describe, so only the body's own id identifies
	// what this mutation invalidated. Already-replaced bodies make it a no-op.
	if err := s.rooms.InvalidatePreviewKey(ctx, roomID, msgID, at.UnixMilli()); err != nil {
		slog.WarnContext(c, "withdraw stale room preview key failed", "room_id", roomID,
			"message_id", msgID, "request_id", natsutil.RequestIDFromContext(c), "error", err)
	}
}

// writeMutatedPreview applies the walk's outcome, reporting whether the write landed. A
// degraded walk establishes nothing, so it writes nothing and reports not-applied — the
// caller's repair is what keeps the room from serving the body it could not re-derive.
//
// ctx is the caller's shared repair budget, not this write's own: see persistMutatedPreview.
func (s *HistoryService) writeMutatedPreview(ctx context.Context, roomID string, w *previewWalk, asOf int64) (bool, error) {
	switch w.State {
	case previewFound:
		// Body only: a mutation does not move lastMsgId. Pinned to the key the walk
		// OBSERVED, not left unconditional — an insert landing between the walk and this
		// write advances the key, and an unpinned body would then be stored under it.
		return s.rooms.UpdatePreviewBody(ctx, roomID, w.Preview, w.NewestObservedID, asOf)
	case previewEmpty:
		// The one outcome authorising a destructive write: completed, and nothing left.
		return s.rooms.ClearPreview(ctx, roomID, asOf)
	default: // previewDegraded — a survivor may still exist, so the body must survive
		return false, nil
	}
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

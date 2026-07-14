package service

import (
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// emptyThreadResponse is the shared "no replies" shape for all short-circuit branches.
// parent is the fetched thread-parent message and is always included in the response.
func emptyThreadResponse(parent *models.Message) *models.GetThreadMessagesResponse {
	return &models.GetThreadMessagesResponse{Messages: []models.Message{}, HasNext: false, ParentMessage: parent}
}

// NATS: chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread
func (s *HistoryService) GetThreadMessages(c *natsrouter.Context, req models.GetThreadMessagesRequest) (*models.GetThreadMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	if req.ThreadMessageID == "" {
		return nil, errcode.BadRequest("threadMessageId is required")
	}

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	msg, err := s.findMessage(c, roomID, req.ThreadMessageID)
	if err != nil {
		return nil, err
	}

	if msg.ThreadParentID != "" {
		return nil, errcode.BadRequest("threadMessageId must be a top-level message, not a reply")
	}

	if accessSince != nil && msg.CreatedAt.Before(*accessSince) {
		return nil, errcode.Forbidden("thread is outside access window", errcode.WithReason(errcode.MessageOutsideAccessWindow))
	}

	// Apply redaction to the parent's quoted message in-place before including it in the
	// response. This must run before both short-circuit returns below so no branch can
	// return an unredacted parent. msg is subsequently stored in ParentMessage.
	redactUnavailableQuote(msg, accessSince)
	// Decode the parent's attachments here too — before the early returns below, so
	// no-reply / tcount==0 threads still return a decoded ParentMessage.
	decodeMessageAttachments(c, msg)

	// Empty ThreadRoomID means no replies yet or a silently-failed stamp in message-worker.
	if msg.ThreadRoomID == "" {
		slog.Warn("thread fetch: parent has empty thread_room_id, returning no replies",
			"request_id", natsutil.RequestIDFromContext(c),
			"room_id", roomID,
			"messageID", req.ThreadMessageID,
			"messageCreatedAt", msg.CreatedAt,
			"account", account,
		)
		return emptyThreadResponse(msg), nil
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

	// tcount==0 means all replies were deleted — skip Cassandra. nil means never written
	// (new parent, or mid-write before the tcount LWT) and must fall through or replies could be hidden.
	if msg.TCount != nil && *msg.TCount == 0 {
		return emptyThreadResponse(msg), nil
	}

	// Server-clock bounds only: thread replies never bump rooms.lastMsgAt (fan-out skips it),
	// and the single-partition slice has no bucket walk — the loose ceiling only guards future-dated rows.
	now := time.Now().UTC()
	ceiling := now.Add(clockSkewTolerance)
	floor := now.Add(-s.historyFloor)
	if accessSince != nil && accessSince.After(floor) {
		floor = *accessSince
	}
	// Defensive: reachable only with an accessSince beyond the skew tolerance and a parent
	// dated past it; collapse rather than hand Cassandra an inverted slice.
	if ceiling.Before(floor) {
		ceiling = floor
	}

	// Fetch Cassandra page and minUserLastSeenAt from Mongo in parallel.
	// Floor fetch failure is non-fatal: thread messages load normally and
	// minUserLastSeenAt is simply omitted from the response.
	var page cassrepo.Page[models.Message]
	var threadFloor *time.Time
	g, gctx := errgroup.WithContext(c)
	g.Go(func() error {
		var pErr error
		page, pErr = s.msgReader.GetThreadMessages(gctx, msg.ThreadRoomID, ceiling, floor, pageReq)
		return pErr
	})
	g.Go(func() error {
		t, fErr := s.threadRooms.GetMinThreadUserLastSeenAt(gctx, msg.ThreadRoomID)
		if fErr != nil {
			slog.Warn("loading thread minUserLastSeenAt", "error", fErr,
				"request_id", natsutil.RequestIDFromContext(c),
				"account", account, "room_id", roomID, "thread_room_id", msg.ThreadRoomID)
			return nil
		}
		threadFloor = t
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("loading thread messages: %w", err)
	}

	var minMs *int64
	if threadFloor != nil {
		ms := threadFloor.UTC().UnixMilli()
		minMs = &ms
	}

	redactUnavailableQuotes(page.Data, accessSince)
	setDecodedAttachments(c, page.Data)
	return &models.GetThreadMessagesResponse{
		Messages:          page.Data,
		NextCursor:        page.NextCursor,
		HasNext:           page.HasNext,
		ParentMessage:     msg,
		MinUserLastSeenAt: minMs,
	}, nil
}

// threadUnread reports whether a thread has activity the user hasn't seen: a nil
// lastSeenAt (never opened) is always unread, otherwise lastMsgAt must be newer.
func threadUnread(lastMsgAt time.Time, lastSeenAt *time.Time) bool {
	if lastSeenAt == nil {
		return true
	}
	return lastMsgAt.After(*lastSeenAt)
}

// ListThreadSubscriptions is the per-site leaf of the cross-site thread inbox:
// it returns the account's thread subscriptions on this site, newest activity
// first, hydrated with each thread's parent and last message plus the owning
// room's name/type. Server-to-server.
// NATS: chat.server.request.thread.{siteID}.subscription.list
func (s *HistoryService) ListThreadSubscriptions(c *natsrouter.Context, req pkgmodel.ThreadSubscriptionListRequest) (*pkgmodel.ThreadSubscriptionListResponse, error) {
	if req.Account == "" {
		return nil, errcode.BadRequest("account is required")
	}
	c.WithLogValues("account", req.Account)

	limit := req.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}

	var cursorTs *time.Time
	if req.CursorLastMsgAt != nil {
		t := time.UnixMilli(*req.CursorLastMsgAt).UTC()
		cursorTs = &t
	}

	// Rows are returned as-is — every thread subscription the user holds on this
	// site is visible. The page is a single keyset fetch; hasMore comes straight
	// from the repository's limit+1 probe.
	rows, hasMore, err := s.threadSubs.ListUserThreadSubscriptions(c, req.Account, cursorTs, req.CursorThreadRoomID, limit)
	if err != nil {
		return nil, fmt.Errorf("listing thread subscriptions: %w", err)
	}
	items, err := s.buildThreadItems(c, rows)
	if err != nil {
		return nil, err
	}
	return &pkgmodel.ThreadSubscriptionListResponse{Items: items, HasMore: hasMore}, nil
}

// buildThreadItems hydrates one page of thread rows into list items. A thread is
// included only when BOTH its parent and last message hydrate from Cassandra; a
// row missing either (hard-deleted, or not yet replicated) is skipped rather than
// surfaced as a half-empty item. Room access is not re-checked here: every row is
// the user's own thread subscription on this site.
func (s *HistoryService) buildThreadItems(c *natsrouter.Context, rows []mongorepo.ThreadSubRow) ([]pkgmodel.ThreadListItem, error) {
	items := make([]pkgmodel.ThreadListItem, 0, len(rows))
	if len(rows) == 0 {
		return items, nil
	}

	// Hydrate message bodies from Cassandra (room name/type already rode in on
	// the rows via the aggregation's rooms $lookup).
	msgs, err := s.msgReader.GetMessagesByIDs(c, threadListLookupMsgIDs(rows))
	if err != nil {
		return nil, fmt.Errorf("hydrating thread list messages: %w", err)
	}
	msgByID := make(map[string]models.Message, len(msgs))
	for i := range msgs {
		msgByID[msgs[i].MessageID] = msgs[i]
	}

	for i := range rows {
		row := rows[i]
		parent, hasParent := msgByID[row.ParentMessageID]
		last, hasLast := msgByID[row.LastMsgID]
		if !hasParent || !hasLast {
			continue // skip threads we can't fully hydrate
		}
		item := pkgmodel.ThreadListItem{
			SiteID:          row.SiteID,
			RoomID:          row.RoomID,
			RoomName:        row.RoomName,
			RoomType:        row.RoomType,
			ThreadRoomID:    row.ThreadRoomID,
			ParentMessageID: row.ParentMessageID,
			HasMention:      row.HasMention,
			Unread:          threadUnread(row.LastMsgAt, row.LastSeenAt),
			LastMsgAt:       row.LastMsgAt.UTC().UnixMilli(),
		}
		if row.LastSeenAt != nil {
			ms := row.LastSeenAt.UTC().UnixMilli()
			item.LastSeenAt = &ms
		}
		item.ParentMessage = &parent
		item.LastMessage = &last
		items = append(items, item)
	}
	return items, nil
}

// threadListLookupMsgIDs collects the distinct message IDs (parents ∪ last) the
// page needs hydrated from Cassandra. Room name/type ride in on the rows via the
// aggregation's rooms $lookup, so rooms need no separate hydration.
func threadListLookupMsgIDs(rows []mongorepo.ThreadSubRow) []string {
	msgSeen := make(map[string]struct{}, len(rows)*2)
	msgIDs := make([]string, 0, len(rows)*2)
	addMsg := func(id string) {
		if id == "" {
			return
		}
		if _, dup := msgSeen[id]; dup {
			return
		}
		msgSeen[id] = struct{}{}
		msgIDs = append(msgIDs, id)
	}
	for i := range rows {
		addMsg(rows[i].ParentMessageID)
		addMsg(rows[i].LastMsgID)
	}
	return msgIDs
}

// validateThreadFilter normalizes an empty filter to "all" so clients can omit the field.
func validateThreadFilter(filter models.ThreadFilter) (models.ThreadFilter, error) {
	switch filter {
	case "", models.ThreadFilterAll:
		return models.ThreadFilterAll, nil
	case models.ThreadFilterFollowing, models.ThreadFilterUnread:
		return filter, nil
	default:
		return "", errcode.BadRequest(fmt.Sprintf("invalid thread filter: %q", filter))
	}
}

// GetThreadParentMessages handles chat.user.{account}.request.room.{roomID}.{siteID}.msg.thread.parent.
func (s *HistoryService) GetThreadParentMessages(c *natsrouter.Context, req models.GetThreadParentMessagesRequest) (*models.GetThreadParentMessagesResponse, error) {
	account := c.Param("account")
	roomID := c.Param("roomID")
	c.WithLogValues("account", account, "room_id", roomID)

	accessSince, err := s.getAccessSince(c, account, roomID)
	if err != nil {
		return nil, err
	}

	filter, err := validateThreadFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	pageReq := mongoutil.NewOffsetPageRequest(req.Offset, req.Limit)

	var threadPage mongoutil.OffsetPage[pkgmodel.ThreadRoom]
	switch filter {
	case models.ThreadFilterAll:
		threadPage, err = s.threadRooms.GetThreadRooms(c, roomID, accessSince, pageReq)
	case models.ThreadFilterFollowing:
		threadPage, err = s.threadRooms.GetFollowingThreadRooms(c, roomID, account, accessSince, pageReq)
	case models.ThreadFilterUnread:
		threadPage, err = s.threadRooms.GetUnreadThreadRooms(c, roomID, account, accessSince, pageReq)
	default:
		return nil, errcode.Internal("unhandled thread filter",
			errcode.WithCause(fmt.Errorf("unhandled thread filter: %q", filter)))
	}
	if err != nil {
		return nil, fmt.Errorf("loading thread rooms (filter %s): %w", filter, err)
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
		return nil, fmt.Errorf("hydrating thread parent messages: %w", err)
	}

	msgByID := make(map[string]models.Message, len(cassMessages))
	for i := range cassMessages {
		msgByID[cassMessages[i].MessageID] = cassMessages[i]
	}

	// Iterate parentIDs (deduplicated) to avoid emitting the same parent twice for duplicate MongoDB thread rooms.
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
	setDecodedAttachments(c, parentMessages)
	return &models.GetThreadParentMessagesResponse{ParentMessages: parentMessages, Total: threadPage.Total}, nil
}

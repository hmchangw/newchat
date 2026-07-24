package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const (
	maxRoomsGetBatch       = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency = 16  // mirrors cassrepo.maxConcurrentIDReads
	lastMsgWalkPageSize    = 50  // messages scanned per walk-back page
	lastMsgWalkMaxPages    = 5   // ponytail: cap the ineligible-tail walk; a room with >250 trailing ineligible messages just shows no last message
)

// RoomsGet handles chat.server.request.history.{siteID}.rooms.get: for each requested
// room, return its latest non-deleted message. Server-to-server (no per-account access
// check). Per-room failures degrade to no entry so one bad room never fails the batch.
func (s *HistoryService) RoomsGet(c *natsrouter.Context, req models.RoomsGetRequest) (*models.RoomsGetResponse, error) {
	if len(req.RoomIDs) == 0 {
		return nil, errcode.BadRequest("roomIds must not be empty")
	}
	if len(req.RoomIDs) > maxRoomsGetBatch {
		return nil, errcode.BadRequest("too many roomIds")
	}

	ids := dedupRoomIDs(req.RoomIDs)
	now := time.Now().UTC()

	out := make(map[string]models.PreviewMessage, len(ids))
	var mu sync.Mutex
	// WaitGroup+sem (not errgroup): per-room failures must degrade, never cancel
	// siblings. Acquire sem before spawning so live goroutine count stays bounded.
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxRoomsGetConcurrency)
	for _, roomID := range ids {
		// Context cancelled/timed out: the caller's gone, so propagate the error
		// rather than return a partial OK that won't be read.
		if err := c.Err(); err != nil {
			return nil, err
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			lm, ok := s.roomLastMessage(c, roomID, now)
			if !ok {
				return
			}
			mu.Lock()
			out[roomID] = lm
			mu.Unlock()
		}()
	}
	wg.Wait()

	return &models.RoomsGetResponse{Rooms: out}, nil
}

// roomLastMessage resolves one room's latest eligible message at read time.
// ok=false means drop the room (empty, all-ineligible within the walk cap, or a read
// failure). Walks backward from lastMsgAt in pages, skipping ineligible messages.
func (s *HistoryService) roomLastMessage(ctx context.Context, roomID string, now time.Time) (models.PreviewMessage, bool) {
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(ctx, roomID, nil, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.PreviewMessage{}, false
	}

	pageReq, err := parsePageRequest("", lastMsgWalkPageSize)
	if err != nil {
		return models.PreviewMessage{}, false
	}
	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	for range lastMsgWalkMaxPages {
		page, err := s.msgReader.GetMessagesBefore(ctx, roomID, before, floor, pageReq)
		if err != nil {
			slog.WarnContext(ctx, "rooms.get latest-message read degraded", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return models.PreviewMessage{}, false
		}
		if len(page.Data) == 0 {
			return models.PreviewMessage{}, false // room empty or floor reached
		}
		for i := range page.Data {
			m := page.Data[i]
			// System messages and quoted replies aren't representative room content —
			// skip to the previous eligible message, same as a deleted one.
			if m.Deleted || m.Type != "" || m.QuotedParentMessage != nil {
				continue
			}
			return s.toPreviewMessage(ctx, &m), true
		}
		// Whole page ineligible (deleted/system/quoted). A short page means the walk
		// is exhausted (no older messages) — stop. Otherwise page again strictly
		// before the oldest one seen.
		if len(page.Data) < lastMsgWalkPageSize {
			return models.PreviewMessage{}, false
		}
		before = page.Data[len(page.Data)-1].CreatedAt
	}
	return models.PreviewMessage{}, false // ineligible tail longer than the walk cap
}

// toPreviewMessage enriches an eligible message into the room-list preview: sender and
// mentions become wire Participants (chineseName from the Cassandra company_name), a bot
// sender's displayName is its app name, and attachments/visibleTo pass through the
// projection the walk already read.
func (s *HistoryService) toPreviewMessage(ctx context.Context, m *models.Message) models.PreviewMessage {
	// The walk reads raw attachment blobs; other read paths decode via
	// setDecodedAttachments, so decode this one message before mapping.
	decodeMessageAttachments(ctx, m)
	sender := toWireParticipant(&m.Sender)
	sender.DisplayName = s.botAwareDisplayName(ctx, m.Sender.EngName, m.Sender.CompanyName, m.Sender.Account)

	var mentions []pkgmodel.Participant
	if len(m.Mentions) > 0 {
		mentions = make([]pkgmodel.Participant, len(m.Mentions))
		for i := range m.Mentions {
			mentions[i] = toWireParticipant(&m.Mentions[i])
		}
	}

	return models.PreviewMessage{
		MessageID:   m.MessageID,
		Sender:      sender,
		Content:     m.Msg,
		CreatedAt:   m.CreatedAt.UTC(),
		Attachments: m.DecodedAttachments,
		Mentions:    mentions,
		VisibleTo:   m.VisibleTo,
	}
}

// dedupRoomIDs removes duplicate roomIds, preserving first-seen order.
func dedupRoomIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

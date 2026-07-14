package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const (
	maxRoomsGetBatch        = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency  = 16  // mirrors cassrepo.maxConcurrentIDReads
	lastMessagePreviewRunes = 256 // room-list snippet cap
	lastMsgWalkPageSize     = 50  // messages scanned per walk-back page
	lastMsgWalkMaxPages     = 5   // ponytail: cap the deleted-tail walk; a room with >250 trailing deletes just shows no last message
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

	out := make(map[string]models.LastMessage, len(ids))
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

// roomLastMessage resolves one room's latest NON-deleted message at read time.
// ok=false means drop the room (empty, all-deleted within the walk cap, or a read
// failure). Walks backward from lastMsgAt in pages, skipping soft-deleted messages.
func (s *HistoryService) roomLastMessage(ctx context.Context, roomID string, now time.Time) (models.LastMessage, bool) {
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(ctx, roomID, nil, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.LastMessage{}, false
	}

	pageReq, err := parsePageRequest("", lastMsgWalkPageSize)
	if err != nil {
		return models.LastMessage{}, false
	}
	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	for range lastMsgWalkMaxPages {
		page, err := s.msgReader.GetMessagesBefore(ctx, roomID, before, floor, pageReq)
		if err != nil {
			slog.WarnContext(ctx, "rooms.get latest-message read degraded", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return models.LastMessage{}, false
		}
		if len(page.Data) == 0 {
			return models.LastMessage{}, false // room empty or floor reached
		}
		for i := range page.Data {
			m := page.Data[i]
			if m.Deleted {
				continue
			}
			return models.LastMessage{
				MessageID: m.MessageID,
				Sender:    m.Sender,
				Content:   previewContent(m.Msg),
				CreatedAt: m.CreatedAt.UTC().UnixMilli(),
			}, true
		}
		// Whole page deleted. A short page means the walk is exhausted (no older
		// messages) — stop. Otherwise page again strictly before the oldest one seen.
		if len(page.Data) < lastMsgWalkPageSize {
			return models.LastMessage{}, false
		}
		before = page.Data[len(page.Data)-1].CreatedAt
	}
	return models.LastMessage{}, false // deleted tail longer than the walk cap
}

// previewContent trims a message body to a rune-bounded room-list snippet.
func previewContent(msg string) string {
	if len(msg) <= lastMessagePreviewRunes {
		return msg // bytes ≤ cap ⇒ runes ≤ cap; no alloc on the common short case
	}
	r := []rune(msg)
	if len(r) <= lastMessagePreviewRunes {
		return msg
	}
	return string(r[:lastMessagePreviewRunes])
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

package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const (
	maxRoomsGetBatch        = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency  = 16  // mirrors cassrepo.maxConcurrentIDReads
	lastMessagePreviewRunes = 256 // room-list snippet cap
)

// RoomsGet handles chat.user.{account}.request.history.{siteID}.rooms.get: for each
// requested room the caller can access, return its latest message (A2 read-time,
// no walk-back). Per-room failures (incl. not-subscribed) degrade to no entry so
// one bad room never fails the batch.
func (s *HistoryService) RoomsGet(c *natsrouter.Context, req models.RoomsGetRequest) (*models.RoomsGetResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)

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
			lm, ok := s.roomLastMessage(c, account, roomID, now)
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

// roomLastMessage resolves one room's latest message at read time. ok=false means
// drop the room (not subscribed, empty, or a read failure). A soft-deleted latest
// message is returned as-is with Deleted=true — no walk-back to an earlier survivor.
func (s *HistoryService) roomLastMessage(ctx context.Context, account, roomID string, now time.Time) (models.LastMessage, bool) {
	accessSince, lastMsgAt, createdAt, err := s.checkAccessAndRoomTimes(ctx, account, roomID, nil, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "account", account, "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.LastMessage{}, false
	}

	pageReq, err := parsePageRequest("", 1)
	if err != nil {
		return models.LastMessage{}, false
	}
	// Cap the walk at the last message so a dormant room is a single-bucket read.
	before := lastMsgAt.Add(time.Millisecond)

	var page cassrepo.Page[models.Message]
	if accessSince == nil {
		// Clamp createdAt to the configured floor (mirrors LoadHistory).
		historyFloor := now.Add(-s.historyFloor)
		walkFloor := createdAt
		if walkFloor.IsZero() || walkFloor.Before(historyFloor) {
			walkFloor = historyFloor
		}
		page, err = s.msgReader.GetMessagesBefore(ctx, roomID, before, walkFloor, pageReq)
	} else {
		page, err = s.msgReader.GetMessagesBetweenDesc(ctx, roomID, *accessSince, before, pageReq)
	}
	if err != nil {
		slog.WarnContext(ctx, "rooms.get latest-message read degraded", "account", account, "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.LastMessage{}, false
	}
	if len(page.Data) == 0 {
		return models.LastMessage{}, false
	}

	m := page.Data[0]
	return models.LastMessage{
		MessageID: m.MessageID,
		Sender:    m.Sender,
		Content:   previewContent(m.Msg),
		CreatedAt: m.CreatedAt.UTC().UnixMilli(),
		Deleted:   m.Deleted,
	}, true
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

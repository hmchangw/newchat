package service

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

const (
	maxRoomsGetBatch       = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency = 16  // mirrors cassrepo.maxConcurrentIDReads

	// Preview walk: fetch a tiny first page so the common case (newest message
	// eligible) costs one Cassandra query instead of over-walking older buckets
	// to fill a 50-row page whose extra rows are unused. Grow geometrically only
	// to skip a run of ineligible (deleted/system) messages; lastMsgWalkMaxScan
	// preserves the previous 50×5 = 250 ineligible-skip budget before giving up.
	// lastMsgWalkMaxPage caps the escalation at cassrepo.MaxPageSize, the
	// codebase-wide per-query row cap, so the ×8 growth never over-requests.
	lastMsgWalkFirstPage = 1
	lastMsgWalkGrowth    = 8
	lastMsgWalkMaxScan   = 250
	lastMsgWalkMaxPage   = cassrepo.MaxPageSize
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
	// Hints are only ever consulted for the (capped) requested room IDs, so a larger
	// map is malformed; reject it symmetrically rather than unmarshal/allocate it.
	if len(req.Hints) > maxRoomsGetBatch {
		return nil, errcode.BadRequest("too many hints")
	}

	ids := dedupRoomIDs(req.RoomIDs)
	now := time.Now().UTC()
	timesByRoom := s.resolveRoomMetaHints(c, ids, req.Hints, now)

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
		times := timesByRoom[roomID]
		if times.state == roomTimesMissing {
			continue // room doesn't exist here — degrade to no entry, no further reads
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			lm, ok := s.resolvePreview(c, roomID, times, now)
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

// roomTimesState says how a room's walk-bound times were (or weren't) resolved by
// the up-front hint/batch step, and therefore what the per-room resolve must do.
type roomTimesState int

const (
	// roomTimesFallback: no up-front resolution (batch read degraded, or a direct
	// caller like previewAfterMutation) — resolve per-room via resolveRoomTimes.
	roomTimesFallback roomTimesState = iota
	// roomTimesHinted: a usable caller hint — validate via resolveRoomTimes
	// (sanitize + consistency refetch), which skips Mongo in the common case.
	roomTimesHinted
	// roomTimesResolved: times came from our own batch read — authoritative, use
	// verbatim (re-reading the same document past the hint sanitizer buys nothing).
	roomTimesResolved
	// roomTimesMissing: absent from a successful batch read — the room doesn't
	// exist here (orphaned subscription / deleted room); a per-room retry of the
	// same read is doomed, so drop the room without further store reads.
	roomTimesMissing
)

// roomTimesResolution is the per-room outcome of resolveRoomMetaHints. The zero
// value means roomTimesFallback (fresh per-room resolve).
type roomTimesResolution struct {
	state     roomTimesState
	meta      *models.RoomMeta // hint payload when state == roomTimesHinted
	lastMsgAt time.Time        // batch times when state == roomTimesResolved
	createdAt time.Time
}

// resolveRoomMetaHints resolves each room's walk-bound times up front: a usable hint
// (per the same sanitizeLastMsgAt every per-room resolve uses) is kept as a hint, and
// ONE batched GetRoomTimesByIDs read covers all the rest. Batch results are
// authoritative — present rooms carry verbatim times (even a zero lastMsgAt), absent
// rooms are known missing. A batch-read failure degrades: those rooms keep the
// zero-value fallback resolution, so the per-room resolveRoomTimes fires for them —
// the batch failure never fails the whole RPC.
func (s *HistoryService) resolveRoomMetaHints(
	ctx context.Context,
	ids []string,
	hints map[string]pkgmodel.RoomTimeHint,
	now time.Time,
) map[string]roomTimesResolution {
	res := make(map[string]roomTimesResolution, len(ids))
	unhinted := make([]string, 0, len(ids))
	for _, roomID := range ids {
		hint, ok := hints[roomID]
		if !ok || sanitizeLastMsgAt(hint.LastMsgAt, now) == nil {
			unhinted = append(unhinted, roomID)
			continue
		}
		res[roomID] = roomTimesResolution{
			state: roomTimesHinted,
			meta:  &models.RoomMeta{LastMsgAt: hint.LastMsgAt, CreatedAt: hint.CreatedAt},
		}
	}
	if len(unhinted) == 0 {
		return res
	}

	times, err := s.rooms.GetRoomTimesByIDs(ctx, unhinted)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get batch room-times read degraded, falling back per-room",
			"room_count", len(unhinted), "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return res
	}
	for _, roomID := range unhinted {
		rt, ok := times[roomID]
		if !ok {
			res[roomID] = roomTimesResolution{state: roomTimesMissing}
			continue
		}
		res[roomID] = roomTimesResolution{state: roomTimesResolved, lastMsgAt: rt.LastMsgAt, createdAt: rt.CreatedAt}
	}
	return res
}

// resolvePreview resolves one room's preview, serving it from the preview cache
// when installed. The cache is positives-only, so empty rooms and read failures
// fall through to a fresh resolve. times is the caller/batch resolution outcome
// (zero value when none applies); previewAfterMutation (edit/delete) keeps calling
// roomLastPreviewMessage directly with the zero value so mutations always see fresh state.
func (s *HistoryService) resolvePreview(ctx context.Context, roomID string, times roomTimesResolution, now time.Time) (models.PreviewMessage, bool) {
	if s.previewCache == nil {
		return s.roomLastPreviewMessage(ctx, roomID, times, now)
	}
	preview, ok, err := s.previewCache.Get(ctx, roomID, func(ctx context.Context) (models.PreviewMessage, bool, error) {
		p, found := s.roomLastPreviewMessage(ctx, roomID, times, now)
		return p, found, nil
	})
	if err != nil {
		// ctx cancelled while waiting on a shared load — degrade like a read miss.
		return models.PreviewMessage{}, false
	}
	return preview, ok
}

// roomLastPreviewMessage resolves one room's latest eligible preview message at read time.
// times carries the up-front resolution outcome: batch-resolved times are used verbatim,
// a hint lets resolveRoomTimes skip its own Mongo read (see resolveRoomMetaHints), and the
// zero value always resolves fresh (previewAfterMutation's contract). ok=false means drop
// the room (empty, all-ineligible within the walk cap, or a read failure). Walks backward
// from lastMsgAt in pages, skipping ineligible messages.
func (s *HistoryService) roomLastPreviewMessage(ctx context.Context, roomID string, times roomTimesResolution, now time.Time) (models.PreviewMessage, bool) {
	lastMsgAt, createdAt := times.lastMsgAt, times.createdAt
	if times.state != roomTimesResolved {
		var err error
		lastMsgAt, createdAt, err = s.resolveRoomTimesOrError(ctx, roomID, times.meta, now)
		if err != nil {
			slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return models.PreviewMessage{}, false
		}
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	pageSize := lastMsgWalkFirstPage
	scanned := 0
	for scanned < lastMsgWalkMaxScan {
		// Clamp to the per-query cap and the remaining scan budget (both bounds are
		// positive here, so order doesn't matter). The next iteration's ×8 growth
		// starts from this clamped value.
		pageSize = min(pageSize, lastMsgWalkMaxPage, lastMsgWalkMaxScan-scanned)
		page, err := s.msgReader.GetMessagesBefore(ctx, roomID, before, floor, cassrepo.PageRequest{PageSize: pageSize})
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
			// System and deleted messages aren't representative room content — skip to the
			// previous eligible message. Quoted replies ARE eligible (normal user content).
			if m.Deleted || pkgmodel.IsSystemMessageType(m.Type) {
				continue
			}
			return s.toPreviewMessage(ctx, &m), true
		}
		// Whole page ineligible. HasNext=false means the walk reached a terminal
		// state (floor/empty) — stop. Otherwise grow the page and continue strictly
		// before the oldest one seen.
		scanned += len(page.Data)
		if !page.HasNext {
			return models.PreviewMessage{}, false
		}
		before = page.Data[len(page.Data)-1].CreatedAt
		pageSize *= lastMsgWalkGrowth
	}
	return models.PreviewMessage{}, false // ineligible tail longer than the scan budget
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

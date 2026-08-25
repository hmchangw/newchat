package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/mongorepo"
	"github.com/hmchangw/chat/pkg/errcode"
	pkgmodel "github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/preview"
)

const (
	maxRoomsGetBatch       = 100 // mirrors maxGetByIDsBatchSize
	maxRoomsGetConcurrency = 16  // mirrors cassrepo.maxConcurrentIDReads

	// A tiny first page: the common case costs one query; MaxScan keeps the prior 50×5 budget.
	lastMsgWalkFirstPage = 1
	lastMsgWalkGrowth    = 8
	lastMsgWalkMaxScan   = 250
	lastMsgWalkMaxPage   = cassrepo.MaxPageSize

	// Bounds the optional write: it runs before the reply, so a stall can't eat the deadline.
	warmBackTimeout = 2 * time.Second
	// warmBackReserve is the budget left for the reply itself. Below timeout+reserve the
	// warm-back is skipped: it holds one of maxRoomsGetConcurrency slots for its whole
	// run, so on a cold batch the waves compound and would otherwise overrun the request.
	warmBackReserve = 250 * time.Millisecond
)

// RoomsGet returns each room's list preview from the doc, falling back to the walk so a
// site without the eager writer still gets one. Server-to-server, no access check.
func (s *HistoryService) RoomsGet(c *natsrouter.Context, req models.RoomsGetRequest) (*models.RoomsGetResponse, error) {
	if len(req.RoomIDs) == 0 {
		return nil, errcode.BadRequest("roomIds must not be empty")
	}
	if len(req.RoomIDs) > maxRoomsGetBatch {
		return nil, errcode.BadRequest("too many roomIds")
	}
	// Ignored, but an oversized map is malformed input; reject it symmetrically.
	if len(req.Hints) > maxRoomsGetBatch {
		return nil, errcode.BadRequest("too many hints")
	}

	ids := dedupRoomIDs(req.RoomIDs)
	// One read serves both halves: the stored preview, and the walk bounds without one.
	rows, err := s.rooms.GetRoomTimesByIDs(c, ids)
	if err != nil {
		// An error, not an empty map: the client cannot tell that from "no previews".
		return nil, fmt.Errorf("read rooms for preview: %w", err)
	}

	out := make(map[string]models.PreviewMessage, len(ids))
	var lazy []string
	for _, roomID := range ids {
		rt, ok := rows[roomID]
		switch {
		case !ok:
			// Absent from Mongo: no preview, and no room times to bound a walk with.
			continue
		case rt.Preview != nil:
			out[roomID] = *rt.Preview
		default:
			lazy = append(lazy, roomID)
		}
	}
	if len(lazy) > 0 {
		if err := s.fillLazyPreviews(c, lazy, rows, out); err != nil {
			return nil, err
		}
	}
	return &models.RoomsGetResponse{Rooms: out}, nil
}

// fillLazyPreviews walks the rooms the stored path could not serve, in bounded parallel.
// WaitGroup+semaphore over errgroup: one room's failure must not cancel its siblings.
func (s *HistoryService) fillLazyPreviews(
	ctx context.Context,
	ids []string,
	rows map[string]mongorepo.RoomTimes,
	out map[string]models.PreviewMessage,
) error {
	now := time.Now().UTC()
	var mu sync.Mutex
	var wg sync.WaitGroup
	// Acquire before spawning: bounds live goroutines, not just concurrent reads.
	sem := make(chan struct{}, maxRoomsGetConcurrency)
	for _, roomID := range ids {
		// Caller's gone; drain in-flight walks so none outlives the request.
		if err := ctx.Err(); err != nil {
			wg.Wait()
			return err
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			pvw, ok := s.resolvePreview(ctx, roomID, rows[roomID], now)
			if !ok {
				return
			}
			mu.Lock()
			out[roomID] = pvw
			mu.Unlock()
		}()
	}
	wg.Wait()
	// Cancellation after the last worker launched leaves its rooms unresolved, and an
	// omitted room is indistinguishable at the client from one with no preview — the same
	// reason the batched read above errors rather than returning an empty map.
	return ctx.Err()
}

// walkBoundsFromRow derives the walk's bounds from the batched read directly. The row IS
// the room document, so a zero lastMsgAt there is what Mongo says, not an unknown to go
// re-read: routing it through RoomMeta made resolveRoomTimes fetch the same document
// again, once per never-messaged room per request (#291). walkBounds already ceilings a
// zero lastMsgAt at now+skew, which is the same walk that second read produced.
func walkBoundsFromRow(rt mongorepo.RoomTimes) (lastMsgAt, createdAt time.Time) {
	return rt.LastMsgAt, rt.CreatedAt
}

// resolvePreview resolves one room the lazy way, through the cache when installed.
// The cache is positives-only, so empty and degraded walks re-resolve rather than cache.
func (s *HistoryService) resolvePreview(ctx context.Context, roomID string, rt mongorepo.RoomTimes, now time.Time) (models.PreviewMessage, bool) {
	lastMsgAt, createdAt := walkBoundsFromRow(rt)
	load := func(ctx context.Context) (models.PreviewMessage, bool, error) {
		w := s.walkForPreview(ctx, roomID, lastMsgAt, createdAt, now)
		if w.State == previewFound {
			s.warmBackPreview(ctx, roomID, &w, now)
		}
		return w.Preview, w.State == previewFound, nil
	}
	if s.previewCache == nil {
		pvw, ok, _ := load(ctx) // load never errors; the walk reports through state
		return pvw, ok
	}
	pvw, ok, err := s.previewCache.Get(ctx, roomID, load)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get preview cache degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return models.PreviewMessage{}, false
	}
	return pvw, ok
}

// warmBackPreview stores a walk-resolved preview, making an eager-path miss self-healing.
// asOf is walk time: on createdAt, an edited room would reject this write forever.
func (s *HistoryService) warmBackPreview(ctx context.Context, roomID string, w *previewWalk, now time.Time) {
	// No observed id means no key to invalidate against later.
	if w.NewestObservedID == "" {
		return
	}
	// Storing the preview is optional; returning it is not. A skipped warm-back costs one
	// re-walk on a later read, when the budget may be whole — overrunning costs the reply.
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) < warmBackTimeout+warmBackReserve {
		slog.DebugContext(ctx, "room preview warm-back skipped; too little request budget left",
			"room_id", roomID, "request_id", natsutil.RequestIDFromContext(ctx))
		return
	}
	wctx, cancel := context.WithTimeout(ctx, warmBackTimeout)
	defer cancel()
	if err := s.rooms.SetPreviewMessage(wctx, roomID, w.Preview, w.NewestObservedID, now.UnixMilli()); err != nil {
		slog.WarnContext(ctx, "room preview warm-back failed", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
	}
}

// previewResolveState separates previewEmpty (safe to clear) from previewDegraded (unknown).
type previewResolveState int

// previewDegraded is deliberately the zero value: an unpopulated walk must read as
// "unknown, touch nothing", not as a resolved preview with an empty body.
const (
	previewDegraded previewResolveState = iota // a read failed or the scan budget was exhausted — unknown
	previewFound                               // an eligible message was resolved
	previewEmpty                               // walk completed and definitively found no eligible message
	previewSkipped                             // the walk was never run (hidden thread reply) — says nothing
)

// previewWalk is one preview resolution's outcome.
type previewWalk struct {
	Preview models.PreviewMessage
	State   previewResolveState
	// The newest id the walk SAW, and the warm-back key. Not Preview.MessageID (skipped
	// ineligibles would re-walk forever) nor lastMsgId (may name a message Cassandra lacks).
	NewestObservedID string
}

// EventPreview is the mutation event's preview; nil means "none to show", not "clear yours".
func (w *previewWalk) EventPreview() *models.PreviewMessage {
	if w.State != previewFound {
		return nil
	}
	return &w.Preview
}

// roomLastPreviewMessage walks back from lastMsgAt for the latest eligible message.
func (s *HistoryService) roomLastPreviewMessage(ctx context.Context, roomID string, meta *models.RoomMeta, now time.Time) previewWalk {
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(ctx, roomID, meta, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return previewWalk{State: previewDegraded}
	}
	return s.walkForPreview(ctx, roomID, lastMsgAt, createdAt, now)
}

// walkForPreview is roomLastPreviewMessage with the room's times already in hand, so a
// caller that just read the room document does not read it again.
func (s *HistoryService) walkForPreview(ctx context.Context, roomID string, lastMsgAt, createdAt, now time.Time) previewWalk {
	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	// The first row of the first page, eligible or not: the id to invalidate against.
	newestObserved := ""
	pageSize := lastMsgWalkFirstPage
	scanned := 0
	// `before` is the ceiling for every page, never advanced per page. created_at does not
	// identify a row (messages_by_room clusters by created_at AND message_id), so lowering
	// it to the last row's timestamp would re-apply `created_at < ?` and permanently strand
	// that row's same-timestamp siblings — which the walk could then report as previewEmpty
	// and clear. Continuation rides the page cursor instead.
	var cursor *cassrepo.Cursor
	for scanned < lastMsgWalkMaxScan {
		// Clamp to the per-query cap and the remaining budget; ×8 growth starts from here.
		pageSize = min(pageSize, lastMsgWalkMaxPage, lastMsgWalkMaxScan-scanned)
		page, err := s.msgReader.GetMessagesBefore(ctx, roomID, before, floor,
			cassrepo.PageRequest{PageSize: pageSize, Cursor: cursor})
		if err != nil {
			slog.WarnContext(ctx, "rooms.get latest-message read degraded", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return previewWalk{State: previewDegraded}
		}
		if len(page.Data) == 0 {
			return previewWalk{State: previewEmpty, NewestObservedID: newestObserved} // room empty or floor reached
		}
		if newestObserved == "" {
			newestObserved = page.Data[0].MessageID
		}
		for i := range page.Data {
			m := page.Data[i]
			// Shared with the insert-side predicate so the two cannot disagree.
			if !preview.Eligible(m.Deleted, m.Type) {
				continue
			}
			return previewWalk{
				Preview:          s.toPreviewMessage(ctx, &m),
				State:            previewFound,
				NewestObservedID: newestObserved,
			}
		}
		// Whole page ineligible. HasNext=false is terminal; otherwise grow and continue.
		scanned += len(page.Data)
		if !page.HasNext {
			return previewWalk{State: previewEmpty, NewestObservedID: newestObserved}
		}
		// A cursor we cannot decode is unknown territory, not an empty room: continuing
		// without it would silently restart the walk at the ceiling and loop.
		next, err := cassrepo.NewCursor(page.NextCursor)
		if err != nil {
			slog.WarnContext(ctx, "rooms.get latest-message walk cursor undecodable", "room_id", roomID,
				"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
			return previewWalk{State: previewDegraded, NewestObservedID: newestObserved}
		}
		cursor = next
		pageSize *= lastMsgWalkGrowth
	}
	// Ineligible tail past the budget: a survivor may exist, so unknown, never a clear.
	return previewWalk{State: previewDegraded, NewestObservedID: newestObserved}
}

// toPreviewMessage enriches an eligible message into the room-list preview.
func (s *HistoryService) toPreviewMessage(ctx context.Context, m *models.Message) models.PreviewMessage {
	// The walk reads raw blobs; other paths decode via setDecodedAttachments.
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

	// Through preview.Build, as the insert path does: otherwise a room's snippet length
	// would depend on whether its preview last came from an insert or an edit.
	return preview.Build(models.PreviewMessage{
		MessageID:   m.MessageID,
		Sender:      sender,
		Content:     m.Msg,
		CreatedAt:   m.CreatedAt,
		Attachments: m.DecodedAttachments,
		Mentions:    mentions,
		VisibleTo:   m.VisibleTo,
	})
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

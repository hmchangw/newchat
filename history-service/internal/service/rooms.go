package service

import (
	"context"
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
	rows := s.readRoomRows(c, ids)
	metaByRoom := s.resolveRoomMetaHints(ids, req.Hints, rows, now)

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
			lm, ok := s.resolvePreview(c, roomID, metaByRoom[roomID], rows[roomID].Preview, now)
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

// readRoomRows issues the ONE batched room-doc read behind rooms.get. It covers
// every requested room, not just the unhinted ones: the memoized preview and the
// lastMsgId its freshness check compares against both live on the doc, and
// RoomTimeHint carries neither, so a hinted room would otherwise never be able
// to serve a stored preview — which is every room that has messages.
//
// A read failure degrades to an empty map: rooms then keep a nil meta and no
// stored preview, so each falls back to its per-room GetRoomTimes plus a walk.
// It never fails the whole RPC.
func (s *HistoryService) readRoomRows(ctx context.Context, ids []string) map[string]mongorepo.RoomTimes {
	rows, err := s.rooms.GetRoomTimesByIDs(ctx, ids)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get batch room-doc read degraded, falling back per-room",
			"room_count", len(ids), "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return nil
	}
	return rows
}

// resolveRoomMetaHints maps req.Hints into a per-room *models.RoomMeta, falling
// back to the batched room-doc read (rows) for rooms whose hint doesn't resolve
// to a usable lastMsgAt (missing entirely, or rejected by the same
// sanitizeLastMsgAt every per-room resolve uses). A room with neither keeps a
// nil meta, so the per-room resolveRoomTimes falls back to its own GetRoomTimes.
func (s *HistoryService) resolveRoomMetaHints(
	ids []string,
	hints map[string]pkgmodel.RoomTimeHint,
	rows map[string]mongorepo.RoomTimes,
	now time.Time,
) map[string]*models.RoomMeta {
	metaByRoom := make(map[string]*models.RoomMeta, len(ids))
	for _, roomID := range ids {
		if hint, ok := hints[roomID]; ok && sanitizeLastMsgAt(hint.LastMsgAt, now) != nil {
			metaByRoom[roomID] = &models.RoomMeta{LastMsgAt: hint.LastMsgAt, CreatedAt: hint.CreatedAt}
			continue
		}
		rt, ok := rows[roomID]
		if !ok || rt.LastMsgAt.IsZero() {
			// Absent room, or a never-messaged one: nothing usable to hint. Leave
			// the meta nil so resolveRoomTimes takes its plain meta==nil path (one
			// full per-room GetRoomTimes, normalized directly) instead of treating a
			// synthetic zero lastMsgAt as a "hint" and tripping the created>last
			// consistency refetch.
			continue
		}
		last := rt.LastMsgAt.UnixMilli()
		created := rt.CreatedAt.UnixMilli()
		metaByRoom[roomID] = &models.RoomMeta{LastMsgAt: &last, CreatedAt: &created}
	}
	return metaByRoom
}

// resolvePreview resolves one room's preview, serving it from the preview cache
// when installed. The cache is positives-only, so empty rooms and read failures
// fall through to a fresh resolve. meta is the caller/batch-resolved times hint
// (nil when none applies); previewAfterMutation (edit/delete) keeps calling
// roomLastPreviewMessage directly with meta=nil so mutations always see fresh state.
// Both resolve paths (cache-miss loader and no-cache) route through the same load
// closure so a walk that actually runs and finds a preview always warm-backs it;
// a cache HIT never invokes load, so it never warm-backs (nothing new to persist).
func (s *HistoryService) resolvePreview(ctx context.Context, roomID string, meta *models.RoomMeta, stored *models.PreviewMessage, now time.Time) (models.PreviewMessage, bool) {
	// A current stored preview short-circuits the walk entirely — the whole
	// point of the denormalization. The repo has already checked freshness and
	// key epoch and opened it, so a non-nil value here is ready to serve.
	if stored != nil {
		return *stored, true
	}
	load := func(ctx context.Context) (models.PreviewMessage, bool, error) {
		w := s.roomLastPreviewMessage(ctx, roomID, meta, now)
		if w.State == previewFound {
			s.warmBackPreview(ctx, roomID, w.Preview, w.NewestObservedID)
		}
		return w.Preview, w.State == previewFound, nil
	}
	if s.previewCache == nil {
		p, found, _ := load(ctx)
		return p, found
	}
	pvw, ok, err := s.previewCache.Get(ctx, roomID, load)
	if err != nil {
		// ctx cancelled while waiting on a shared load — degrade like a read miss.
		return models.PreviewMessage{}, false
	}
	return pvw, ok
}

// warmBackPreview best-effort persists a walk-resolved preview so subsequent
// reads serve it from the room doc instead of re-walking. asOf is the preview's
// own createdAt millis — conservative by construction (<= any event timestamp
// that observed this message), so a warm-back never outranks a post-mutation
// write. Failures only cost the optimization, never the read: log and continue.
//
// newestObservedID is the freshness key and comes from the walk, never from the
// room doc; see previewWalk.NewestObservedID.
//
//nolint:gocritic // hugeParam: p's by-value shape matches roomLastPreviewMessage's return and RoomRepository.SetPreviewMessage's contract; the copy cost is negligible on this best-effort path.
func (s *HistoryService) warmBackPreview(ctx context.Context, roomID string, p models.PreviewMessage, newestObservedID string) {
	if newestObservedID == "" {
		// Nothing observed means nothing to key freshness on; storing the preview
		// would make it permanently un-invalidatable. Skip rather than guess.
		return
	}
	if err := s.rooms.SetPreviewMessage(ctx, roomID, p, newestObservedID, p.CreatedAt.UnixMilli()); err != nil {
		slog.WarnContext(ctx, "preview warm-back failed", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
	}
}

// previewResolveState distinguishes the two ways the preview walk can come back
// without a message: a completed walk that definitively found none (previewEmpty,
// safe to clear a stored preview) versus one that gave up mid-flight
// (previewDegraded, where a survivor may still exist and clearing would lose data).
type previewResolveState int

const (
	previewFound    previewResolveState = iota
	previewEmpty                        // walk completed and definitively found no eligible message
	previewDegraded                     // a read failed or the scan budget was exhausted — unknown
)

// previewWalk is one preview resolution's outcome.
type previewWalk struct {
	Preview models.PreviewMessage
	// NewestObservedID is the newest message id the walk actually SAW in
	// Cassandra — the freshness key stamped onto a stored preview. Empty when the
	// walk observed nothing.
	//
	// It is deliberately NOT Preview.MessageID: the two differ whenever the newest
	// message is ineligible and skipped, which is the ordinary case for a room
	// whose last activity was a system message. It must also never be taken from
	// the room doc's lastMsgId — broadcast-worker and message-worker are unordered
	// consumers of MESSAGES-CANONICAL, so the doc can name a message Cassandra does
	// not hold yet. Stamping that id would claim freshness for a state never
	// observed, and the identity check would then hold forever, so the resulting
	// stale preview would not self-heal until the next message.
	NewestObservedID string
	State            previewResolveState
}

// roomLastPreviewMessage resolves one room's latest eligible preview message at read time,
// reporting the walk outcome three-valued (see previewResolveState) so callers that must
// tell "definitively none" apart from "unknown" (previewAfterMutation) can.
// meta, when non-nil, is a caller/batch-resolved room-times hint that lets resolveRoomTimes
// skip its own Mongo read (see resolveRoomMetaHints / resolveRoomTimes); pass nil to always
// resolve fresh (previewAfterMutation's contract). Walks backward from lastMsgAt in pages,
// skipping ineligible messages.
func (s *HistoryService) roomLastPreviewMessage(ctx context.Context, roomID string, meta *models.RoomMeta, now time.Time) previewWalk {
	lastMsgAt, createdAt, err := s.resolveRoomTimesOrError(ctx, roomID, meta, now)
	if err != nil {
		slog.WarnContext(ctx, "rooms.get room degraded", "room_id", roomID,
			"request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return previewWalk{State: previewDegraded}
	}

	ceiling, floor := s.walkBounds(lastMsgAt, createdAt, now)
	before := ceiling.Add(time.Millisecond)

	// The walk descends from the ceiling, so the first row of the first non-empty
	// page is the newest message this walk observed.
	newestObservedID := ""

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
			return previewWalk{NewestObservedID: newestObservedID, State: previewDegraded}
		}
		if len(page.Data) == 0 {
			// Room empty or floor reached.
			return previewWalk{NewestObservedID: newestObservedID, State: previewEmpty}
		}
		if newestObservedID == "" {
			newestObservedID = page.Data[0].MessageID
		}
		for i := range page.Data {
			m := page.Data[i]
			// System and deleted messages aren't representative room content — skip to the
			// previous eligible message. Quoted replies ARE eligible (normal user content).
			if m.Deleted || pkgmodel.IsSystemMessageType(m.Type) {
				continue
			}
			return previewWalk{
				Preview:          s.toPreviewMessage(ctx, &m),
				NewestObservedID: newestObservedID,
				State:            previewFound,
			}
		}
		// Whole page ineligible. HasNext=false means the walk reached a terminal
		// state (floor/empty) — stop, definitively none. Otherwise grow the page
		// and continue strictly before the oldest one seen.
		scanned += len(page.Data)
		if !page.HasNext {
			return previewWalk{NewestObservedID: newestObservedID, State: previewEmpty}
		}
		before = page.Data[len(page.Data)-1].CreatedAt
		pageSize *= lastMsgWalkGrowth
	}
	// Ineligible tail longer than the scan budget: an eligible message may still
	// exist beyond it, so this is unknown — never a clear signal.
	return previewWalk{NewestObservedID: newestObservedID, State: previewDegraded}
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

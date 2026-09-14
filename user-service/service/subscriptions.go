package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/mongoutil"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/timeutil"
	"github.com/hmchangw/chat/user-service/models"
)

var validListTypes = map[string]bool{"current": true, "rooms": true, "apps": true}

// ErrShuttingDown is the cancellation cause main attaches when the HTTP drain
// gives up on a still-running handler. It is a server-side abort, so unlike a
// client hang-up the caller is still there to receive — and must not receive a
// partially enriched page dressed as success.
var ErrShuttingDown = errors.New("server is shutting down")

// errTimedOut is the one 503 the list returns when it runs out of budget, so both
// the query and the enrichment report the retryable code the API documents.
func errTimedOut(cause error) error {
	return errcode.Unavailable("subscription list timed out, please retry", errcode.WithCause(cause))
}

// abandoned reports whether ctx died in a way the caller will still observe: a
// deadline, or the shutdown drain. A plain client cancellation is excluded — that
// caller is gone, and turning it into a 503 would log an ERROR per abandoned
// request during exactly the reconnect burst this endpoint serves.
func abandoned(ctx context.Context) error {
	if err := ctx.Err(); err == nil {
		return nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	if cause := context.Cause(ctx); errors.Is(cause, ErrShuttingDown) {
		return cause
	}
	return nil
}

// ListSubscriptions serves the NATS transport; the account comes from the subject.
func (s *UserService) ListSubscriptions(c *natsrouter.Context, req models.SubscriptionListRequest) (*models.PagedSubscriptionListResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	return s.ListSubscriptionsFor(payloadCapped(c), account, req, s.defaultLimit, s.maxSubs)
}

// ListSubscriptionsFor is the transport-neutral core behind subscription.list.
// Page bounds are parameters because HTTP and NATS have different ceilings: the
// NATS reply is capped by the 128 KB payload, an HTTP response is not.
func (s *UserService) ListSubscriptionsFor(ctx context.Context, account string, req models.SubscriptionListRequest, defaultLimit, maxLimit int) (*models.PagedSubscriptionListResponse, error) {
	if !validListTypes[req.Type] {
		return nil, errcode.BadRequest("unknown subscription type")
	}
	if req.UpdatedWithinDays != nil && *req.UpdatedWithinDays < 0 {
		// A negative window computes a FUTURE cutoff and silently returns empty.
		return nil, errcode.BadRequest("updatedWithinDays must be non-negative")
	}
	page := normalizePage(req.Offset, req.Limit, defaultLimit, maxLimit)
	favorite := req.Favorite != nil && *req.Favorite
	// Favorite filtering and the self-DM pin are applied in the query so the page
	// slice and hasMore stay consistent (filtering after slicing would undercount).
	res, err := s.subs.AggregateSubscriptions(ctx, account, req.Type, favorite, req.UpdatedWithinDays, page)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, errTimedOut(err)
		}
		if aborted := abandoned(ctx); aborted != nil {
			return nil, errTimedOut(aborted)
		}
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	withLastMsg := req.IncludeLastMessage == nil || *req.IncludeLastMessage
	s.enrichWithRoomInfoAndLastMsg(ctx, account, res.Data, withLastMsg)
	items := s.buildListItems(ctx, account, res.Data)
	// Every lookup above degrades silently, which is right for a failed RPC and
	// wrong for an abandoned request: the page would return 200 with rooms
	// indistinguishable from ones that no longer exist, and the client would cache a
	// half-empty sidebar. Checked after the app/HR overlays, which degrade the same way.
	if aborted := abandoned(ctx); aborted != nil {
		return nil, errTimedOut(aborted)
	}
	return &models.PagedSubscriptionListResponse{
		Subscriptions: items,
		HasMore:       res.HasMore,
	}, nil
}

// normalizePage clamps the client's offset/limit into a valid page request using
// the supplied bounds (each endpoint passes its own default/max): negative offset
// ⇒ 0; missing/non-positive limit ⇒ defaultLimit; the result is then capped at
// maxLimit, so even a default above the cap cannot exceed it.
func normalizePage(offset, limit, defaultLimit, maxLimit int) mongoutil.OffsetPageRequest {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	return mongoutil.OffsetPageRequest{Offset: int64(offset), Limit: int64(limit)}
}

// buildListItems wraps each enriched subscription into a heterogeneous list row:
//   - channel → base only
//   - botDM   → base + the nested app object; the base name is also swapped to
//     the app's display name (preserving the prior botDM-name behavior)
//   - dm      → base + the counterpart's hrInfo
//
// App and HR lookups degrade independently: a failed/missing lookup keeps the base
// name and omits the app object — it never fails the request.
func (s *UserService) buildListItems(ctx context.Context, account string, subs []model.EnrichedSubscription) []model.SubscriptionItem {
	// One pass over subs yields both name sets the lookups need.
	bots, dmCounterparts := distinctListNames(subs)
	apps := s.lookupApps(ctx, account, bots)
	hrInfo := s.lookupHRInfo(ctx, account, dmCounterparts)
	items := make([]model.SubscriptionItem, len(subs))
	for i := range subs {
		base := &subs[i].Subscription
		switch subs[i].RoomType {
		case model.RoomTypeBotDM:
			botDM := &model.BotDMSubscription{Subscription: base}
			if app, ok := apps[subs[i].Name]; ok && app != nil {
				if app.Name != "" {
					base.Name = app.Name
				}
				botDM.App = model.AppSubscriptionFromApp(app)
			}
			items[i] = botDM
		case model.RoomTypeDM:
			dm := &model.DMSubscription{Subscription: base}
			if hr, ok := hrInfo[subs[i].Name]; ok {
				dm.HRInfo = hr
			}
			items[i] = dm
		default:
			// channel / discussion rows ship the base Subscription unchanged.
			items[i] = &model.ChannelSubscription{Subscription: base}
		}
	}
	return items
}

// lookupApps fetches the full app docs for the given distinct bot accounts; a
// lookup failure degrades to nil (base name kept, no overlay).
func (s *UserService) lookupApps(ctx context.Context, account string, bots []string) map[string]*model.App {
	if len(bots) == 0 {
		return nil
	}
	apps, err := s.apps.GetAppsByAssistants(ctx, bots)
	if err != nil {
		slog.WarnContext(ctx, "app metadata lookup degraded", "account", account, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return nil
	}
	return apps
}

// lookupHRInfo fetches the HR records for the given distinct dm counterpart
// accounts; a lookup failure degrades to nil (no hrInfo).
func (s *UserService) lookupHRInfo(ctx context.Context, account string, accounts []string) map[string]*model.SubscriptionHRInfo {
	if len(accounts) == 0 {
		return nil
	}
	hr, err := s.users.GetHRInfoByAccounts(ctx, accounts)
	if err != nil {
		slog.WarnContext(ctx, "hr info lookup degraded", "account", account, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
		return nil
	}
	return hr
}

// distinctListNames collects, in a single pass, the deduped botDM bot accounts and
// the dm counterpart accounts — the two name sets the app and HR lookups need —
// each in first-seen order.
func distinctListNames(subs []model.EnrichedSubscription) (bots, dmCounterparts []string) {
	seenBot := map[string]struct{}{}
	seenDM := map[string]struct{}{}
	for i := range subs {
		switch subs[i].RoomType {
		case model.RoomTypeBotDM:
			if _, dup := seenBot[subs[i].Name]; !dup {
				seenBot[subs[i].Name] = struct{}{}
				bots = append(bots, subs[i].Name)
			}
		case model.RoomTypeDM:
			if _, dup := seenDM[subs[i].Name]; !dup {
				seenDM[subs[i].Name] = struct{}{}
				dmCounterparts = append(dmCounterparts, subs[i].Name)
			}
		default:
			// channel / discussion rows contribute to neither lookup set.
		}
	}
	return bots, dmCounterparts
}

// enrichWithRoomInfoAndLastMsg populates sub.Room for every subscription and returns
// the surviving slice. LOCAL subs (subs[i].SiteID == s.siteID) are enriched entirely
// from local Mongo — the $lookup baseline plus the room key read from the local
// rooms collection, with NO room-service RPC. Only CROSS-SITE subs fan out to the
// per-site GetRoomsInfo RPC, since their room docs live on another site. When
// withLastMsg, a single grouping also drives one rooms.get per site (all sites incl.
// local) to attach each room's last message.
//
// Enrichment is additive and in place: no row is ever removed. A room the RPC cannot
// resolve (not found, or a degraded site) leaves its sub without a room object.
//
// alert/hasMention are stored subscription state and are never touched here.
func (s *UserService) enrichWithRoomInfoAndLastMsg(ctx context.Context, account string, subs []model.EnrichedSubscription, withLastMsg bool) {
	if len(subs) == 0 {
		return
	}

	// Group by site once — both fan-outs read this grouping. Room info comes from the
	// $lookup baseline for the local site and GetRoomsInfo for the rest; last message
	// is not in the baseline, so its rooms.get runs for ALL sites incl. local.
	// No roomID dedup: the unique (roomId, account) index means one account holds at
	// most one sub per room, so a site's roomIDs are already distinct.
	idxBySite := map[string][]int{}
	for i := range subs {
		idxBySite[subs[i].SiteID] = append(idxBySite[subs[i].SiteID], i)
	}

	s.enrichLocal(subs, idxBySite[s.siteID])
	s.enrichCrossSite(ctx, account, subs, idxBySite)
	if withLastMsg {
		s.enrichLastMessage(ctx, account, subs, idxBySite)
	}
}

// enrichLocal builds sub.Room for LOCAL subs entirely from the $lookup baseline —
// room metadata plus the E2E key projected from the room's encKey sub-document —
// so it needs no separate key store read.
func (s *UserService) enrichLocal(subs []model.EnrichedSubscription, localIdx []int) {
	for _, j := range localIdx {
		room := buildLocalRoom(&subs[j])
		subs[j].Room = room
		// hasUnread / hasGroupMention are computed at read time: room activity (resp.
		// an @all mention) newer than lastSeenAt.
		subs[j].HasUnread = unread(subs[j].LastSeenAt, timeutil.TimeToMillis(timeutil.Coalesce(subs[j].LastUserMsgAt, room.LastMsgAt)))
		subs[j].HasGroupMention = unread(subs[j].LastSeenAt, timeutil.TimeToMillis(room.LastMentionAllAt))
	}
}

// enrichCrossSite fans out per remote site to GetRoomsInfo; a failed site RPC
// leaves that site's subs without a room object (no baseline fallback — there is
// no local room doc for a cross-site room). No sub is ever removed here.
func (s *UserService) enrichCrossSite(ctx context.Context, account string, subs []model.EnrichedSubscription, idxBySite map[string][]int) {
	// The grouping includes the local site (served from the $lookup baseline); skip it here.
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		if site != s.siteID {
			sites = append(sites, site)
		}
	}
	if len(sites) == 0 {
		return
	}
	infoBySite := fanOutChunks(ctx, planChunks(subs, sites, idxBySite, s.roomBatchChunk), len(sites), s.fanout(),
		func(ctx context.Context, job chunkJob) (map[string]model.RoomInfo, error) {
			infos, err := s.rooms.GetRoomsInfo(ctx, job.site, job.roomIDs)
			if err != nil {
				slog.WarnContext(ctx, "room-info enrichment degraded", "account", account, "site", job.site,
					"chunk_size", len(job.roomIDs), "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
				return nil, err
			}
			m := make(map[string]model.RoomInfo, len(infos))
			for k := range infos {
				m[infos[k].RoomID] = infos[k]
			}
			return m, nil
		})

	// A degraded site (m == nil) or a not-found room leaves the sub with no room
	// object but KEPT — a transient RPC failure is indistinguishable from a room
	// that genuinely no longer exists, so neither removes the subscription.
	for i, site := range sites {
		m := infoBySite[i]
		if m == nil {
			continue
		}
		for _, j := range idxBySite[site] {
			info := m[subs[j].RoomID]
			applyRoomInfo(&subs[j].Subscription, &info)
		}
	}
}

// chunkJob is one enrichment RPC. Chunking indices rather than ids keeps rows and
// ids in step, so building a hint map needs no second pass.
type chunkJob struct {
	site    string
	siteIdx int
	rows    []int
	roomIDs []string
}

// planChunks splits each site's rows into batches of at most size. history-service
// hard-rejects over 100 ids or hints and each reply must fit the 128 KB payload, so
// an unsplit page degrades the whole site silently.
func planChunks(subs []model.EnrichedSubscription, sites []string, idxBySite map[string][]int, size int) []chunkJob {
	if size <= 0 {
		size = len(subs)
	}
	var jobs []chunkJob
	for i, site := range sites {
		for rows := range slices.Chunk(idxBySite[site], size) {
			roomIDs := make([]string, len(rows))
			for k, j := range rows {
				roomIDs[k] = subs[j].RoomID
			}
			jobs = append(jobs, chunkJob{site: site, siteIdx: i, rows: rows, roomIDs: roomIDs})
		}
	}
	return jobs
}

// requestableBySite drops the rows whose sub has no Room. The last-message fan-out
// has nothing to attach those previews to and discards them on arrival, so sending
// their ids only crowds the 100-id cap and can split a batch that would otherwise
// fit; a site left with none emits no chunk, which skips its RPC entirely.
//
// ONLY the last-message fan-out may filter this way. Room-info enrichment is what
// POPULATES Room, so applying it there would drop every cross-site sub before the
// RPC that resolves it and leave the whole site unenriched.
func requestableBySite(subs []model.EnrichedSubscription, idxBySite map[string][]int) map[string][]int {
	out := make(map[string][]int, len(idxBySite))
	for site, rows := range idxBySite {
		keep := make([]int, 0, len(rows))
		for _, j := range rows {
			if subs[j].Room == nil {
				continue
			}
			keep = append(keep, j)
		}
		if len(keep) > 0 {
			out[site] = keep
		}
	}
	return out
}

// roomsGetSplitting fetches previews for one chunk, halving and retrying when the
// reply will not fit the transport. A room count cannot bound reply bytes —
// previews carry untruncated message bodies — so even a 100-room chunk can
// overflow, and without this the whole chunk's previews vanish from an otherwise
// successful page. A half that still fails degrades alone.
func (s *UserService) roomsGetSplitting(ctx context.Context, site string, subs []model.EnrichedSubscription, rows []int, roomIDs []string, depth int) (map[string]model.PreviewMessage, error) {
	m, err := s.history.RoomsGet(ctx, site, roomIDs, chunkHints(subs, rows))
	// Truncated at the leaf, so each reply's full-size bodies are collectable the
	// moment its call returns and the merge below holds only short ones.
	s.truncatePreviews(m)
	if err == nil || len(roomIDs) < 2 || !isResponseTooLarge(err) {
		return m, err
	}
	// Recovering data that overflowed history's reply is pointless when our own
	// reply shares the ceiling: the page embeds these previews plus more fields, so
	// it would fail at publish after dozens of extra RPCs. Clients retry over HTTP.
	if isPayloadCapped(ctx) {
		return nil, err
	}
	// Halving is exponential in RPCs — 20 KB bodies need ~6-room batches, so an
	// unbounded recursion turns one page into hundreds of calls. Past the cap the
	// chunk degrades like any other.
	if depth >= maxSplitDepth {
		return nil, err
	}

	mid := len(roomIDs) / 2
	left, lErr := s.roomsGetSplitting(ctx, site, subs, rows[:mid], roomIDs[:mid], depth+1)
	right, rErr := s.roomsGetSplitting(ctx, site, subs, rows[mid:], roomIDs[mid:], depth+1)
	if lErr != nil && rErr != nil {
		return nil, lErr
	}
	// Logged per branch: a sibling's success would otherwise return a nil error and
	// hide half a chunk going missing from an apparently complete page.
	for _, e := range []error{lErr, rErr} {
		if e != nil {
			slog.WarnContext(ctx, "split branch degraded", "site", site, "chunk_size", len(roomIDs),
				"request_id", natsutil.RequestIDFromContext(ctx), "error", e)
		}
	}
	// Partial success is kept: losing half the previews beats losing all of them.
	merged := make(map[string]model.PreviewMessage, len(left)+len(right))
	maps.Copy(merged, left)
	maps.Copy(merged, right)
	return merged, nil
}

// maxSplitDepth bounds the halving recursion: 100 rooms reach ~6 per batch, which
// fits 128 KB even at the 20 KB body ceiling, for at most 31 RPCs per chunk.
const maxSplitDepth = 4

type payloadCappedKey struct{}

// payloadCapped marks a transport whose own reply carries the same size ceiling
// history hit, so splitting cannot produce a deliverable response.
func payloadCapped(ctx context.Context) context.Context {
	return context.WithValue(ctx, payloadCappedKey{}, true)
}

// isPayloadCapped reports whether the caller's transport shares history's size
// ceiling, in which case recovering an oversized reply cannot help.
func isPayloadCapped(ctx context.Context) bool {
	v, _ := ctx.Value(payloadCappedKey{}).(bool)
	return v
}

// isResponseTooLarge reports whether the reply was refused for exceeding the
// transport payload cap, the one enrichment failure a smaller batch can fix.
func isResponseTooLarge(err error) bool {
	var e *errcode.Error
	return errors.As(err, &e) && e.Reason == errcode.ResponseTooLarge
}

// truncatePreviews caps each preview body at previewChars runes, in place.
func (s *UserService) truncatePreviews(m map[string]model.PreviewMessage) {
	n := s.previewChars
	if n <= 0 {
		return
	}
	// Indexed rather than ranged over values: a PreviewMessage is 200+ bytes, and a
	// body already inside the limit needs no copy at all.
	for k := range m {
		content := m[k].Content
		cut := truncateRunes(content, n)
		if len(cut) == len(content) {
			continue
		}
		pm := m[k]
		// Clone, because a slice of a string keeps the whole backing array alive:
		// without it every 50-rune preview would pin its original 20 KB body and
		// the truncation would free nothing.
		pm.Content = strings.Clone(cut)
		m[k] = pm
	}
}

// truncateRunes cuts str to at most n runes. Runes, not bytes: a byte cut would
// split a multi-byte character and put invalid UTF-8 on the wire. The result
// aliases str — callers that retain it must clone.
func truncateRunes(str string, n int) string {
	// A string of n bytes holds at most n runes, so this is the ASCII fast path.
	if len(str) <= n {
		return str
	}
	count := 0
	for i := range str {
		if count == n {
			return str[:i]
		}
		count++
	}
	return str
}

// chunkHints returns the walk bounds for this chunk's rows, letting
// history-service skip its own room-times read. Scoped to the chunk because
// history-service caps hints at the same 100 as room ids.
func chunkHints(subs []model.EnrichedSubscription, rows []int) map[string]model.RoomTimeHint {
	hints := make(map[string]model.RoomTimeHint, len(rows))
	for _, j := range rows {
		if subs[j].Room == nil || subs[j].Room.LastMsgAt == nil {
			continue
		}
		hints[subs[j].RoomID] = model.RoomTimeHint{LastMsgAt: timeutil.TimeToMillis(subs[j].Room.LastMsgAt)}
	}
	return hints
}

// fanOutChunks runs call once per chunk, maxSiteFanout at a time, merged per site.
// A site's map is nil only when every one of its chunks failed, so "site degraded"
// stays distinguishable from "room absent" and one failed chunk costs only its own
// rooms. Each chunk writes its own slot, so the merge needs no lock.
func fanOutChunks[T any](ctx context.Context, jobs []chunkJob, sites, maxFanout int, call func(context.Context, chunkJob) (map[string]T, error)) []map[string]T {
	results := make([]map[string]T, len(jobs))
	// WaitGroup, not errgroup: errgroup.WithContext cancels siblings on the first
	// error, and per-chunk degradation must leave the others running.
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxFanout)
	for i, job := range jobs {
		// Client already gone — stop firing further ~5s RPCs; the rest would only
		// waste round-trips. In-flight calls fail fast via the ctx we pass down.
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		// Acquire before spawning so the live goroutine COUNT, not just the
		// concurrency, stays within the bound.
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			// Re-check after parking on the semaphore: cancellation may have landed
			// while this goroutine waited its turn.
			if ctx.Err() != nil {
				return
			}
			if m, err := call(ctx, job); err == nil {
				results[i] = m
			}
		}()
	}
	wg.Wait()

	bySite := make([]map[string]T, sites)
	for i, job := range jobs {
		if results[i] == nil {
			continue
		}
		if bySite[job.siteIdx] == nil {
			bySite[job.siteIdx] = make(map[string]T, len(results[i]))
		}
		maps.Copy(bySite[job.siteIdx], results[i])
	}
	return bySite
}

// enrichLastMessage populates sub.Room.PreviewMessage (read-time resolve, no
// denormalized write path) via rooms.get — LOCAL subs need it too, since
// last-message is not part of the $lookup baseline. Reuses the caller's per-site
// grouping, split into chunks of roomBatchChunk: history-service hard-rejects a
// batch over 100 ids, and the reply must fit the 128 KB NATS payload. A degraded
// chunk, or a room the RPC omits, just leaves PreviewMessage nil; it never fails
// the list.
//
// Each room already carrying a resolved sub.Room.LastMsgAt (set by enrichLocal /
// enrichCrossSite, which both run first) is passed as a hint so history-service
// can skip its own room-times read; rooms with no Room contribute no hint.
func (s *UserService) enrichLastMessage(ctx context.Context, account string, subs []model.EnrichedSubscription, idxBySite map[string][]int) {
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		sites = append(sites, site)
	}
	lastMsgBySite := fanOutChunks(ctx, planChunks(subs, sites, requestableBySite(subs, idxBySite), s.roomBatchChunk), len(sites), s.fanout(),
		func(ctx context.Context, job chunkJob) (map[string]model.PreviewMessage, error) {
			m, err := s.roomsGetSplitting(ctx, job.site, subs, job.rows, job.roomIDs, 0)
			if err != nil {
				slog.WarnContext(ctx, "last-message enrichment degraded", "account", account, "site", job.site,
					"chunk_size", len(job.roomIDs), "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
				return nil, err
			}
			return m, nil
		})

	for i, site := range sites {
		m := lastMsgBySite[i]
		if m == nil {
			continue
		}
		for _, j := range idxBySite[site] {
			// An unresolved room (Room==nil) has nothing to attach a last message to.
			if subs[j].Room == nil {
				continue
			}
			lm, ok := m[subs[j].RoomID]
			if !ok {
				continue
			}
			subs[j].Room.PreviewMessage = &lm
		}
	}
}

// roomKeySecretLen is the AES-256-GCM key length. A baseline encKeyPriv of any
// other length is treated as absent (mirrors roomkeystore's secret validation).
const roomKeySecretLen = 32

// buildLocalRoom builds a SubscriptionRoom for a LOCAL sub entirely from its flat
// $lookup baseline — room metadata plus the E2E key projected from the room's
// encKey sub-document — so no separate key store read is needed. The baseline and
// the wire room object both carry *time.Time, so LastMsgAt/LastMentionAllAt pass
// through unconverted.
func buildLocalRoom(sub *model.EnrichedSubscription) *model.SubscriptionRoom {
	room := &model.SubscriptionRoom{
		SiteID:            sub.SiteID,
		Name:              sub.RoomName,
		CrossSite:         sub.CrossSite,
		UserCount:         sub.UserCount,
		AppCount:          sub.AppCount,
		LastMsgAt:         timeutil.Coalesce(sub.LastUserMsgAt, sub.LastMsgAt),
		LastMsgID:         sub.LastMsgID,
		LastMentionAllAt:  sub.LastMentionAllAt,
		MinUserLastSeenAt: sub.MinUserLastSeenAt,
	}
	if len(sub.RoomKeyPriv) == roomKeySecretLen {
		enc := base64.StdEncoding.EncodeToString(sub.RoomKeyPriv)
		ver := sub.RoomKeyVer
		room.PrivateKey = &enc
		room.KeyVersion = &ver
	}
	return room
}

// applyRoomInfo nests all room-derived fields (including the E2E key for initial
// key bootstrap) under sub.Room; zero-value info (Found=false) is skipped. The
// subscription's own fields are never overwritten — name, alert, and hasMention
// are authoritative subscription state; room-service only supplies room data.
//
// A not-found or degraded room is left with no room object; the subscription is
// kept either way.
func applyRoomInfo(sub *model.Subscription, info *model.RoomInfo) {
	if !info.Found {
		return
	}
	// info.LastMsgAt/LastMentionAllAt arrive from the RPC as epoch millis (*int64);
	// the wire room object returns RFC3339 timestamps, so convert them here.
	room := &model.SubscriptionRoom{
		SiteID:            info.SiteID,
		Name:              info.Name,
		CrossSite:         info.CrossSite,
		UserCount:         info.UserCount,
		AppCount:          info.AppCount,
		LastMsgAt:         timeutil.MillisToTime(timeutil.Coalesce(info.LastUserMsgAt, info.LastMsgAt)),
		LastMsgID:         info.LastMsgID,
		LastMentionAllAt:  timeutil.MillisToTime(info.LastMentionAllAt),
		MinUserLastSeenAt: timeutil.MillisToTime(info.MinUserLastSeenAt),
		PrivateKey:        info.PrivateKey,
		KeyVersion:        info.KeyVersion,
	}
	sub.Room = room
	// hasUnread / hasGroupMention are computed at read time from the room's
	// last-message / last-@all-mention time vs lastSeenAt.
	sub.HasUnread = unread(sub.LastSeenAt, timeutil.Coalesce(info.LastUserMsgAt, info.LastMsgAt))
	sub.HasGroupMention = unread(sub.LastSeenAt, info.LastMentionAllAt)
}

// unread: a room event at ms (epoch millis) is newer than lastSeen; nil ms ⇒ false, nil lastSeen with ms set ⇒ true.
func unread(lastSeen *time.Time, ms *int64) bool {
	if ms == nil {
		return false
	}
	if lastSeen == nil {
		return true
	}
	return lastSeen.UTC().UnixMilli() < *ms
}

// GetChannels lists channel subscriptions; DMs come back from GetDM instead.
func (s *UserService) GetChannels(c *natsrouter.Context, req models.GetChannelsRequest) (*models.PagedSubscriptionListResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	hasContain, hasNames := req.MembersContain != "", len(req.AccountNames) > 0
	if hasContain == hasNames {
		return nil, errcode.BadRequest("exactly one of membersContain or accountNames is required")
	}
	// maxAccountNames caps getChannels' accountNames — unbounded input builds an arbitrarily large $in/$setIsSubset operand.
	if len(req.AccountNames) > s.maxAccountNames {
		return nil, errcode.BadRequest("too many accountNames")
	}
	members := req.AccountNames
	if hasContain {
		members = []string{req.MembersContain}
	}
	page := normalizePage(req.Offset, req.Limit, s.defaultLimit, s.maxSubs)
	res, err := s.subs.FindChannelsByMembers(c, account, members, page)
	if err != nil {
		return nil, fmt.Errorf("get channels: %w", err)
	}
	s.enrichWithRoomInfoAndLastMsg(c, account, res.Data, false)
	items := s.buildListItems(c, account, res.Data)
	return &models.PagedSubscriptionListResponse{
		Subscriptions: items,
		HasMore:       res.HasMore,
	}, nil
}

// GetDM resolves one DM room, whose id is derived from the two accounts.
func (s *UserService) GetDM(c *natsrouter.Context, req models.GetDMRequest) (*models.DMResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account, "target", req.AccountName)
	if req.AccountName == "" {
		return nil, errcode.BadRequest("accountName required")
	}
	// A DM counterpart may be any account — an ordinary user, a bot, or the
	// platform-admin pseudo-account — since all of them can log into the chat
	// frontend and hold a DM subscription.
	dm, err := s.subs.GetDMSubscription(c, account, req.AccountName)
	if err != nil {
		return nil, fmt.Errorf("get dm: %w", err)
	}
	if dm == nil {
		return nil, errcode.NotFound("dm not found", errcode.WithReason(errcode.UserSubscriptionNotFound))
	}
	// The wire DMSubscription points at the boxed stored sub plus HRInfo; a room the
	// remote site cannot resolve simply arrives without a room object.
	one := []model.EnrichedSubscription{dm.EnrichedSubscription}
	s.enrichWithRoomInfoAndLastMsg(c, account, one, false)
	return &models.DMResponse{Subscription: model.DMSubscription{
		Subscription: &one[0].Subscription,
		HRInfo:       dm.HRInfo,
	}}, nil
}

// GetByRoomID returns the caller's room-info-enriched subscription for req.RoomID
// as a 0-or-1-element list (empty = not subscribed; absence is a normal answer).
func (s *UserService) GetByRoomID(c *natsrouter.Context, req models.GetByRoomIDRequest) (*models.SubscriptionListResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account, "roomId", req.RoomID)
	if req.RoomID == "" {
		return nil, errcode.BadRequest("roomId required")
	}
	sub, err := s.subs.GetSubscriptionByRoomID(c, account, req.RoomID)
	if err != nil {
		return nil, fmt.Errorf("get subscription by roomId: %w", err)
	}
	if sub == nil {
		return &models.SubscriptionListResponse{Subscriptions: []model.SubscriptionItem{}, Total: 0}, nil
	}
	one := []model.EnrichedSubscription{*sub}
	s.enrichWithRoomInfoAndLastMsg(c, account, one, false)
	items := s.buildListItems(c, account, one)
	return &models.SubscriptionListResponse{Subscriptions: items, Total: len(items)}, nil
}

func (s *UserService) CountSubscriptions(c *natsrouter.Context, req models.CountRequest) (*models.CountResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	return s.CountSubscriptionsFor(c, account, req)
}

// CountSubscriptionsFor is the transport-agnostic count both the NATS handler
// and the HTTP endpoint share. Unread nil/false ⇒ total active subs; true ⇒
// the unread-badge count (cache-first when gated).
func (s *UserService) CountSubscriptionsFor(ctx context.Context, account string, req models.CountRequest) (*models.CountResponse, error) {
	if req.Unread == nil || !*req.Unread {
		total, err := s.subs.CountActiveSubscriptions(ctx, account)
		if err != nil {
			return nil, fmt.Errorf("count subscriptions: %w", err)
		}
		return &models.CountResponse{Count: total}, nil
	}
	// Cache-first (gated): serve the badge set's size on freshness-marker hit;
	// miss/stale falls through to the Mongo compute, whose Reseed rewrites the
	// set and marker.
	if s.badgeCacheFirst {
		if n, fresh := s.badge.Count(ctx, account); fresh {
			return &models.CountResponse{Count: n}, nil
		}
	}
	ids, degraded, err := s.unreadRooms(ctx, account)
	if err != nil {
		return nil, err
	}
	// Best-effort reconciliation from the Mongo source of truth (fail-open) —
	// skipped when degraded, since caching a partial set would stamp the
	// freshness marker on data we already know is incomplete.
	if !degraded {
		s.badge.Reseed(ctx, account, ids)
	}
	return &models.CountResponse{Count: len(ids)}, nil
}

// unreadRooms returns the account's active room IDs with unread activity, and
// whether the result is degraded — true when at least one cross-site
// GetRoomsMeta RPC failed (or was skipped because the client disconnected)
// and that site's rooms were dropped. A degraded result is still returned
// (best-effort, as before) but must not be cached: writing it would stamp
// the freshness marker on a knowingly-incomplete set.
// Local subs read lastMsgAt from the $lookup; cross-site subs fetch it via
// per-site GetRoomsMeta (rooms the remote site cannot resolve are excluded).
func (s *UserService) unreadRooms(ctx context.Context, account string) ([]string, bool, error) {
	subs, err := s.subs.GetActiveSubscriptions(ctx, account, s.maxSubs)
	if err != nil {
		return nil, false, fmt.Errorf("unread rooms: %w", err)
	}

	var ids []string
	degraded := false
	crossBySite := map[string][]models.ActiveSubscription{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(timeutil.Coalesce(subs[i].LastUserMsgAt, subs[i].LastMsgAt))) || len(subs[i].ThreadUnread) > 0 {
				ids = append(ids, subs[i].RoomID)
			}
			continue
		}
		site := subs[i].SiteID
		crossBySite[site] = append(crossBySite[site], subs[i])
		roomIDsBySite[site] = append(roomIDsBySite[site], subs[i].RoomID)
	}

	if len(crossBySite) > 0 {
		sites := make([]string, 0, len(crossBySite))
		for site := range crossBySite {
			sites = append(sites, site)
		}
		// Per-site degradation (matches the list path's enrichCrossSite): a failed site is
		// SKIPPED — its subs drop out of the result — while local subs and the sites that
		// did respond still contribute. results[i] and failed[i] are each written by
		// exactly one goroutine (or, for the break path below, by the launching loop
		// itself before any goroutine for that index exists).
		results := make([][]string, len(sites))
		failed := make([]bool, len(sites))
		var wg sync.WaitGroup
		sem := make(chan struct{}, s.fanout()) // bound concurrent per-site RPCs
		for i, site := range sites {
			// Client already gone — stop firing further ~5s RPCs. The remaining sites'
			// rooms will never be counted, so mark them (and this one) degraded rather
			// than let a cancelled request be cached as if it were complete.
			if ctx.Err() != nil {
				for j := i; j < len(sites); j++ {
					failed[j] = true
				}
				break
			}
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if ctx.Err() != nil {
					failed[i] = true
					return
				}
				infos, err := s.rooms.GetRoomsMeta(ctx, site, roomIDsBySite[site])
				if err != nil {
					// Skip this site rather than nuking the whole result.
					slog.WarnContext(ctx, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
					failed[i] = true
					return
				}
				lastMsg := make(map[string]*int64, len(infos))
				for k := range infos {
					// Mirror the list path (applyRoomInfo): a room the remote site cannot
					// resolve contributes nothing.
					if !infos[k].Found {
						continue
					}
					lastMsg[infos[k].RoomID] = timeutil.Coalesce(infos[k].LastUserMsgAt, infos[k].LastMsgAt)
				}
				var res []string
				siteSubs := crossBySite[site]
				for j := range siteSubs {
					rid := siteSubs[j].RoomID
					// Not-found rooms are absent from lastMsg — not counted.
					ms, ok := lastMsg[rid]
					if !ok {
						continue
					}
					if unread(siteSubs[j].LastSeenAt, ms) || len(siteSubs[j].ThreadUnread) > 0 {
						res = append(res, rid)
					}
				}
				results[i] = res
			}()
		}
		wg.Wait()
		for i := range results {
			ids = append(ids, results[i]...)
			if failed[i] {
				degraded = true
			}
		}
	}
	return ids, degraded, nil
}

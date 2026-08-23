package service

import (
	"encoding/base64"
	"fmt"
	"log/slog"
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

// maxSiteFanout bounds concurrent per-site room-service RPCs — otherwise a
// heavily-federated ALL_SITE_IDS fans one request into N simultaneous 5s RPCs.
const maxSiteFanout = 8

func (s *UserService) ListSubscriptions(c *natsrouter.Context, req models.SubscriptionListRequest) (*models.PagedSubscriptionListResponse, error) {
	if !validListTypes[req.Type] {
		return nil, errcode.BadRequest("unknown subscription type")
	}
	if req.UpdatedWithinDays != nil && *req.UpdatedWithinDays < 0 {
		// A negative window computes a FUTURE cutoff and silently returns empty.
		return nil, errcode.BadRequest("updatedWithinDays must be non-negative")
	}
	account := c.Param("account")
	c.WithLogValues("account", account)
	page := normalizePage(req.Offset, req.Limit, s.defaultLimit, s.maxSubs)
	favorite := req.Favorite != nil && *req.Favorite
	// Favorite filtering and the self-DM pin are applied in the query so the page
	// slice and hasMore stay consistent (filtering after slicing would undercount).
	res, err := s.subs.AggregateSubscriptions(c, account, req.Type, favorite, req.UpdatedWithinDays, page)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	withLastMsg := req.IncludeLastMessage == nil || *req.IncludeLastMessage
	s.enrichWithRoomInfoAndLastMsg(c, res.Data, withLastMsg)
	items := s.buildListItems(c, res.Data)
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
func (s *UserService) buildListItems(c *natsrouter.Context, subs []model.EnrichedSubscription) []model.SubscriptionItem {
	// One pass over subs yields both name sets the lookups need.
	bots, dmCounterparts := distinctListNames(subs)
	apps := s.lookupApps(c, bots)
	hrInfo := s.lookupHRInfo(c, dmCounterparts)
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
func (s *UserService) lookupApps(c *natsrouter.Context, bots []string) map[string]*model.App {
	if len(bots) == 0 {
		return nil
	}
	apps, err := s.apps.GetAppsByAssistants(c, bots)
	if err != nil {
		slog.WarnContext(c, "app metadata lookup degraded", "account", c.Param("account"), "request_id", natsutil.RequestIDFromContext(c), "error", err)
		return nil
	}
	return apps
}

// lookupHRInfo fetches the HR records for the given distinct dm counterpart
// accounts; a lookup failure degrades to nil (no hrInfo).
func (s *UserService) lookupHRInfo(c *natsrouter.Context, accounts []string) map[string]*model.SubscriptionHRInfo {
	if len(accounts) == 0 {
		return nil
	}
	hr, err := s.users.GetHRInfoByAccounts(c, accounts)
	if err != nil {
		slog.WarnContext(c, "hr info lookup degraded", "account", c.Param("account"), "request_id", natsutil.RequestIDFromContext(c), "error", err)
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
func (s *UserService) enrichWithRoomInfoAndLastMsg(c *natsrouter.Context, subs []model.EnrichedSubscription, withLastMsg bool) {
	if len(subs) == 0 {
		return
	}

	// Group by site once — both fan-outs read this grouping. Room info comes from the
	// $lookup baseline for the local site and GetRoomsInfo for the rest; last message
	// is not in the baseline, so its rooms.get runs for ALL sites incl. local.
	// No roomID dedup: the unique (roomId, account) index means one account holds at
	// most one sub per room, so a site's roomIDs are already distinct.
	idxBySite := map[string][]int{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		site := subs[i].SiteID
		idxBySite[site] = append(idxBySite[site], i)
		roomIDsBySite[site] = append(roomIDsBySite[site], subs[i].RoomID)
	}

	s.enrichLocal(subs, idxBySite[s.siteID])
	s.enrichCrossSite(c, subs, idxBySite, roomIDsBySite)
	if withLastMsg {
		s.enrichLastMessage(c, subs, idxBySite, roomIDsBySite)
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
		subs[j].HasUnread = unread(subs[j].LastSeenAt, timeutil.TimeToMillis(room.LastMsgAt))
		subs[j].HasGroupMention = unread(subs[j].LastSeenAt, timeutil.TimeToMillis(room.LastMentionAllAt))
	}
}

// enrichCrossSite fans out per remote site to GetRoomsInfo; a failed site RPC
// leaves that site's subs without a room object (no baseline fallback — there is
// no local room doc for a cross-site room). No sub is ever removed here.
func (s *UserService) enrichCrossSite(c *natsrouter.Context, subs []model.EnrichedSubscription, idxBySite map[string][]int, roomIDsBySite map[string][]string) {
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
	infoBySite := make([]map[string]model.RoomInfo, len(sites)) // nil ⇒ site degraded
	// WaitGroup (not errgroup): errgroup.WithContext would cancel sibling site RPCs on the first error; per-site degradation must keep siblings running.
	// Acquire sem BEFORE spawning so live goroutine COUNT (not just concurrency) stays ≤ maxSiteFanout — a wide federation otherwise spawns one parked goroutine per site.
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for i, site := range sites {
		// Client already gone — stop firing further ~5s RPCs; the remaining sites
		// would only waste round-trips. In-flight calls fail fast via the ctx we
		// pass to GetRoomsInfo.
		if c.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			// Re-check after parking on the semaphore: cancellation may have
			// landed while this goroutine waited its turn behind earlier RPCs.
			if c.Err() != nil {
				return
			}
			infos, err := s.rooms.GetRoomsInfo(c, site, roomIDsBySite[site])
			if err != nil {
				slog.WarnContext(c, "room-info enrichment degraded", "account", c.Param("account"), "site", site, "request_id", natsutil.RequestIDFromContext(c), "error", err)
				return
			}
			m := make(map[string]model.RoomInfo, len(infos))
			for k := range infos {
				m[infos[k].RoomID] = infos[k]
			}
			infoBySite[i] = m
		}()
	}
	wg.Wait()
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

// enrichLastMessage populates sub.Room.PreviewMessage (read-time resolve, no denormalized
// write path) via one rooms.get RPC per site — LOCAL subs need it too (last-message
// isn't part of the $lookup baseline). One call per site: a subscription page is
// bounded well under history-service's 100-roomId batch cap, so no chunk-split is
// needed. Reuses the caller's per-site grouping. A degraded/absent site, or a room the
// RPC omits, just leaves PreviewMessage nil; it never fails the list.
// Each room already carrying a resolved sub.Room.LastMsgAt (set by enrichLocal/
// enrichCrossSite, which both run before this) is passed as a hint so
// history-service can skip its own room-times read for that room; rooms with no
// Room (not found, or a degraded site) contribute no hint.
func (s *UserService) enrichLastMessage(c *natsrouter.Context, subs []model.EnrichedSubscription, idxBySite map[string][]int, roomIDsBySite map[string][]string) {
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		sites = append(sites, site)
	}
	lastMsgBySite := make([]map[string]model.PreviewMessage, len(sites)) // nil ⇒ site degraded
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for i, site := range sites {
		if c.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if c.Err() != nil {
				return
			}
			hints := map[string]model.RoomTimeHint{}
			for _, j := range idxBySite[site] {
				if subs[j].Room == nil || subs[j].Room.LastMsgAt == nil {
					continue
				}
				hints[subs[j].RoomID] = model.RoomTimeHint{LastMsgAt: timeutil.TimeToMillis(subs[j].Room.LastMsgAt)}
			}
			m, err := s.history.RoomsGet(c, site, roomIDsBySite[site], hints)
			if err != nil {
				slog.WarnContext(c, "last-message enrichment degraded", "account", c.Param("account"), "site", site, "request_id", natsutil.RequestIDFromContext(c), "error", err)
				return
			}
			lastMsgBySite[i] = m
		}()
	}
	wg.Wait()
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
		LastMsgAt:         sub.LastMsgAt,
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
		LastMsgAt:         timeutil.MillisToTime(info.LastMsgAt),
		LastMsgID:         info.LastMsgID,
		LastMentionAllAt:  timeutil.MillisToTime(info.LastMentionAllAt),
		MinUserLastSeenAt: timeutil.MillisToTime(info.MinUserLastSeenAt),
		PrivateKey:        info.PrivateKey,
		KeyVersion:        info.KeyVersion,
	}
	sub.Room = room
	// hasUnread / hasGroupMention are computed at read time from the room's
	// last-message / last-@all-mention time vs lastSeenAt.
	sub.HasUnread = unread(sub.LastSeenAt, info.LastMsgAt)
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
	s.enrichWithRoomInfoAndLastMsg(c, res.Data, false)
	items := s.buildListItems(c, res.Data)
	return &models.PagedSubscriptionListResponse{
		Subscriptions: items,
		HasMore:       res.HasMore,
	}, nil
}

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
	s.enrichWithRoomInfoAndLastMsg(c, one, false)
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
	s.enrichWithRoomInfoAndLastMsg(c, one, false)
	items := s.buildListItems(c, one)
	return &models.SubscriptionListResponse{Subscriptions: items, Total: len(items)}, nil
}

func (s *UserService) CountSubscriptions(c *natsrouter.Context, req models.CountRequest) (*models.CountResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	if req.Unread == nil || !*req.Unread {
		total, err := s.subs.CountActiveSubscriptions(c, account)
		if err != nil {
			return nil, fmt.Errorf("count subscriptions: %w", err)
		}
		return &models.CountResponse{Count: total}, nil
	}
	// Cache-first (gated): serve the badge set's size on freshness-marker hit;
	// miss/stale falls through to the Mongo compute, whose Reseed rewrites the
	// set and marker.
	if s.badgeCacheFirst {
		if n, fresh := s.badge.Count(c, account); fresh {
			return &models.CountResponse{Count: n}, nil
		}
	}
	ids, degraded, err := s.unreadRooms(c, account)
	if err != nil {
		return nil, err
	}
	// Best-effort reconciliation from the Mongo source of truth (fail-open) —
	// skipped when degraded, since caching a partial set would stamp the
	// freshness marker on data we already know is incomplete.
	if !degraded {
		s.badge.Reseed(c, account, ids)
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
func (s *UserService) unreadRooms(c *natsrouter.Context, account string) ([]string, bool, error) {
	subs, err := s.subs.GetActiveSubscriptions(c, account, s.maxSubs)
	if err != nil {
		return nil, false, fmt.Errorf("unread rooms: %w", err)
	}

	var ids []string
	degraded := false
	crossBySite := map[string][]model.EnrichedSubscription{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(subs[i].LastMsgAt)) || len(subs[i].ThreadUnread) > 0 {
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
		sem := make(chan struct{}, maxSiteFanout) // bound concurrent per-site RPCs
		for i, site := range sites {
			// Client already gone — stop firing further ~5s RPCs. The remaining sites'
			// rooms will never be counted, so mark them (and this one) degraded rather
			// than let a cancelled request be cached as if it were complete.
			if c.Err() != nil {
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
				if c.Err() != nil {
					failed[i] = true
					return
				}
				infos, err := s.rooms.GetRoomsMeta(c, site, roomIDsBySite[site])
				if err != nil {
					// Skip this site rather than nuking the whole result.
					slog.WarnContext(c, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(c), "error", err)
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
					lastMsg[infos[k].RoomID] = infos[k].LastMsgAt
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

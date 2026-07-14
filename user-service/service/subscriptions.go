package service

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
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

// maxSiteFanout bounds concurrent per-site room-service RPCs — otherwise a
// heavily-federated ALL_SITE_IDS fans one request into N simultaneous 5s RPCs.
const maxSiteFanout = 8

// DM-target markers rejected by GetDM: platform/system accounts are prefixed
// "p_" and bot accounts end in ".bot" — neither is a valid human DM counterpart.
const (
	dmTargetSystemPrefix = "p_"
	dmTargetBotSuffix    = ".bot"
)

// deletedRoomNamePrefix marks a soft-deleted room (room-service renames it to
// "Del-"+name); such rooms are surfaced on the subscription with no room object.
const deletedRoomNamePrefix = "Del-"

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
	res.Data = s.enrichWithRoomInfo(c, res.Data, true)
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

// enrichWithRoomInfo populates sub.Room for every subscription and returns the
// surviving slice. LOCAL subs (subs[i].SiteID == s.siteID) are enriched entirely
// from local Mongo — the $lookup baseline plus the room key read from the local
// rooms collection, with NO room-service RPC. Only CROSS-SITE subs fan out to the
// per-site GetRoomsInfo RPC, since their room docs live on another site.
//
// dropDeleted controls how a soft-deleted ("Del-") room is handled, mirroring the
// Mongo deleted-filter so LOCAL and CROSS-SITE rooms behave identically:
//   - true  (list/count paths): a cross-site Del- sub is DROPPED, just as the query
//     already drops local Del- subs there.
//   - false (single-item getDM/getByRoomID): a cross-site Del- sub is KEPT with no
//     room object, just as those lookups keep a local Del- sub room-nulled.
//
// A LOCAL Del- sub is never dropped here regardless of the flag: list paths never
// see one (the query removed it), and single-item paths null its room via
// enrichLocal. Callers MUST use the returned slice, not the input.
//
// alert/hasMention are stored subscription state and are never touched here.
func (s *UserService) enrichWithRoomInfo(c *natsrouter.Context, subs []model.EnrichedSubscription, dropDeleted bool) []model.EnrichedSubscription {
	if len(subs) == 0 {
		return subs
	}

	// Partition by locality, building each remote site's roomID list directly here.
	// No roomID dedup: the unique (roomId, account) index means one account holds at
	// most one sub per room, so a site's roomIDs are already distinct.
	var localIdx []int
	idxBySite := map[string][]int{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			localIdx = append(localIdx, i)
			continue
		}
		site := subs[i].SiteID
		idxBySite[site] = append(idxBySite[site], i)
		roomIDsBySite[site] = append(roomIDsBySite[site], subs[i].RoomID)
	}

	s.enrichLocal(subs, localIdx)
	dropped := s.enrichCrossSite(c, subs, idxBySite, roomIDsBySite)
	// Single-item lookups (dropDeleted=false) keep a cross-site Del- sub room-less;
	// only the list/count paths remove it.
	if !dropDeleted || len(dropped) == 0 {
		return subs
	}
	return removeIndices(subs, dropped)
}

// removeIndices returns subs with the elements at the given indices removed,
// preserving the order of the rest. drop holds distinct valid indices (each
// cross-site sub belongs to exactly one site, collected at most once), so
// len(subs)-len(drop) is a sound capacity.
func removeIndices(subs []model.EnrichedSubscription, drop []int) []model.EnrichedSubscription {
	dropSet := make(map[int]struct{}, len(drop))
	for _, i := range drop {
		dropSet[i] = struct{}{}
	}
	kept := make([]model.EnrichedSubscription, 0, len(subs)-len(drop))
	for i := range subs {
		if _, gone := dropSet[i]; gone {
			continue
		}
		kept = append(kept, subs[i])
	}
	return kept
}

// enrichLocal builds sub.Room for LOCAL subs entirely from the $lookup baseline —
// room metadata plus the E2E key projected from the room's encKey sub-document —
// so it needs no separate key store read.
func (s *UserService) enrichLocal(subs []model.EnrichedSubscription, localIdx []int) {
	for _, j := range localIdx {
		subs[j].Room = buildLocalRoom(&subs[j])
		// hasUnread / hasGroupMention are computed at read time: room activity (resp.
		// an @all mention) newer than lastSeenAt. No room object (deleted/absent) ⇒
		// nothing to be unread/mentioned about.
		subs[j].HasUnread = subs[j].Room != nil && unread(subs[j].LastSeenAt, timeutil.TimeToMillis(subs[j].Room.LastMsgAt))
		subs[j].HasGroupMention = subs[j].Room != nil && unread(subs[j].LastSeenAt, timeutil.TimeToMillis(subs[j].Room.LastMentionAllAt))
	}
}

// enrichCrossSite fans out per remote site to GetRoomsInfo; a failed site RPC
// leaves that site's subs without a room object (no baseline fallback — there is
// no local room doc for a cross-site room). It returns the indices of subs whose
// remote room is soft-deleted ("Del-"), for the caller to drop.
func (s *UserService) enrichCrossSite(c *natsrouter.Context, subs []model.EnrichedSubscription, idxBySite map[string][]int, roomIDsBySite map[string][]string) []int {
	if len(idxBySite) == 0 {
		return nil
	}
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		sites = append(sites, site)
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
	// A cross-site room reported soft-deleted ("Del-") is collected for the caller
	// to drop. A degraded site (m == nil) or a not-found room is left with no room
	// object but KEPT — we can't tell a transient RPC failure from a real deletion.
	var dropped []int
	for i, site := range sites {
		m := infoBySite[i]
		if m == nil {
			continue
		}
		for _, j := range idxBySite[site] {
			info := m[subs[j].RoomID]
			if applyRoomInfo(&subs[j].Subscription, &info) {
				dropped = append(dropped, j)
			}
		}
	}
	return dropped
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
	// A soft-deleted room (name "Del-...") is surfaced with no room object.
	if strings.HasPrefix(sub.RoomName, deletedRoomNamePrefix) {
		return nil
	}
	room := &model.SubscriptionRoom{
		SiteID:            sub.SiteID,
		Name:              sub.RoomName,
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
// Returns true when the cross-site room is soft-deleted ("Del-" name prefix),
// signalling the caller to DROP the subscription entirely — the same exclusion the
// Mongo query applies to locally-deleted rooms. A not-found or degraded room
// returns false and is kept with no room object.
func applyRoomInfo(sub *model.Subscription, info *model.RoomInfo) bool {
	if !info.Found {
		return false
	}
	// Soft-deleted at the remote origin (name "Del-...") ⇒ drop the subscription.
	if strings.HasPrefix(info.Name, deletedRoomNamePrefix) {
		return true
	}
	// info.LastMsgAt/LastMentionAllAt arrive from the RPC as epoch millis (*int64);
	// the wire room object returns RFC3339 timestamps, so convert them here.
	room := &model.SubscriptionRoom{
		SiteID:            info.SiteID,
		Name:              info.Name,
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
	return false
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
	res.Data = s.enrichWithRoomInfo(c, res.Data, true)
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
	if strings.HasPrefix(req.AccountName, dmTargetSystemPrefix) || strings.HasSuffix(req.AccountName, dmTargetBotSuffix) {
		return nil, errcode.BadRequest("invalid DM target", errcode.WithReason(errcode.UserInvalidDMTarget))
	}
	dm, err := s.subs.GetDMSubscription(c, account, req.AccountName)
	if err != nil {
		return nil, fmt.Errorf("get dm: %w", err)
	}
	if dm == nil {
		return nil, errcode.NotFound("dm not found", errcode.WithReason(errcode.UserSubscriptionNotFound))
	}
	// Single-item lookup: dropDeleted=false, so a Del- room yields a sub with the
	// room nulled (never a drop) — the row always survives, matching how a LOCAL
	// Del- DM is kept room-less. The wire DMSubscription points at the boxed stored
	// sub plus HRInfo.
	one := []model.EnrichedSubscription{dm.EnrichedSubscription}
	one = s.enrichWithRoomInfo(c, one, false)
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
	one = s.enrichWithRoomInfo(c, one, false)
	items := s.buildListItems(c, one)
	return &models.SubscriptionListResponse{Subscriptions: items, Total: len(items)}, nil
}

func (s *UserService) CountSubscriptions(c *natsrouter.Context, req models.CountRequest) (*models.CountResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)
	total, err := s.subs.CountActiveSubscriptions(c, account)
	if err != nil {
		return nil, fmt.Errorf("count subscriptions: %w", err)
	}
	if req.Unread == nil || !*req.Unread {
		return &models.CountResponse{Count: total}, nil
	}
	return s.countUnread(c, account, total)
}

// countUnread counts active subs with unread messages. LOCAL subs are counted from the
// $lookup baseline (room.lastMsgAt) with no RPC; CROSS-SITE subs use per-site GetRoomsInfo
// RPCs that degrade independently — an unreachable site is skipped (its subs omitted),
// while local subs and the sites that did respond still count, so a remote hiccup yields a
// best-effort partial rather than the raw active-sub total.
func (s *UserService) countUnread(ctx context.Context, account string, total int) (*models.CountResponse, error) {
	// Short-circuit zero: min(0, maxSubs)=0 would build a $limit:0 MongoDB rejects.
	if total == 0 {
		return &models.CountResponse{Count: 0}, nil
	}
	// Cap at maxSubs — query-side total can exceed the cap; min keeps the fetch bounded and consistent with the list endpoints.
	subs, err := s.subs.GetActiveSubscriptions(ctx, account, min(total, s.maxSubs))
	if err != nil {
		return nil, fmt.Errorf("count unread: %w", err)
	}

	// LOCAL subs carry room.lastMsgAt on the $lookup baseline — count them with no RPC.
	// Only CROSS-SITE subs need the per-site GetRoomsInfo RPC (their room docs live remotely).
	unreadTotal := 0
	crossBySite := map[string][]model.EnrichedSubscription{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(subs[i].LastMsgAt)) {
				unreadTotal++
			}
			continue
		}
		site := subs[i].SiteID
		crossBySite[site] = append(crossBySite[site], subs[i])
		roomIDsBySite[site] = append(roomIDsBySite[site], subs[i].RoomID)
	}
	if len(crossBySite) == 0 {
		return &models.CountResponse{Count: unreadTotal}, nil
	}

	sites := make([]string, 0, len(crossBySite))
	for site := range crossBySite {
		sites = append(sites, site)
	}
	// Per-site degradation (matches the list path's enrichCrossSite): a failed site is
	// SKIPPED — its subs drop out of the count — while local subs and the sites that did
	// respond still contribute. WaitGroup (not errgroup.WithContext) so one site's failure
	// never cancels its siblings; results[i] is written by exactly one goroutine.
	results := make([]int, len(sites))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout) // bound concurrent per-site RPCs
	for i, site := range sites {
		// Client already gone — stop firing further ~5s RPCs.
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if ctx.Err() != nil {
				return
			}
			infos, err := s.rooms.GetRoomsInfo(ctx, site, roomIDsBySite[site])
			if err != nil {
				// Skip this site rather than nuking the whole count to total.
				slog.WarnContext(ctx, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(ctx), "error", err)
				return
			}
			lastMsg := make(map[string]*int64, len(infos))
			for k := range infos {
				// Mirror the list path (applyRoomInfo): a not-found or soft-deleted
				// (^Del-) room must not contribute to the count, even though the RPC
				// still returns a stale lastMsgAt for a room soft-deleted at its origin.
				if !infos[k].Found || strings.HasPrefix(infos[k].Name, deletedRoomNamePrefix) {
					continue
				}
				lastMsg[infos[k].RoomID] = infos[k].LastMsgAt
			}
			n := 0
			siteSubs := crossBySite[site]
			for j := range siteSubs {
				if unread(siteSubs[j].LastSeenAt, lastMsg[siteSubs[j].RoomID]) {
					n++
				}
			}
			results[i] = n
		}()
	}
	wg.Wait()
	for _, n := range results {
		unreadTotal += n
	}
	return &models.CountResponse{Count: unreadTotal}, nil
}

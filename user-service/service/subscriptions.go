package service

import (
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
	withLastMsg := req.IncludeLastMessage == nil || *req.IncludeLastMessage
	res.Data = s.enrichWithRoomInfoAndLastMsg(c, res.Data, true, withLastMsg)
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
func (s *UserService) enrichWithRoomInfoAndLastMsg(c *natsrouter.Context, subs []model.EnrichedSubscription, dropDeleted, withLastMsg bool) []model.EnrichedSubscription {
	if len(subs) == 0 {
		return subs
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
	dropped := s.enrichCrossSite(c, subs, idxBySite, roomIDsBySite)
	if withLastMsg {
		s.enrichLastMessage(c, subs, idxBySite, roomIDsBySite)
	}
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
	// The grouping includes the local site (served from the $lookup baseline); skip it here.
	sites := make([]string, 0, len(idxBySite))
	for site := range idxBySite {
		if site != s.siteID {
			sites = append(sites, site)
		}
	}
	if len(sites) == 0 {
		return nil
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

// enrichLastMessage populates sub.Room.PreviewMessage (read-time resolve, no denormalized
// write path) via one rooms.get RPC per site — LOCAL subs need it too (last-message
// isn't part of the $lookup baseline). One call per site: a subscription page is
// bounded well under history-service's 100-roomId batch cap, so no chunk-split is
// needed. Reuses the caller's per-site grouping. A degraded/absent site, or a room the
// RPC omits, just leaves PreviewMessage nil; it never fails the list.
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
			m, err := s.history.RoomsGet(c, site, roomIDsBySite[site])
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
			// Soft-deleted room (Room==nil) has nothing to attach a last message to.
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
	// A soft-deleted room (name "Del-...") is surfaced with no room object.
	if strings.HasPrefix(sub.RoomName, deletedRoomNamePrefix) {
		return nil
	}
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
	res.Data = s.enrichWithRoomInfoAndLastMsg(c, res.Data, true, false)
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
	// Single-item lookup: dropDeleted=false, so a Del- room yields a sub with the
	// room nulled (never a drop) — the row always survives, matching how a LOCAL
	// Del- DM is kept room-less. The wire DMSubscription points at the boxed stored
	// sub plus HRInfo.
	one := []model.EnrichedSubscription{dm.EnrichedSubscription}
	one = s.enrichWithRoomInfoAndLastMsg(c, one, false, false)
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
	one = s.enrichWithRoomInfoAndLastMsg(c, one, false, false)
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
	// Short-circuit zero: nothing to fetch or fan out for an empty active set.
	if total == 0 {
		return &models.CountResponse{Count: 0}, nil
	}
	ids, err := s.unreadRooms(c, account)
	if err != nil {
		return nil, err
	}
	// Best-effort reconciliation: refresh the badge accelerator from the Mongo
	// source of truth. Fail-open (badgeCache.Reseed never errors) and does not
	// block the reply — same goroutine, after the count is already computed.
	s.badge.Reseed(c, account, ids)
	return &models.CountResponse{Count: len(ids)}, nil
}

// unreadRooms returns the IDs of the account's active rooms with unread activity. A
// room contributes once if its messages are unread (LOCAL from the $lookup baseline,
// CROSS-SITE via per-site GetRoomsInfo) OR it is message-read but its subscription
// carries >=1 unread followed thread (Subscription.ThreadUnread — federated onto the
// home-replica sub by message-worker/inbox-worker, so no separate thread RPC is
// needed for either local or cross-site rooms). Everything degrades best-effort — an
// unreachable site is skipped rather than nuking the result.
func (s *UserService) unreadRooms(c *natsrouter.Context, account string) ([]string, error) {
	subs, err := s.subs.GetActiveSubscriptions(c, account, s.maxSubs)
	if err != nil {
		return nil, fmt.Errorf("unread rooms: %w", err)
	}

	// LOCAL subs carry room.lastMsgAt on the $lookup baseline — resolve them with no RPC.
	// Only CROSS-SITE subs need the per-site GetRoomsInfo RPC (their room docs live remotely).
	// pendingRooms collects the ThreadUnread of subs that came out READ (roomID ->
	// ThreadUnread) for the thread phase below.
	var ids []string
	pendingRooms := map[string][]string{}
	crossBySite := map[string][]model.EnrichedSubscription{}
	roomIDsBySite := map[string][]string{}
	for i := range subs {
		if subs[i].SiteID == s.siteID {
			if unread(subs[i].LastSeenAt, timeutil.TimeToMillis(subs[i].LastMsgAt)) {
				ids = append(ids, subs[i].RoomID)
			} else {
				pendingRooms[subs[i].RoomID] = subs[i].ThreadUnread
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
		// roomCand is a read room's thread-bump candidacy: just enough to key pendingRooms
		// without copying the whole (large) EnrichedSubscription through the results channel.
		type roomCand struct {
			roomID       string
			threadUnread []string
		}
		// Per-site degradation (matches the list path's enrichCrossSite): a failed site is
		// SKIPPED — its subs drop out of the result and out of pendingRooms — while local
		// subs and the sites that did respond still contribute. results[i] is written by
		// exactly one goroutine.
		type siteResult struct {
			unreadIDs []string
			readCands []roomCand
		}
		results := make([]siteResult, len(sites))
		var wg sync.WaitGroup
		sem := make(chan struct{}, maxSiteFanout) // bound concurrent per-site RPCs
		for i, site := range sites {
			// Client already gone — stop firing further ~5s RPCs.
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
				infos, err := s.rooms.GetRoomsInfo(c, site, roomIDsBySite[site])
				if err != nil {
					// Skip this site rather than nuking the whole result.
					slog.WarnContext(c, "unread count degraded for site", "account", account, "site", site, "request_id", natsutil.RequestIDFromContext(c), "error", err)
					return
				}
				lastMsg := make(map[string]*int64, len(infos))
				for k := range infos {
					// Mirror the list path (applyRoomInfo): a not-found or soft-deleted
					// (^Del-) room must not contribute, even though the RPC still returns
					// a stale lastMsgAt for a room soft-deleted at its origin.
					if !infos[k].Found || strings.HasPrefix(infos[k].Name, deletedRoomNamePrefix) {
						continue
					}
					lastMsg[infos[k].RoomID] = infos[k].LastMsgAt
				}
				var res siteResult
				siteSubs := crossBySite[site]
				for j := range siteSubs {
					rid := siteSubs[j].RoomID
					// Not-found / soft-deleted rooms are absent from lastMsg — neither
					// counted nor a thread candidate.
					ms, ok := lastMsg[rid]
					if !ok {
						continue
					}
					if unread(siteSubs[j].LastSeenAt, ms) {
						res.unreadIDs = append(res.unreadIDs, rid)
					} else {
						res.readCands = append(res.readCands, roomCand{roomID: rid, threadUnread: siteSubs[j].ThreadUnread})
					}
				}
				results[i] = res
			}()
		}
		wg.Wait()
		for i := range results {
			ids = append(ids, results[i].unreadIDs...)
			for _, cand := range results[i].readCands {
				pendingRooms[cand.roomID] = cand.threadUnread
			}
		}
	}

	// Thread phase: a message-read room still counts once if it has >=1 unread followed
	// thread. pendingRooms is keyed by roomID, so each room bumps at most once here.
	for roomID, threadUnread := range pendingRooms {
		if len(threadUnread) > 0 {
			ids = append(ids, roomID)
		}
	}
	return ids, nil
}

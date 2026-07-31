package main

import (
	"context"
	"log/slog"
	"sync"

	"github.com/hmchangw/chat/pkg/displayfmt"
	"github.com/hmchangw/chat/pkg/model"
)

// maxSiteFanout bounds concurrent RoomsInfoBatch RPCs across distinct sites.
const maxSiteFanout = 8

// enrichMessages layers room/sender enrichment onto the base hit projections.
// Every lookup is best-effort: a failure is logged and the affected field is
// omitted — the search response is never failed by enrichment.
func (h *handler) enrichMessages(ctx context.Context, account string, hits []messageSearchHit) []model.SearchMessage {
	out := make([]model.SearchMessage, 0, len(hits))
	for i := range hits {
		out = append(out, toSearchMessage(&hits[i]))
	}
	if len(hits) == 0 {
		return out
	}

	roomIDSet := map[string]struct{}{}
	senderSet := map[string]struct{}{}
	roomSite := map[string]string{}
	for i := range hits {
		roomIDSet[hits[i].RoomID] = struct{}{}
		roomSite[hits[i].RoomID] = hits[i].SiteID
		if hits[i].UserAccount != "" {
			senderSet[hits[i].UserAccount] = struct{}{}
		}
	}

	// Every mongo lookup is guarded: a handler may be wired without a MongoStore
	// (e.g. base message-search tests), and enrichment degrades rather than panics.
	subs := map[string]SubscriptionMeta{}
	if h.mongo != nil {
		s, err := h.mongo.SubscriptionsByRoomIDs(ctx, account, keysOf(roomIDSet))
		if err != nil {
			slog.Warn("search enrich: subscriptions lookup failed", "account", account, "error", err)
		} else {
			subs = s
		}
	}

	// Partition rooms and collect the join keys for the batch lookups.
	dmCounterparts := map[string]struct{}{}
	botAccounts := map[string]struct{}{}
	channelRoomsBySite := map[string][]string{}
	for rid := range roomIDSet {
		meta, ok := subs[rid]
		if !ok {
			channelRoomsBySite[roomSite[rid]] = append(channelRoomsBySite[roomSite[rid]], rid)
			continue
		}
		switch meta.RoomType {
		case model.RoomTypeDM:
			if meta.Name != "" {
				dmCounterparts[meta.Name] = struct{}{}
			}
		case model.RoomTypeBotDM:
			if meta.Name != "" {
				botAccounts[meta.Name] = struct{}{}
			}
		default:
			channelRoomsBySite[roomSite[rid]] = append(channelRoomsBySite[roomSite[rid]], rid)
		}
	}

	// Sender names: bots resolve via apps, humans via users.
	userAccts := map[string]struct{}{}
	for a := range dmCounterparts {
		userAccts[a] = struct{}{}
	}
	for a := range senderSet {
		if model.IsBot(a) {
			botAccounts[a] = struct{}{}
		} else {
			userAccts[a] = struct{}{}
		}
	}

	users := map[string]HRUser{}
	if h.mongo != nil && len(userAccts) > 0 {
		u, err := h.mongo.UsersByAccounts(ctx, keysOf(userAccts))
		if err != nil {
			slog.Warn("search enrich: users lookup failed", "error", err)
		} else {
			users = u
		}
	}
	apps := map[string]model.App{}
	if h.mongo != nil && len(botAccounts) > 0 {
		a, err := h.mongo.AppsByAssistantNames(ctx, keysOf(botAccounts))
		if err != nil {
			slog.Warn("search enrich: apps lookup failed", "error", err)
		} else {
			apps = a
		}
	}

	roomNames := h.fetchRoomNames(ctx, channelRoomsBySite)

	for i := range hits {
		rid := hits[i].RoomID
		out[i].Sender = &model.MessageSender{
			Account:     hits[i].UserAccount,
			DisplayName: resolveSenderName(hits[i].UserAccount, users, apps),
		}
		room := &model.MessageRoom{ID: rid}
		if meta, ok := subs[rid]; ok {
			room.Type = meta.RoomType
			switch meta.RoomType {
			case model.RoomTypeDM:
				if hr, ok := users[meta.Name]; ok {
					room.HRInfo = &model.SubscriptionHRInfo{Account: hr.Account, Name: hr.ChineseName, EngName: hr.EngName}
					room.Name = displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, meta.Name)
				} else {
					room.Name = meta.Name
				}
			case model.RoomTypeBotDM:
				if app, ok := apps[meta.Name]; ok {
					room.AppInfo = model.AppSubscriptionFromApp(&app)
					room.Name = app.Name
				} else {
					room.Name = meta.Name
				}
			default:
				room.Name = roomNames[rid]
			}
		} else {
			room.Name = roomNames[rid]
		}
		out[i].Room = room
	}
	return out
}

// fetchRoomNames issues one RoomsInfoBatch RPC per distinct site (bounded
// fan-out), returning roomID→name for rooms the RPC resolved. A per-site
// failure is logged and skipped.
func (h *handler) fetchRoomNames(ctx context.Context, bySite map[string][]string) map[string]string {
	names := map[string]string{}
	if len(bySite) == 0 || h.room == nil {
		return names
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for site, ids := range bySite {
		site, ids := site, ids
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			infos, err := h.room.GetRoomsInfo(ctx, site, ids)
			if err != nil {
				slog.Warn("search enrich: room-info rpc failed", "site", site, "error", err)
				return
			}
			mu.Lock()
			for i := range infos {
				if infos[i].Found && infos[i].Name != "" {
					names[infos[i].RoomID] = infos[i].Name
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	return names
}

// resolveSenderName returns the sender's display name: the app name for a bot
// account, else engName+chineseName (fallback account) from the users map.
func resolveSenderName(account string, users map[string]HRUser, apps map[string]model.App) string {
	if account == "" {
		return ""
	}
	if model.IsBot(account) {
		if app, ok := apps[account]; ok && app.Name != "" {
			return app.Name
		}
		return account
	}
	if hr, ok := users[account]; ok {
		return displayfmt.CombineWithFallback(hr.EngName, hr.ChineseName, account)
	}
	return account
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

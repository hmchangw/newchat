package service

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
)

// threadInfoBatchChunk keeps each ThreadRoomInfoBatch request under room-service's
// MAX_BATCH_SIZE (default 1000).
const threadInfoBatchChunk = 500

// GetThreadUnreadSummary returns the user's cross-site thread-unread badge: it
// reads the access-gated home-replica thread-subs, groups them by owning site,
// fetches each thread's lastMsgAt from that site, and folds them via unread().
// NATS: chat.user.{account}.request.user.{siteID}.thread.unread.summary
func (s *UserService) GetThreadUnreadSummary(c *natsrouter.Context, _ model.ThreadUnreadSummaryRequest) (*model.ThreadUnreadSummaryResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)

	rows, err := s.threadSubs.ListByAccount(c, account)
	if err != nil {
		return nil, fmt.Errorf("list thread subscriptions: %w", err)
	}
	if len(rows) == 0 {
		return &model.ThreadUnreadSummaryResponse{}, nil
	}

	// Group thread rooms by owning site, and keep each row's read state + room type.
	type subState struct {
		lastSeenAt *time.Time
		hasMention bool
		roomType   model.RoomType
	}
	idsBySite := map[string][]string{}
	stateByThread := make(map[string]subState, len(rows))
	for i := range rows {
		r := &rows[i] // index to avoid copying the full row each iteration
		idsBySite[r.SiteID] = append(idsBySite[r.SiteID], r.ThreadRoomID)
		stateByThread[r.ThreadRoomID] = subState{lastSeenAt: r.LastSeenAt, hasMention: r.HasMention, roomType: r.RoomType}
	}

	type siteResult struct {
		infos  []model.ThreadRoomInfo
		failed bool
	}
	sites := make([]string, 0, len(idsBySite))
	for site := range idsBySite {
		sites = append(sites, site)
	}
	results := make([]siteResult, len(sites))

	var wg sync.WaitGroup
	for i, site := range sites {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, chunk := range chunkStrings(idsBySite[site], threadInfoBatchChunk) {
				if c.Err() != nil {
					results[i].failed = true
					return
				}
				infos, err := s.rooms.GetThreadRoomInfoBatch(c, site, chunk)
				if err != nil {
					slog.WarnContext(c, "thread-unread site degraded",
						"account", account, "site", site,
						"request_id", natsutil.RequestIDFromContext(c), "error", err)
					results[i].failed = true
					return
				}
				results[i].infos = append(results[i].infos, infos...)
			}
		}()
	}
	wg.Wait()

	resp := &model.ThreadUnreadSummaryResponse{}
	var maxLastMsg int64
	var haveLastMsg bool
	for i, site := range sites {
		if results[i].failed {
			resp.UnavailableSites = append(resp.UnavailableSites, site)
			continue
		}
		for _, info := range results[i].infos {
			if !info.Found {
				continue
			}
			if info.LastMsgAt > maxLastMsg {
				maxLastMsg, haveLastMsg = info.LastMsgAt, true
			}
			st := stateByThread[info.ThreadRoomID]
			ms := info.LastMsgAt
			isUnread := unread(st.lastSeenAt, &ms)
			resp.Unread = resp.Unread || isUnread
			resp.UnreadDirectMessage = resp.UnreadDirectMessage || (isUnread && st.roomType == model.RoomTypeDM)
			// mention counts only for still-existing threads (Found); a mention on a deleted thread is stale.
			resp.UnreadMention = resp.UnreadMention || st.hasMention
		}
	}
	if haveLastMsg {
		resp.LastMessageAt = &maxLastMsg
	}
	return resp, nil
}

// ClearAllThreadUnread is the cross-site "mark all threads read" aggregator: it
// reads the user's home-replica thread-subs, derives the distinct owning sites,
// and asks each site's room-service to clear all of the account's thread-unread
// state. Per-site failures degrade into UnavailableSites rather than failing the
// call, mirroring GetThreadUnreadSummary.
// NATS: chat.user.{account}.request.user.{siteID}.thread.read.all
func (s *UserService) ClearAllThreadUnread(c *natsrouter.Context, _ model.ThreadReadAllRequest) (*model.ThreadReadAllResponse, error) {
	account := c.Param("account")
	c.WithLogValues("account", account)

	rows, err := s.threadSubs.ListByAccount(c, account)
	if err != nil {
		return nil, fmt.Errorf("list thread subscriptions: %w", err)
	}
	if len(rows) == 0 {
		return &model.ThreadReadAllResponse{}, nil
	}

	// Distinct owning sites, in first-seen order.
	seen := make(map[string]struct{}, len(rows))
	sites := make([]string, 0, len(rows))
	for i := range rows {
		site := rows[i].SiteID
		if site == "" {
			continue
		}
		if _, dup := seen[site]; dup {
			continue
		}
		seen[site] = struct{}{}
		sites = append(sites, site)
	}

	failed := make([]bool, len(sites))

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxSiteFanout)
	for i, site := range sites {
		if c.Err() != nil {
			failed[i] = true
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.rooms.ClearAllThreadUnread(c, site, account); err != nil {
				slog.WarnContext(c, "thread-read-all site degraded",
					"account", account, "site", site,
					"request_id", natsutil.RequestIDFromContext(c), "error", err)
				failed[i] = true
			}
		}()
	}
	wg.Wait()

	resp := &model.ThreadReadAllResponse{}
	for i, site := range sites {
		if failed[i] {
			resp.UnavailableSites = append(resp.UnavailableSites, site)
		}
	}
	return resp, nil
}

// chunkStrings splits ids into slices of at most size elements.
func chunkStrings(ids []string, size int) [][]string {
	if size <= 0 {
		size = len(ids)
	}
	if len(ids) <= size {
		return [][]string{ids}
	}
	var out [][]string
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}

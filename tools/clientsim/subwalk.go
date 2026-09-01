package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/errcode"
)

// subListPageLimit is the server's default page size; we request it
// explicitly so the walk is deterministic (docs/client-api.md §subscription.list).
const subListPageLimit = 40

// maxSubListPages bounds the bootstrap walk (loadgen's soakMaxPages
// precedent): a server stuck on hasMore=true must not spin the walk
// forever. 250 pages × 40 rows covers a 10k-room sidebar.
const maxSubListPages = 250

type subListRequest struct {
	Type   string `json:"type"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type subRoom struct {
	CrossSite *bool `json:"crossSite,omitempty"`
}

type subRow struct {
	RoomID   string   `json:"roomId"`
	RoomType string   `json:"roomType"`
	Room     *subRoom `json:"room,omitempty"`
}

type subListPage struct {
	Subscriptions []subRow `json:"subscriptions"`
	HasMore       bool     `json:"hasMore"`
}

type subscriptionLister interface {
	List(ctx context.Context, req subListRequest) (*subListPage, error)
}

// roomGlobal applies the frontend's tri-state crossSite rule: only an
// explicit false routes to the local namespace; missing data fails safe to
// global (chat-frontend subjects.ts).
func roomGlobal(room *subRoom) bool {
	return room == nil || room.CrossSite == nil || *room.CrossSite
}

// fetchSubscriptionPlan runs the paginated subscription.list bootstrap and
// returns roomID -> global for every channel subscription. Cross-page
// duplicate rows are deduped by roomID (multi-page drains are best-effort
// ordered; docs/client-api.md).
func fetchSubscriptionPlan(ctx context.Context, l subscriptionLister) (map[string]bool, error) {
	plan := map[string]bool{}
	for page := 0; page < maxSubListPages; page++ {
		offset := page * subListPageLimit
		reply, err := l.List(ctx, subListRequest{Type: "rooms", Offset: offset, Limit: subListPageLimit})
		if err != nil {
			return nil, fmt.Errorf("subscription.list page at offset %d: %w", offset, err)
		}
		for _, row := range reply.Subscriptions {
			if row.RoomType != "channel" {
				continue
			}
			if _, seen := plan[row.RoomID]; seen {
				continue
			}
			plan[row.RoomID] = roomGlobal(row.Room)
		}
		// An empty page claiming more is a server bug; stop rather than spin.
		if !reply.HasMore || len(reply.Subscriptions) == 0 {
			return plan, nil
		}
	}
	return nil, fmt.Errorf("subscription.list exceeded %d pages without hasMore=false", maxSubListPages)
}

// natsLister issues the real client RPC over the simulated user's own
// connection, exactly as the frontend does.
type natsLister struct {
	conn    simConn
	subject string // subject.UserSubscriptionList(account, siteID)
	timeout time.Duration
}

func (l *natsLister) List(ctx context.Context, req subListRequest) (*subListPage, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal subscription.list request: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	msg, err := l.conn.Request(reqCtx, l.subject, body)
	if err != nil {
		return nil, fmt.Errorf("subscription.list request: %w", err)
	}
	if ec, isErr := errcode.Parse(msg.Data); isErr {
		return nil, fmt.Errorf("subscription.list rejected: %w", ec)
	}
	var page subListPage
	if err := json.Unmarshal(msg.Data, &page); err != nil {
		return nil, fmt.Errorf("decode subscription.list reply: %w", err)
	}
	return &page, nil
}

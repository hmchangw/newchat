package main

import (
	"context"
	"encoding/json"
	"errors"
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

// errWalkProtocol tags a reply the walk cannot use — the wrong shape, or a
// page that contradicts itself. Unlike a timeout or a downed responder, a
// retry produces the same reply, so the resync abandons rather than spins.
var errWalkProtocol = errors.New("subscription.list protocol violation")

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
		if !reply.HasMore {
			return plan, nil
		}
		// hasMore with no rows leaves the plan truncated. Returning it as a
		// success would be the worst outcome available: the client subscribes
		// to a subset, marks the plan verified and reports ready, so a soak
		// silently measures a fraction of the sidebar behind green gauges.
		// Failing sends the walk back through the resync instead.
		if len(reply.Subscriptions) == 0 {
			return nil, fmt.Errorf("subscription.list page at offset %d claims hasMore with no rows: %w", offset, errWalkProtocol)
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
	// Decoded through a pointer-valued shadow of subListPage so an absent or
	// null `subscriptions` is distinguishable from an explicit empty array.
	// Straight into subListPage, a reply of `null`, `{}` or one missing the
	// field decodes into a zero value that reads exactly like "this user has
	// no channels" — a broken responder would then leave the whole fleet
	// ready with nothing subscribed.
	var reply struct {
		Subscriptions *[]subRow `json:"subscriptions"`
		HasMore       bool      `json:"hasMore"`
	}
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return nil, fmt.Errorf("decode subscription.list reply: %w: %w", errWalkProtocol, err)
	}
	if reply.Subscriptions == nil {
		return nil, fmt.Errorf("subscription.list reply has no subscriptions field: %w", errWalkProtocol)
	}
	return &subListPage{Subscriptions: *reply.Subscriptions, HasMore: reply.HasMore}, nil
}

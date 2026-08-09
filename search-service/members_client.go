package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/subject"
)

// membersRPCTimeout bounds the subscription.getChannels round trip.
const membersRPCTimeout = 5 * time.Second

// getChannelsRequest/getChannelsResponse mirror the wire shape of user-service's
// subscription.getChannels (user-service/models.GetChannelsRequest /
// PagedSubscriptionListResponse). Declared locally — that package is
// user-service-internal, not a shared pkg/model contract — and only the
// roomId this caller needs is decoded.
type getChannelsRequest struct {
	AccountNames []string `json:"accountNames,omitempty"`
}

type getChannelsResponse struct {
	Subscriptions []struct {
		RoomID string `json:"roomId"`
	} `json:"subscriptions"`
}

// membersClient is the NATS-backed MembersClient. It issues
// subscription.getChannels on the REQUESTER's own account subject (a
// server-to-server call within the same site), matching how every other
// user-scoped RPC in this codebase is addressed.
type membersClient struct {
	nc *o11ynats.Conn
}

func newMembersClient(nc *o11ynats.Conn) *membersClient { return &membersClient{nc: nc} }

func (c *membersClient) GetChannels(ctx context.Context, account, siteID string, members []string) ([]string, error) {
	req, err := json.Marshal(getChannelsRequest{AccountNames: members})
	if err != nil {
		return nil, fmt.Errorf("marshal getChannels request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.UserSubscriptionGetChannels(account, siteID), req, membersRPCTimeout)
	if err != nil {
		return nil, fmt.Errorf("getChannels rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return nil, e
	}
	var out getChannelsResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode getChannels response: %w", err)
	}
	roomIDs := make([]string, 0, len(out.Subscriptions))
	for _, sub := range out.Subscriptions {
		roomIDs = append(roomIDs, sub.RoomID)
	}
	return roomIDs, nil
}

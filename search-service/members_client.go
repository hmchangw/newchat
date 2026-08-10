package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/subject"
)

const (
	// membersRPCTimeout bounds each subscription.getChannels round trip.
	membersRPCTimeout = 5 * time.Second
	// membersPageLimit is the page size per getChannels round trip; GetChannels
	// pages until hasMore is false so a member in many rooms isn't truncated.
	membersPageLimit = 200
)

// getChannelsRequest/getChannelsResponse mirror the wire shape of user-service's
// subscription.getChannels. Declared locally — that package is user-service-internal,
// not a shared pkg/model contract — and only the fields this caller needs are decoded.
type getChannelsRequest struct {
	AccountNames []string `json:"accountNames,omitempty"`
	Offset       int      `json:"offset,omitempty"`
	Limit        int      `json:"limit,omitempty"`
}

type getChannelsResponse struct {
	Subscriptions []struct {
		RoomID string `json:"roomId"`
	} `json:"subscriptions"`
	HasMore bool `json:"hasMore"`
}

// subscriptionRequester is the narrow NATS request surface GetChannels needs;
// *o11ynats.Conn satisfies it, and tests inject a stub.
type subscriptionRequester interface {
	Request(ctx context.Context, subj string, data []byte, timeout time.Duration) (*nats.Msg, error)
}

// membersClient issues subscription.getChannels on the REQUESTER's own account
// subject (a server-to-server call within the same site).
type membersClient struct {
	req subscriptionRequester
}

func newMembersClient(nc *o11ynats.Conn) *membersClient { return &membersClient{req: nc} }

// GetChannels returns every room id the members share, paging through
// getChannels until hasMore is false so large memberships aren't truncated.
func (c *membersClient) GetChannels(ctx context.Context, account, siteID string, members []string) ([]string, error) {
	subj := subject.UserSubscriptionGetChannels(account, siteID)
	var roomIDs []string
	for offset := 0; ; {
		body, err := json.Marshal(getChannelsRequest{AccountNames: members, Offset: offset, Limit: membersPageLimit})
		if err != nil {
			return nil, fmt.Errorf("marshal getChannels request: %w", err)
		}
		msg, err := c.req.Request(ctx, subj, body, membersRPCTimeout)
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
		for _, sub := range out.Subscriptions {
			roomIDs = append(roomIDs, sub.RoomID)
		}
		// Last page, or defensively an empty page while hasMore stays true.
		if !out.HasMore || len(out.Subscriptions) == 0 {
			break
		}
		offset += len(out.Subscriptions)
	}
	return roomIDs, nil
}

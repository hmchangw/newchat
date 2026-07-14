package historyclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Marz32onE/instrumentation-go/otel-nats/otelnats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// historyRPCTimeout bounds each per-site history-service request/reply round trip.
const historyRPCTimeout = 5 * time.Second

// Client implements service.HistoryClient via NATS request/reply to a site's
// history-service. The destination site is passed per call, so one Client fans
// out across sites.
type Client struct {
	nc *otelnats.Conn
}

// New returns a Client wired to nc.
func New(nc *otelnats.Conn) *Client { return &Client{nc: nc} }

// GetThreadList issues the per-site thread-list RPC to history-service at
// siteID; non-OK reply envelopes are relayed via errcode.Parse to preserve the
// remote classification.
func (c *Client) GetThreadList(ctx context.Context, siteID string, req model.ThreadSubscriptionListRequest) (model.ThreadSubscriptionListResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("marshal thread-list request: %w", err)
	}
	msg, err := c.nc.Request(ctx, subject.ThreadSubscriptionList(siteID), body, historyRPCTimeout)
	if err != nil {
		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("thread-list rpc: %w", err)
	}
	if e, ok := errcode.Parse(msg.Data); ok {
		return model.ThreadSubscriptionListResponse{}, e
	}
	var out model.ThreadSubscriptionListResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("decode thread-list response: %w", err)
	}
	return out, nil
}

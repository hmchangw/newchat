package historyclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// historyRPCTimeout bounds each per-site history-service request/reply round trip.
const historyRPCTimeout = 5 * time.Second

// Client implements service.HistoryClient via NATS request/reply to a site's
// history-service. The destination site is passed per call, so one Client fans
// out across sites.
type Client struct {
	// metrics is injected rather than built here: a second natsmetrics.Metrics
	// would duplicate every shared instrument on a different construction path.
	// The zero value is safe and records nothing.
	metrics natsmetrics.Publisher

	nc *o11ynats.Conn
}

// New returns a Client wired to nc.
func New(nc *o11ynats.Conn, metrics natsmetrics.Publisher) *Client {
	return &Client{nc: nc, metrics: metrics}
}

// GetThreadList issues the per-site thread-list RPC to history-service at
// siteID; non-OK reply envelopes are relayed via errcode.Parse to preserve the
// remote classification.
func (c *Client) GetThreadList(ctx context.Context, siteID string, req model.ThreadSubscriptionListRequest) (_ model.ThreadSubscriptionListResponse, resultErr error) {
	body, err := json.Marshal(req)
	if err != nil {
		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("marshal thread-list request: %w", err)
	}
	// Recorded from the final outcome, not from nc.Request's return: a remote
	// errcode envelope and a decode failure both arrive as a successful
	// transport read, so recording at the call site would label them success
	// and lose error.type.
	started := time.Now()
	defer func() {
		c.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodListThreadSubscriptions, time.Since(started), resultErr)
	}()
	msg, err := c.nc.Request(ctx, subject.ThreadSubscriptionList(siteID), body, historyRPCTimeout)
	if err != nil {
		return model.ThreadSubscriptionListResponse{}, natsutil.RequestFailure("thread-list rpc", err)
	}
	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return model.ThreadSubscriptionListResponse{}, remoteErr
	}
	var out model.ThreadSubscriptionListResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return model.ThreadSubscriptionListResponse{}, fmt.Errorf("decode thread-list response: %w", err)
	}
	return out, nil
}

// RoomsGet issues the per-site rooms.get batch RPC to history-service, returning
// the resolvable last message for each requested room; rooms with no message, or
// that degraded, are simply absent from the map (mirrors the server's own
// per-room best-effort degrade). hints carries caller-known walk-bounds (e.g. the
// room's LastMsgAt already resolved locally) so history-service can skip its own
// room-times read for those rooms; a nil/empty map is wire-compatible with older
// callers that only send roomIds.
func (c *Client) RoomsGet(ctx context.Context, siteID string, roomIDs []string, hints map[string]model.RoomTimeHint) (_ map[string]model.PreviewMessage, resultErr error) {
	body, err := json.Marshal(model.RoomsGetRequest{RoomIDs: roomIDs, Hints: hints})
	if err != nil {
		return nil, fmt.Errorf("marshal rooms-get request: %w", err)
	}
	// Recorded from the final outcome, not from nc.Request's return: a remote
	// errcode envelope and a decode failure both arrive as a successful
	// transport read, so recording at the call site would label them success
	// and lose error.type.
	started := time.Now()
	defer func() {
		c.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodBatchGetRoomPreviews, time.Since(started), resultErr)
	}()
	msg, err := c.nc.Request(ctx, subject.RoomsGet(siteID), body, historyRPCTimeout)
	if err != nil {
		return nil, natsutil.RequestFailure("rooms-get rpc", err)
	}
	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
	}
	var out model.RoomsGetResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode rooms-get response: %w", err)
	}
	return out.Rooms, nil
}

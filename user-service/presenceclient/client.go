package presenceclient

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

// presenceRPCTimeout matches the sibling clients' 5s: presence degrades to
// offline on timeout, so allow a normal round trip before giving up.
const presenceRPCTimeout = time.Second * 5

// Client implements service.PresenceClient via NATS request/reply over the
// server-to-server lane; the passed siteID must be the accounts' home site.
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

// QueryPresence runs the batch presence query at siteID; non-OK envelopes relay via errcode.Parse.
func (c *Client) QueryPresence(ctx context.Context, siteID string, accounts []string) (_ []model.PresenceState, resultErr error) {
	body, err := json.Marshal(model.PresenceQuery{Accounts: accounts})
	if err != nil {
		return nil, fmt.Errorf("marshal presence-query request: %w", err)
	}
	// Recorded from the final outcome, not from nc.Request's return: a remote
	// errcode envelope and a decode failure both arrive as a successful
	// transport read, so recording at the call site would label them success
	// and lose error.type.
	started := time.Now()
	defer func() {
		c.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodBatchGetPeerPresence, time.Since(started), resultErr)
	}()
	msg, err := c.nc.Request(ctx, subject.PresenceQueryBatchPeer(siteID), body, presenceRPCTimeout)
	if err != nil {
		return nil, natsutil.RequestFailure("presence-query rpc", err)
	}
	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
	}
	var out model.PresenceQueryResponse
	if err := json.Unmarshal(msg.Data, &out); err != nil {
		return nil, fmt.Errorf("decode presence-query response: %w", err)
	}
	return out.States, nil
}

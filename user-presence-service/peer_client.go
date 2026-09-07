//go:generate mockgen -source=peer_client.go -destination=mock_peer_client_test.go -package=main

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// PeerPresenceClient fetches presence states for accounts homed on a remote
// site via the server-to-server batch RPC (the fan-out leaf).
type PeerPresenceClient interface {
	QueryPeer(ctx context.Context, siteID string, accounts []string) ([]model.PresenceState, error)
}

// natsPeerPresenceClient is a NATS request/reply implementation.
type natsPeerPresenceClient struct {
	nc      *nats.Conn
	timeout time.Duration
}

// NewNATSPeerPresenceClient returns the concrete type so future struct-only
// methods don't require widening the interface ("accept interfaces, return structs").
func NewNATSPeerPresenceClient(nc *nats.Conn, timeout time.Duration) *natsPeerPresenceClient {
	return &natsPeerPresenceClient{nc: nc, timeout: timeout}
}

// QueryPeer issues a batch presence query to a remote site's leaf endpoint.
func (c *natsPeerPresenceClient) QueryPeer(ctx context.Context, siteID string, accounts []string) ([]model.PresenceState, error) {
	body, err := json.Marshal(model.PresenceQuery{Accounts: accounts})
	if err != nil {
		return nil, fmt.Errorf("marshal peer presence query: %w", err)
	}

	// c.timeout is validated as > 0 at config-load time (see main.go).
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	out := &nats.Msg{
		Subject: subject.PresenceQueryBatchPeer(siteID),
		Data:    body,
		Header:  nats.Header{},
	}
	reply, err := c.nc.RequestMsgWithContext(reqCtx, out)
	if err != nil {
		return nil, fmt.Errorf("presence peer query to %s: %w", siteID, err)
	}
	if remoteErr := errcode.FromReply(reply.Data); remoteErr != nil {
		return nil, fmt.Errorf("remote presence query: %w", remoteErr)
	}

	var resp model.PresenceQueryResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal peer presence reply: %w", err)
	}
	return resp.States, nil
}

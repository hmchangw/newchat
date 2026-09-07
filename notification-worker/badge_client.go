package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

// badgeFetchTimeout bounds the badge.count.batch RPC so a slow or unreachable
// remote site's user-service can't stall the push fan-out.
const badgeFetchTimeout = 5 * time.Second

// badgeClient resolves badge unread-room counts from a site's user-service.
// Consumer-defined so tests inject a fake; a nil badgeClient disables the
// badge phase entirely (Phase A compatibility — see HandlerDeps.BadgeClient).
type badgeClient interface {
	Counts(ctx context.Context, siteID, roomID string, accounts []string) (map[string]int, error)
}

// natsBadgeClient requests subject.BadgeCountBatch(siteID) via NATS request/reply,
// mirroring historyParentFetcher's shape (sonic codec, errcode.Parse for the remote envelope).
type natsBadgeClient struct {
	nc *o11ynats.Conn
	// metrics is injected on the same terms as historyParentFetcher's: building
	// a second natsmetrics.Metrics here would duplicate the shared instruments,
	// and the zero value records nothing.
	metrics natsmetrics.Publisher
}

func newNatsBadgeClient(nc *o11ynats.Conn, metrics natsmetrics.Publisher) *natsBadgeClient {
	return &natsBadgeClient{nc: nc, metrics: metrics}
}

// Counts requests badge unread-room counts for accounts from siteID's user-service,
// naming roomID as the room that triggered the notification. Any error (timeout, no
// responder, remote errcode envelope, unmarshal) is wrapped and returned so the caller
// can decide how to degrade — the badge phase must never NAK the push on its behalf.
func (c *natsBadgeClient) Counts(ctx context.Context, siteID, roomID string, accounts []string) (_ map[string]int, resultErr error) {
	// fetchUnreadCounts is fail-open and discards this error, so it never reaches
	// a settle decision.
	// nosemgrep: jsretry-marshal-failure-must-be-permanent
	reqBytes, err := sonic.Marshal(model.BadgeCountBatchRequest{RoomID: roomID, Accounts: accounts})
	if err != nil {
		return nil, fmt.Errorf("marshal badge count batch request for site %s: %w", siteID, err)
	}
	// Recorded because this call blocks the notification handler: fetchUnreadCounts
	// runs it inside an errgroup the handler awaits, bounded at badgeFetchTimeout,
	// on a MAX_WORKERS semaphore loop. Without a client series a slow user-service
	// shows up only as consumer lag, and the callee's own histogram does not close
	// the gap — the fan-out is per home site, so a remote peer's server-side
	// samples live in that site's Prometheus, not this one's.
	//
	// The outcome is the function's, not nc.Request's.
	// A remote errcode envelope and a decode failure both arrive as a
	// successful transport read, so recording at the call site labelled them
	// success and lost error.type — the client histogram then disagreed with
	// the callee's server histogram about whether the call failed.
	started := time.Now()
	defer func() {
		c.metrics.RecordRPCClientCall(ctx, natsmetrics.MethodBatchGetBadgeCounts, time.Since(started), resultErr)
	}()
	msg, err := c.nc.Request(ctx, subject.BadgeCountBatch(siteID), reqBytes, badgeFetchTimeout)
	if err != nil {
		return nil, fmt.Errorf("badge count batch request to site %s: %w", siteID, err)
	}
	// FromReply, not Parse: an envelope is always a failure, and an
	// unrecognised code must not be relayed as a typed *errcode.Error. See its
	// doc comment for the two ways hand-rolling this goes wrong.
	if remoteErr := errcode.FromReply(msg.Data); remoteErr != nil {
		return nil, remoteErr
	}
	var resp model.BadgeCountBatchResponse
	if err := sonic.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal badge count batch response from site %s: %w", siteID, err)
	}
	return resp.Counts, nil
}

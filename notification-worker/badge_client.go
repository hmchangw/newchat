package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bytedance/sonic"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
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
}

func newNatsBadgeClient(nc *o11ynats.Conn) *natsBadgeClient {
	return &natsBadgeClient{nc: nc}
}

// Counts requests badge unread-room counts for accounts from siteID's user-service,
// naming roomID as the room that triggered the notification. Any error (timeout, no
// responder, remote errcode envelope, unmarshal) is wrapped and returned so the caller
// can decide how to degrade — the badge phase must never NAK the push on its behalf.
func (c *natsBadgeClient) Counts(ctx context.Context, siteID, roomID string, accounts []string) (map[string]int, error) {
	// fetchUnreadCounts is fail-open and discards this error, so it never reaches
	// a settle decision.
	// nosemgrep: jsretry-marshal-failure-must-be-permanent
	reqBytes, err := sonic.Marshal(model.BadgeCountBatchRequest{RoomID: roomID, Accounts: accounts})
	if err != nil {
		return nil, fmt.Errorf("marshal badge count batch request for site %s: %w", siteID, err)
	}
	msg, err := c.nc.Request(ctx, subject.BadgeCountBatch(siteID), reqBytes, badgeFetchTimeout)
	if err != nil {
		return nil, fmt.Errorf("badge count batch request to site %s: %w", siteID, err)
	}
	// The errcode envelope has a top-level "error"; a real response never does, so this
	// can't false-positive. Propagate the typed remote error for accurate classification.
	if ee, ok := errcode.Parse(msg.Data); ok && ee.Code.Valid() {
		return nil, ee
	}
	var resp model.BadgeCountBatchResponse
	if err := sonic.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal badge count batch response from site %s: %w", siteID, err)
	}
	return resp.Counts, nil
}

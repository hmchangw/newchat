package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/subject"
)

// PublishFunc publishes data on subj; msgID becomes the Nats-Msg-Id for
// stream-level dedup.
type PublishFunc func(ctx context.Context, subj string, data []byte, msgID string) error

// ForwardWithFailover publishes a federation event to the destination site's
// INBOX, redirecting to its buddy-hosted failover lane when — and only when —
// the destination is unambiguously unreachable.
//
// The fallback triggers on no-responders alone, never on a timeout. INBOX and
// INBOX-FAILOVER are separate streams with independent dedup windows, so a
// shared Nats-Msg-Id does NOT collapse a duplicate across them; redirecting an
// ambiguous failure (which may already have landed) would risk applying the
// event twice. No-responders carries no such ambiguity — no interest existed, so
// nothing was delivered.
//
// Any other error returns unchanged, so the caller Naks and parks exactly as
// before this fallback existed.
func ForwardWithFailover(ctx context.Context, publish PublishFunc,
	destSiteID, eventType string, data []byte, msgID string,
) error {
	err := publish(ctx, subject.InboxExternal(destSiteID, eventType), data, msgID)
	if err == nil {
		return nil
	}
	if !IsNoResponders(err) {
		return fmt.Errorf("forward to %s inbox: %w", destSiteID, err)
	}
	if ferr := publish(ctx, subject.FailoverInboxExternal(destSiteID, eventType), data, msgID); ferr != nil {
		return fmt.Errorf("forward to %s failover inbox (primary unreachable): %w", destSiteID, ferr)
	}
	return nil
}

// IsNoResponders reports whether err means nothing was subscribed to the
// subject, so the publish provably did not land. The jetstream package wraps
// core NATS's ErrNoResponders as ErrNoStreamResponse on the publish path;
// matching both keeps the check correct regardless of which surfaces.
//
// Exported because it is the gate for every failover redirect, not just this
// one — a caller that redirects on a different error class reintroduces the
// cross-stream duplicate this rule exists to prevent.
func IsNoResponders(err error) bool {
	return errors.Is(err, nats.ErrNoResponders) || errors.Is(err, jetstream.ErrNoStreamResponse)
}

// PublishWithFailover is PublishTo's producer-side twin of ForwardWithFailover:
// it buffers the event on the live OUTBOX and, when — and only when — that
// publish finds no responders, re-buffers it on the buddy-hosted failover
// OUTBOX. That is the signal that this site's own NATS is down, so the local
// buffer is unreachable and the site would otherwise stop federating outward.
//
// The same no-responders-only gate as ForwardWithFailover, for the same reason:
// the two OUTBOX streams have independent dedup windows, so redirecting a timed
// out publish that may already have landed would enqueue the event twice.
//
// A nil failover publisher (a single-site deployment, or a buddy that never
// bound) returns the live publish's error unchanged.
func PublishWithFailover(ctx context.Context, live, failover PublishFunc,
	originSiteID, roomID, destSiteID string, eventType model.InboxEventType, payload []byte, dedupID string, ts int64,
) error {
	err := PublishTo(ctx, live, subject.LaneHome, originSiteID, roomID, destSiteID, eventType, payload, dedupID, ts)
	if err == nil || failover == nil || !IsNoResponders(err) {
		return err
	}
	return PublishTo(ctx, failover, subject.LaneFailover, originSiteID, roomID, destSiteID, eventType, payload, dedupID, ts)
}

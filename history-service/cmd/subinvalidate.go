package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	o11ynats "github.com/flywindy/o11y/nats"
	"github.com/nats-io/nats.go"

	"github.com/hmchangw/chat/pkg/subject"
)

// evictor is the subset of *readcache.SubscriptionCache the invalidation
// subscription drives. Declared at the consumer so the decode step is testable
// with a spy rather than a live cache.
type evictor interface {
	Evict(account, roomID string)
}

// subUpdateEnvelope is the minimal projection shared by both events published on
// the subscription.update subject: model.SubscriptionUpdateEvent ("added", …)
// and model.SubscriptionRemovedEvent ("removed"). Both nest roomId under
// "subscription", and both carry the action at top level — so this single shape
// decodes either without depending on which one arrived.
type subUpdateEnvelope struct {
	Action       string `json:"action"`
	Subscription struct {
		RoomID string `json:"roomId"`
	} `json:"subscription"`
}

// isAccessBoundaryAction reports whether a subscription.update action changes a
// member's history-access window (subscribed / historySharedSince). Only these
// need an eviction: a re-add — including "Don't include history" — publishes
// "added"; a remove publishes "removed". mute/favorite/read/role/section leave
// the boundary untouched, and evicting on the high-frequency "read" would churn
// the cache for no correctness gain.
func isAccessBoundaryAction(action string) bool {
	return action == "added" || action == "removed"
}

// evictOnSubscriptionUpdate decodes one subscription.update message and evicts
// the (account, roomID) access-check entry when the action changes the access
// boundary. account comes from the subject (the encoded token the event was
// addressed to); roomID from the payload. Best-effort: an unparseable subject or
// payload logs and returns without evicting — never panics, never crashes the
// consumer.
func evictOnSubscriptionUpdate(ev evictor, subj string, payload []byte) {
	token, ok := subject.ParseSubscriptionUpdateAccount(subj)
	if !ok {
		slog.Warn("subscription.update: unparseable subject, skipping eviction", "subject", subj)
		return
	}
	// The subject token is the encoded transport form; decode it so the cache key
	// matches the decoded account natsrouter hands the read handlers (a ".bot"
	// account is stored decoded). A no-op for every non-bot account.
	account := subject.DecodeAccount(token)
	var env subUpdateEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		slog.Warn("subscription.update: decode failed, skipping eviction", "error", err, "account", account)
		return
	}
	if !isAccessBoundaryAction(env.Action) || env.Subscription.RoomID == "" {
		return
	}
	ev.Evict(account, env.Subscription.RoomID)
}

// startSubCacheInvalidation subscribes to every account's subscription.update
// and evicts history-service's access-check cache on membership changes, closing
// the stale-full-access window (#414). Non-queue core NATS: every instance must
// receive every event to evict its own local cache. Best-effort — a dropped
// event just falls back to the cache TTL. nc.Drain at shutdown tears the
// subscription down, so the returned handle is discarded.
func startSubCacheInvalidation(ctx context.Context, nc *o11ynats.Conn, ev evictor) error {
	_, err := nc.Subscribe(ctx, subject.SubscriptionUpdateWildcard(), func(_ context.Context, msg *nats.Msg) {
		evictOnSubscriptionUpdate(ev, msg.Subject, msg.Data)
	})
	if err != nil {
		return fmt.Errorf("subscribe to subscription updates: %w", err)
	}
	return nil
}

package valkeyutil

import (
	"context"
	"log/slog"
	"time"
)

// This file holds the policy half of a read-through L2 tier. ReadCachedJSON
// above is the read half; these are the three decisions every tier in this repo
// otherwise re-implements — when an entry is due for re-validation, how its
// deadline is re-armed when the source of truth cannot answer, and how it is
// dropped when a write makes it wrong.
//
// They live together because they are one policy. The refresh window only works
// if it leaves room for the slide, and the slide only stays safe because it
// re-arms rather than rewrites. Six copies of that reasoning drifted; one does not.

// bustTimeout bounds every invalidation so a hung Valkey cannot stall the write
// path that triggered it. The authoritative write has already landed by then and
// the entry's TTL reconciles a missed bust, so waiting buys nothing.
const bustTimeout = 2 * time.Second

// RefreshAfter is how long an entry may be served before it is re-validated:
// three quarters of its TTL.
//
// The remaining quarter is the outage headroom. Re-validation is what discovers
// the source of truth is unreachable, so it has to happen early enough that the
// entry can still be slid — refreshing at the deadline would leave nothing to
// extend. It must also exceed the process-local L1 TTL in front of the tier,
// or every L1 miss turns into a source read and the L2 absorbs nothing.
func RefreshAfter(ttl time.Duration) time.Duration { return ttl / 4 * 3 }

// Fresh reports whether an entry stamped at cachedAt (Unix milliseconds) can be
// served as-is. A false means re-validate, not evict: the entry is still within
// its TTL and stays serveable if the re-validation cannot complete.
func Fresh(cachedAt int64, now time.Time, ttl time.Duration) bool {
	return now.Sub(time.UnixMilli(cachedAt)) < RefreshAfter(ttl)
}

// SlideTTL re-arms an entry's deadline without rewriting its value, so a tier
// keeps answering while its source of truth is unreachable.
//
// EXPIRE, deliberately, not SET. An entry can be busted between the read that
// produced the value and this call — exactly the window a membership change or
// a revocation lands in — and EXPIRE no-ops on an absent key where a SET would
// resurrect it, handing back access the source had already withdrawn.
//
// Best-effort: the caller is already serving a stale-but-valid entry, so a
// failure warns and leaves the current deadline in place.
func SlideTTL(ctx context.Context, client Client, key string, ttl time.Duration, label string) {
	if client == nil {
		return
	}
	if _, err := client.Expire(ctx, key, ttl); err != nil {
		slog.WarnContext(ctx, label+" L2 TTL slide failed (entry keeps its current deadline)",
			"key", key, "error", err)
	}
}

// BustKeys best-effort deletes entries an authoritative write has made wrong.
// Fail-open: a nil client or no keys is a no-op, and any error warns and is
// swallowed, since the TTL reconciles a missed bust.
func BustKeys(ctx context.Context, client Client, label string, keys ...string) {
	if client == nil || len(keys) == 0 {
		return
	}
	bustCtx, cancel := context.WithTimeout(ctx, bustTimeout)
	defer cancel()
	if err := client.Del(bustCtx, keys...); err != nil {
		slog.WarnContext(ctx, label+" L2 invalidate failed (TTL will reconcile)",
			"count", len(keys), "error", err)
	}
}

// NoopRecorder satisfies CacheRecorder and does nothing, for tiers constructed
// without metrics. Saves each package hand-writing three empty methods.
type NoopRecorder struct{}

func (NoopRecorder) Hit(context.Context)   {}
func (NoopRecorder) Miss(context.Context)  {}
func (NoopRecorder) Error(context.Context) {}

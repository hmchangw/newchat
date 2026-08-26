package valkeyutil

import (
	"context"
	"log/slog"
	"slices"
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

// bustBatchSize caps the keys in one invalidation call. A room-wide mutation
// busts one key per member per key generation, so an unbounded call would build
// a pipeline proportional to room size. Batching keeps it bounded; the extra
// round trips are off the hot path.
const bustBatchSize = 256

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
//
// A non-positive ttl is refused rather than passed through: Valkey treats
// EXPIRE key 0 as an immediate DELETE, so a tier misconfigured to a zero TTL
// would evict on every slide — the exact inverse of re-arming.
func SlideTTL(ctx context.Context, client Client, key string, ttl time.Duration, label string) {
	if client == nil || ttl <= 0 {
		return
	}
	// WithoutCancel, for the same reason BustKeys drops cancellation. A slide
	// runs precisely when the source read just failed, and one ordinary cause of
	// that failure is the caller's own context expiring — so inheriting it would
	// kill the EXPIRE too, leave the entry on its original deadline, and let it
	// lapse during the outage this tier exists to survive. The timeout still
	// bounds the call; only cancellation is dropped.
	slideCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bustTimeout)
	defer cancel()
	if _, err := client.Expire(slideCtx, key, ttl); err != nil {
		slog.WarnContext(ctx, label+" L2 TTL slide failed (entry keeps its current deadline)",
			"key", key, "error", err)
	}
}

// BustKeys best-effort deletes entries an authoritative write has made wrong.
// Fail-open: a nil client or no keys is a no-op, and any error warns and is
// swallowed, since the TTL reconciles a missed bust.
//
// Callers may pass any key set. Spreading the keys across cluster slots is the
// client's job (see clusterClient.Del) — leaving it to each caller had already
// cost one production defect and produced three hand-rolled obediences.
func BustKeys(ctx context.Context, client Client, label string, keys ...string) {
	if client == nil || len(keys) == 0 {
		return
	}
	// WithoutCancel first: a bust runs after the authoritative write has already
	// committed, so inheriting the caller's cancellation would let a finished
	// request skip the DEL and leave the cache serving what that write just
	// invalidated, for a full TTL. The timeout still bounds the call — only
	// cancellation is dropped, never the deadline. One budget covers every
	// batch, so a slow Valkey cannot multiply the bound by the batch count.
	bustCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bustTimeout)
	defer cancel()

	for batch := range slices.Chunk(keys, bustBatchSize) {
		// Each batch is independent: one failure must not skip the rest.
		if err := client.Del(bustCtx, batch...); err != nil {
			slog.WarnContext(ctx, label+" L2 invalidate failed (TTL will reconcile)",
				"count", len(batch), "error", err)
		}
	}
}

// KV is one key/value pair for SetMany.
type KV struct {
	Key   string
	Value string
}

// multiSetter is an optional Client capability: storing many keys in one round
// trip. It is deliberately NOT part of Client — sixteen test doubles implement
// that interface, and widening it would churn all of them to reach one call
// site. clusterClient satisfies this; anything that does not still works
// through SetMany's fallback.
type multiSetter interface {
	MSet(ctx context.Context, entries []KV, ttl time.Duration) error
}

// SetMany stores every entry under one TTL, in a single round trip where the
// client supports it and a Set loop where it does not.
//
// The read side has always been one round trip (see Client.MGet); the write
// side was one per key. That asymmetry is only visible under load: a bulk fill
// writes one entry per user in a mention list, whose size the sender chooses,
// on the message hot path — so a serialized loop turns a wide mention into a
// long chain of round trips right where latency is most visible.
//
// Best-effort, like every other cache write: a failure warns and is swallowed,
// since the caller already holds the values it was trying to store and the
// source of truth can answer again. A non-positive ttl is refused — Set with a
// zero TTL stores without expiry, which for a cache is a leak, not a long life.
func SetMany(ctx context.Context, client Client, entries []KV, ttl time.Duration, label string) {
	if client == nil || len(entries) == 0 || ttl <= 0 {
		return
	}
	if ms, ok := client.(multiSetter); ok {
		if err := ms.MSet(ctx, entries, ttl); err != nil {
			slog.WarnContext(ctx, label+" L2 bulk write failed (TTL will reconcile)",
				"count", len(entries), "error", err)
		}
		return
	}
	for _, e := range entries {
		if err := client.Set(ctx, e.Key, e.Value, ttl); err != nil {
			slog.WarnContext(ctx, label+" L2 write failed (TTL will reconcile)",
				"key", e.Key, "error", err)
		}
	}
}

// NoopRecorder satisfies CacheRecorder and does nothing, for tiers constructed
// without metrics. Saves each package hand-writing three empty methods.
type NoopRecorder struct{}

func (NoopRecorder) Hit(context.Context)   {}
func (NoopRecorder) Miss(context.Context)  {}
func (NoopRecorder) Error(context.Context) {}

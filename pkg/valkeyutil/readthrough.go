package valkeyutil

import (
	"context"
	"log/slog"
	"time"
)

// A read-through cache tier over Valkey. tier.go holds the pieces it is built
// from (RefreshAfter, SlideTTL, BustKeys); valkey.go holds the read
// (ReadCachedJSON).
//
//	read cache
//	  hit, fresh   -> serve
//	  hit, stale   -> reload:
//	                    source down     -> extend the deadline, serve the cached value
//	                    confirmed gone  -> delete, report absent
//	                    confirmed       -> rewrite with a fresh stamp, serve
//	  miss         -> load; store only a confirmed value
//
// Used by subauthcache, sessioncache, roommetacache and atrest. Only atrest
// differs, via SlideOnFresh and KeepOnAbsent.
//
// roomsubcache and userstore stay on their own copies on purpose: roomsubcache
// needs a byte cap on the read plus request coalescing, and userstore stores one
// record under two keys and has a bulk read. Both still share the pieces above.

// Box is the stored envelope for every tier: the value plus when the source last
// confirmed it, in Unix milliseconds.
//
// Owned here rather than declared per package. Each tier used to write its own
// envelope, its own accessor and its own "is this usable" method, and they
// drifted — two checked the stamp, two did not, so an entry written before the
// envelope existed reloaded from the source on every single read.
type Box[T any] struct {
	V        T     `json:"v"`
	CachedAt int64 `json:"cachedAt"`
}

// usable reports whether a decoded box is worth serving. A zero stamp means the
// key holds something that is not a Box — an older format, or a foreign value —
// and valid gives the tier its own say on the payload.
func (b *Box[T]) usable(valid func(*T) bool) bool {
	return b.CachedAt != 0 && (valid == nil || valid(&b.V))
}

// TierConfig wires a Tier. Client, TTL, Key and Load are required; a nil
// Client or a non-positive TTL disables the L2 and every read goes to Load.
type TierConfig[K, V any] struct {
	// Client is the Valkey handle. Nil disables the tier, so a deployment
	// without Valkey wires the same code path.
	Client Client
	// TTL is the entry's lifetime, and via RefreshAfter also sets the reload
	// window. Keep it above the TTL of any in-process cache in front, or every
	// miss there triggers a reload here and the cache absorbs nothing.
	TTL time.Duration
	// Label names the cache in log messages.
	Label string
	// Rec records hit/miss/error outcomes. Nil records nothing.
	Rec CacheRecorder
	// Key maps an identifier to its Valkey key.
	Key func(K) string
	// Load reads one id from the source of truth:
	//
	//	(value, true,  nil) — found
	//	(zero,  false, nil) — confirmed missing
	//	(zero,  false, err) — the source could not answer
	//
	// Report "missing" and "could not answer" separately, or a deletion and an
	// outage look the same and the tier deletes on both. Put a circuit breaker
	// here, not around Resolve, so an open breaker still serves cached entries.
	Load func(ctx context.Context, id K) (V, bool, error)
	// Valid rejects a decoded value that is not worth serving — one with no id on
	// it, say. Nil accepts anything that decodes. The stamp is checked either way.
	Valid func(*V) bool
	// SlideOnFresh extends the deadline on a fresh serve as well.
	//
	// For a tier fronted by a cache whose TTL outlives the reload window: every
	// hit arrives before the window, so the entry is never extended and expires
	// mid-outage. Sliding moves only the expiry, never CachedAt, so staleness is
	// unchanged. Off by default — otherwise it is a wasted round trip per read.
	SlideOnFresh bool
	// KeepOnAbsent serves and restamps a cached entry the source now reports
	// absent, instead of evicting it.
	//
	// For a tier over rows nothing deletes, where an absence is lag or an
	// anomaly and acting on it costs more than serving a stale value. Restamping
	// (rather than sliding) is what makes the retry once per window rather than
	// once per read. Off by default: for everything else an absence is a real
	// deletion and must take effect now rather than at the TTL.
	KeepOnAbsent bool
}

// Tier caches one id through Valkey in front of a source of truth. Two
// properties, both from the entry's own age:
//
//   - An entry past the reload window is re-checked, so a missed invalidation is
//     corrected within that window instead of lasting a full TTL.
//   - If that re-check fails, the deadline is extended, so an entry that keeps
//     being read survives an outage of any length.
//
// Only confirmed values are stored, so the cache can never serve something the
// source did not return. An id that was never cached still fails.
type Tier[K, V any] struct {
	cfg TierConfig[K, V]
	now func() time.Time // overridden in tests
}

// NewTier returns a Tier over cfg. A nil Rec records nothing. Build it once and
// keep it: the closures in cfg go on the heap, so rebuilding per call allocates.
func NewTier[K, V any](cfg TierConfig[K, V]) Tier[K, V] {
	return NewTierWithClock(cfg, time.Now)
}

// NewTierWithClock is NewTier with an injected clock. Every decision here
// compares against now, so tests need to move it rather than sleep.
func NewTierWithClock[K, V any](cfg TierConfig[K, V], now func() time.Time) Tier[K, V] {
	if cfg.Rec == nil {
		cfg.Rec = NoopRecorder{}
	}
	if now == nil {
		now = time.Now
	}
	return Tier[K, V]{cfg: cfg, now: now}
}

// enabled reports whether the cache is in play. A zero or negative TTL turns it
// off: Valkey reads a zero TTL as "keep forever", which is not what "0 disables
// the cache" should mean.
func (t *Tier[K, V]) enabled() bool { return t.cfg.Client != nil && t.cfg.TTL > 0 }

// Resolve returns the entry for id, with the same three results as Load. An
// error means the source could not answer AND nothing was cached, so an outage
// with a cached entry reads as success and one without it fails.
//
// A Valkey error just falls through to Load; only Load's result sets the error.
func (t *Tier[K, V]) Resolve(ctx context.Context, id K) (V, bool, error) {
	var zero V
	if t.enabled() {
		key := t.cfg.Key(id)
		if box, found := ReadCachedJSON(ctx, t.cfg.Client, key, t.cfg.Label,
			t.cfg.Rec, func(b *Box[V]) bool { return b.usable(t.cfg.Valid) }); found {
			return t.serveHit(ctx, id, key, box)
		}
	}

	v, found, err := t.cfg.Load(ctx, id)
	switch {
	case err != nil:
		return zero, false, err
	case !found:
		return zero, false, nil
	}
	t.write(ctx, id, v)
	return v, true, nil
}

// serveHit handles a cache hit. Inside the reload window it is a plain read.
// Past it, the entry is re-checked and the three outcomes differ:
//
//   - Source down: extend the deadline and keep serving, so an outage cannot
//     revoke an entry. EXPIRE, not SET — a SET would restore an entry that a
//     write site deleted in between.
//   - Confirmed gone: delete and report it. Serving it instead would keep a
//     deleted thing alive forever, since every later read extends it again.
//     KeepOnAbsent reverses this for rows nothing ever deletes.
//   - Confirmed present: rewrite with a fresh stamp.
func (t *Tier[K, V]) serveHit(ctx context.Context, id K, key string, box Box[V]) (V, bool, error) {
	if Fresh(box.CachedAt, t.now(), t.cfg.TTL) {
		if t.cfg.SlideOnFresh {
			t.slide(ctx, key)
		}
		return box.V, true, nil
	}

	loaded, found, err := t.cfg.Load(ctx, id)
	switch {
	case err != nil:
		t.slide(ctx, key)
		return box.V, true, nil //nolint:nilerr // fail-open by design; see above
	case !found && t.cfg.KeepOnAbsent:
		// Restamp rather than slide: the value is unchanged, but the reload clock
		// has to advance or every later read hits the source again.
		slog.WarnContext(ctx, t.cfg.Label+" L2 refresh found nothing, keeping the cached entry")
		t.write(ctx, id, box.V)
		return box.V, true, nil
	case !found:
		t.bust(ctx, id)
		var zero V
		return zero, false, nil
	}
	t.write(ctx, id, loaded)
	return loaded, true, nil
}

// write stores v stamped now. Never stores what Valid would refuse to serve: it
// would read back as a miss every time, costing a round trip and caching nothing.
func (t *Tier[K, V]) write(ctx context.Context, id K, v V) {
	if !t.enabled() || (t.cfg.Valid != nil && !t.cfg.Valid(&v)) {
		return
	}
	box := Box[V]{V: v, CachedAt: t.now().UnixMilli()}
	if err := SetJSONWithTTL(ctx, t.cfg.Client, t.cfg.Key(id), box, t.cfg.TTL); err != nil {
		slog.WarnContext(ctx, t.cfg.Label+" L2 write failed (TTL will reconcile)", "error", err)
	}
}

func (t *Tier[K, V]) slide(ctx context.Context, key string) {
	SlideTTL(ctx, t.cfg.Client, key, t.cfg.TTL, t.cfg.Label)
}

// bust drops an entry the source says is gone. Gated on the client, not
// enabled(), so keys written while the TTL was set can still be cleared. Each
// package exports its own invalidation on top, since most clear more than one
// key per id.
func (t *Tier[K, V]) bust(ctx context.Context, id K) {
	if t.cfg.Client == nil {
		return
	}
	BustKeys(ctx, t.cfg.Client, t.cfg.Label, t.cfg.Key(id))
}

package valkeyutil

import (
	"context"
	"log/slog"
	"time"
)

// This file holds the control flow of a read-through L2 tier. tier.go holds the
// three policy primitives it is built from (RefreshAfter, SlideTTL, BustKeys)
// and valkey.go the read half (ReadCachedJSON).
//
// Six tiers in this repo — subauth, session, room meta, room members, users and
// the room DEK — arrived at the same state machine independently, each wrapping
// it in a different shape: a Tier struct with a baked-in loader, a free function
// with functional options, a Lookup with a decorated loader. The policy had
// already been hoisted here; the flow had not, so a fix to one tier's stale-hit
// branch reached none of the others.
//
// What every one of them does:
//
//	read L2
//	  hit, fresh   -> serve
//	  hit, stale   -> re-validate:
//	                    unreachable      -> slide the deadline, serve the cached value
//	                    confirmed absent -> evict, report absent
//	                    confirmed        -> rewrite with a fresh stamp, serve
//	  miss         -> load; write only a confirmed value; report what the source said
//
// Four tiers use this: subauthcache, sessioncache, roommetacache and atrest.
// Only atrest deviates, in two ways that are load-bearing rather than
// accidental, so they are options here rather than reasons to keep a hand-rolled
// copy: SlideOnFresh and KeepOnAbsent. See their fields.
//
// Two tiers deliberately do NOT use this, and neither is an oversight:
//
//   - pkg/roomsubcache maps one room to a member list that can be large, so its
//     read applies a byte cap the shared read half cannot express (it needs the
//     raw length, which ReadCachedJSON has already discarded), and it collapses
//     concurrent misses AND concurrent refreshes through singleflight because it
//     is the one tier with no process-local L1 in front of it. Adopting it here
//     would mean a size cap and a singleflight mode with exactly one user each.
//   - pkg/userstore stores one record under two key spaces (by id and by
//     account), so a write populates two keys and an eviction drops two — while
//     this Tier is one identifier to one key throughout. It also has a bulk
//     (MGet) path this models nothing of. Half-migrating it would leave two
//     shapes inside one package, which is worse than the one it has.
//
// Both still use the shared policy primitives (Fresh, SlideTTL, BustKeys), so
// what they duplicate is the branch structure, not the decisions.

// Entry is a cache envelope: a payload, a confirmation stamp, and a rule for
// whether what decoded is worth serving.
//
// Stamped returns when the source of truth last confirmed the entry, in Unix
// milliseconds. Declare it as a field with a `json:"cachedAt"` tag and return it
// from a one-line method rather than embedding a shared type: an embedded field
// cannot be set in a composite literal, and every tier's tests build one. The
// field name is each envelope's own, so a tier adopting this interface keeps
// byte-identical wire format — no key-generation bump, no cold-cache window.
//
// It is deliberately separate from any domain timestamp on the payload. A row's
// own CreatedAt says when the thing happened; this says when the cache last
// checked, and only a real re-validation may advance it.
//
// Usable is the guard against a well-formed value that is not a real entry. Any
// valid JSON unmarshals into a struct without error, so "null", an envelope from
// an older build, or a foreign value written under the same key all decode to a
// zero payload — and without this they would be served for the rest of the TTL.
// Gate it on a field the source of truth always populates.
type Entry interface {
	Stamped() int64
	Usable() bool
}

// TierConfig wires a Tier. Client, TTL, Key, Load and Stamp are required; a nil
// Client or a non-positive TTL disables the L2 and every read goes to Load.
type TierConfig[K any, E Entry] struct {
	// Client is the Valkey handle. Nil disables the tier, so a deployment
	// without Valkey wires the same code path.
	Client Client
	// TTL is the entry's lifetime, and through RefreshAfter it also fixes the
	// re-validation window. It must exceed the TTL of any process-local L1 in
	// front of this tier, or every L1 miss lands past the window and the refresh
	// degenerates into a source read on every miss — the exact load the L2 exists
	// to absorb.
	TTL time.Duration
	// Label names the cache in log messages.
	Label string
	// Rec records hit/miss/error outcomes. Nil records nothing.
	Rec CacheRecorder
	// Key maps an identifier to its Valkey key.
	Key func(K) string
	// Load resolves an identifier from the source of truth. The three results
	// are distinct and the tier acts differently on each:
	//
	//	(entry, true,  nil) — confirmed present
	//	(zero,  false, nil) — confirmed absent; a decision, not a failure
	//	(zero,  false, err) — the source could not answer
	//
	// Collapsing absent into an error would make an outage indistinguishable
	// from a deletion, and the tier would evict on both.
	//
	// Wrap a circuit breaker here rather than around Resolve: an open breaker
	// must still serve cached entries, since during the outage that opened it
	// they are the only thing that can answer.
	Load func(ctx context.Context, id K) (E, bool, error)
	// Stamp returns e with its confirmation stamp set to ms. One line for an
	// envelope embedding Stamp: func(e E, ms int64) E { e.CachedAt = ms; return e }.
	Stamp func(e E, ms int64) E
	// SlideOnFresh re-arms the deadline on a fresh serve too.
	//
	// For a tier whose L1 TTL outlives the refresh window: every L2 hit then
	// lands before the window, so the entry is served, never re-armed, and
	// expires on its original deadline — during the outage the tier exists to
	// survive. Safe because a slide moves the eviction deadline and never
	// CachedAt, so the staleness bound is unchanged. Off by default: for a tier
	// read more often than its window it is a wasted round trip per read.
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

// Tier resolves an identifier through a Valkey L2 in front of a source of truth,
// and owns the refresh-and-survive policy that makes the cache useful during an
// outage rather than merely fast.
//
// Two guarantees, both driven by the entry's own age:
//
//   - Freshness. An entry past the refresh window is re-validated, so a change
//     whose invalidation was swallowed is corrected within that window rather
//     than living out the full TTL.
//   - Outage survival. When that re-validation fails, the deadline is re-armed,
//     so an entry that keeps being read stays reachable for an outage of any
//     length instead of lapsing one TTL after it was last confirmed.
//
// Positive-only, and every tier built on it inherits this: a confirmed absence
// is never written, so the cache can only ever serve what the source of truth
// already returned. A cold identifier still fails closed.
type Tier[K any, E Entry] struct {
	cfg TierConfig[K, E]
	now func() time.Time // overridden in tests
}

// NewTier returns a Tier over cfg. A nil Rec records nothing.
//
// It returns a value rather than a pointer so a caller with no long-lived place
// to hold one can build it per call without a heap allocation — roommetacache's
// ReadThrough does exactly that on the per-message path. Callers that do have
// somewhere to keep it should store the value and call through it; a Tier is
// immutable after construction, so a copy is as good as the original.
func NewTier[K any, E Entry](cfg TierConfig[K, E]) Tier[K, E] {
	return NewTierWithClock(cfg, time.Now)
}

// NewTierWithClock is NewTier with an injected clock, for a package whose own
// tests drive the refresh window. Every decision this tier makes is a comparison
// against now, so a test that cannot move the clock has to sleep.
func NewTierWithClock[K any, E Entry](cfg TierConfig[K, E], now func() time.Time) Tier[K, E] {
	if cfg.Rec == nil {
		cfg.Rec = NoopRecorder{}
	}
	if now == nil {
		now = time.Now
	}
	return Tier[K, E]{cfg: cfg, now: now}
}

// enabled reports whether the L2 is in play. A non-positive TTL disables it
// outright: Valkey treats a zero TTL as "store forever", so honoring the "0
// disables the cache" config convention any other way would store entries with
// no expiry.
func (t *Tier[K, E]) enabled() bool { return t.cfg.Client != nil && t.cfg.TTL > 0 }

// Resolve returns the entry for id.
//
// The result mirrors Load's contract: (entry, true, nil) when present,
// (zero, false, nil) for a confirmed absence, and an error only when the source
// of truth could not answer AND nothing cached could stand in for it. An outage
// with a warm entry is therefore reported as a success — that is the fail-open
// this tier exists for — while a cold one fails closed.
//
// Fail-open on the cache too: any Valkey error degrades to Load, and only Load's
// result governs the returned error.
func (t *Tier[K, E]) Resolve(ctx context.Context, id K) (E, bool, error) {
	var zero E
	if t.enabled() {
		key := t.cfg.Key(id)
		if entry, found := ReadCachedJSON(ctx, t.cfg.Client, key, t.cfg.Label,
			t.cfg.Rec, func(e *E) bool { return (*e).Usable() }); found {
			return t.serveHit(ctx, id, key, entry)
		}
	}

	entry, found, err := t.cfg.Load(ctx, id)
	if err != nil {
		return zero, false, err
	}
	if !found {
		return zero, false, nil
	}
	stamped := t.stamp(entry)
	t.write(ctx, id, stamped)
	return stamped, true, nil
}

// serveHit resolves a cache hit, and is where both of the tier's guarantees live.
//
// An entry confirmed within the refresh window is served as a pure read. An
// older one is re-validated, and the three outcomes are deliberately not
// interchangeable:
//
//   - Unreachable: re-arm the deadline and keep serving. Swallowing the error is
//     the point — an outage must not revoke an entry the source confirmed once.
//     EXPIRE rather than SET, so an entry busted between the read and here stays
//     busted; a SET would resurrect exactly what the write site just withdrew.
//   - Confirmed absent: evict and report it. Serving the cached entry here would
//     keep a deleted thing alive indefinitely, because every later read lands on
//     this same branch and re-arms the deadline again — it would outlive its TTL
//     for as long as anyone reads it. KeepOnAbsent inverts this for a tier whose
//     rows are never deleted.
//   - Confirmed present: rewrite with a fresh stamp, picking up whatever a
//     missed invalidation left behind.
func (t *Tier[K, E]) serveHit(ctx context.Context, id K, key string, entry E) (E, bool, error) {
	if Fresh(entry.Stamped(), t.now(), t.cfg.TTL) {
		if t.cfg.SlideOnFresh {
			t.slide(ctx, key)
		}
		return entry, true, nil
	}

	loaded, found, err := t.cfg.Load(ctx, id)
	switch {
	case err != nil:
		t.slide(ctx, key)
		return entry, true, nil //nolint:nilerr // fail-open by design; see above
	case !found && t.cfg.KeepOnAbsent:
		// Restamped, not slid: the value is unchanged but the refresh clock must
		// advance, or every subsequent read re-runs this and hits the source again.
		slog.WarnContext(ctx, t.cfg.Label+" L2 refresh found nothing, keeping the cached entry")
		restamped := t.stamp(entry)
		t.write(ctx, id, restamped)
		return restamped, true, nil
	case !found:
		t.bust(ctx, id)
		var zero E
		return zero, false, nil
	default:
		stamped := t.stamp(loaded)
		t.write(ctx, id, stamped)
		return stamped, true, nil
	}
}

// stamp marks an entry as confirmed now.
func (t *Tier[K, E]) stamp(e E) E { return t.cfg.Stamp(e, t.now().UnixMilli()) }

func (t *Tier[K, E]) write(ctx context.Context, id K, stamped E) {
	if !t.enabled() {
		return
	}
	// Usable gates the write as well as the read. A tier must never store what it
	// would refuse to serve: an entry that fails the rule is written once and then
	// read back as a miss on every subsequent request, so it costs a round trip
	// per read and caches nothing. It is also the guard against a source that
	// answers with a hollow record — found, but with no identity on it.
	if !stamped.Usable() {
		return
	}
	if err := SetJSONWithTTL(ctx, t.cfg.Client, t.cfg.Key(id), stamped, t.cfg.TTL); err != nil {
		slog.WarnContext(ctx, t.cfg.Label+" L2 write failed (TTL will reconcile)", "error", err)
	}
}

func (t *Tier[K, E]) slide(ctx context.Context, key string) {
	SlideTTL(ctx, t.cfg.Client, key, t.cfg.TTL, t.cfg.Label)
}

// bust drops an entry a confirmed absence has made wrong.
//
// Gated on the client alone rather than on enabled(): a tier disabled by a
// zero TTL can still be looking at keys written while it was enabled, and those
// must still be droppable. Every package wraps its own exported invalidation on
// top of this, because each has a wider key set to clear than one id — two key
// generations, two key spaces, or every member of a room.
func (t *Tier[K, E]) bust(ctx context.Context, id K) {
	if t.cfg.Client == nil {
		return
	}
	BustKeys(ctx, t.cfg.Client, t.cfg.Label, t.cfg.Key(id))
}

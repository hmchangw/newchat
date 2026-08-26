package roommetacache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Recorder records the outcome of an L2 cache lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// cacheKeySchemaVersion namespaces cache keys by the stored value's shape,
// mirroring pkg/roomsubcache. Bump whenever the value changes such that a
// binary built against the other shape would decode it without error but with
// the wrong contents — Valkey has no schema check, so the version segment is
// what makes those entries miss and be repopulated instead. Bumped to v2 when
// the value went from a bare Meta to the cachedMeta{Meta, CachedAt} envelope:
// an older binary decodes that envelope into Meta with no JSON error (both
// envelope fields are simply unknown) and every field zero, which during a
// rolling deploy makes an old broadcast-worker drop fan-out on an empty room
// type and an old message-gatekeeper read UserCount 0.
const cacheKeySchemaVersion = "v2"

// MetaKey is the L2 (Valkey) key for a room's cached Meta. The {roomID}
// hash tag colocates it in the same cluster slot as the room's encryption
// key (pkg/roomkeystore), matching house convention for room-scoped keys.
// The version segment trails the key so the hash tag keeps that colocation.
func MetaKey(roomID string) string {
	return "room:{" + roomID + "}:meta:" + cacheKeySchemaVersion
}

// legacyMetaKey is the pre-v2 key, still written and read by binaries that
// predate the cachedMeta envelope. Only invalidation touches it: a bust must
// clear both generations for as long as a rolling deploy can have both binaries
// live, or an old pod serves a room that a new pod just renamed or deleted.
// Delete this (and its use in BustMeta) once no pre-v2 binary can be running.
func legacyMetaKey(roomID string) string {
	return "room:{" + roomID + "}:meta"
}

// tierOption configures a tier at construction. Unexported: every production
// caller passes a breaker, which NewL2Tier takes directly. These exist so this
// package's own tests can drive the guard and the fetch without a live Mongo,
// and the constructors for them live in the test file.
type tierOption func(*tierOpts)

type tierOpts struct {
	guard func(func() error) error
	// fetch defaults to FetchFromMongo. Overridden only by this package's tests,
	// so the read-through's cache decisions can be exercised without a Mongo.
	fetch func(context.Context, *mongo.Collection, string) (Meta, error)
}

// L2Tier resolves a room Meta through the L2 (Valkey) tier: GET on the cache
// key, and on miss (or any L2 error) fall back to Mongo and repopulate L2 with
// the configured TTL. Fail-open — a nil client or any Valkey error degrades to a
// direct Mongo read; only the Mongo result governs the returned error. Intended
// as the terminal loader behind the L1 roommetacache.Cache.
//
// Construct it ONCE per store and hold it. It was a per-call free function
// until the shared tier landed, and the closures a tier is configured with
// (Load, Stamp, the clock) escape to the heap however the tier itself is
// returned — three allocations on a path that runs once per message through
// broadcast-worker's fan-out. Holding the tier moves that to startup.
type L2Tier struct {
	tier   valkeyutil.Tier[string, cachedMeta]
	client valkeyutil.Client
}

// NewL2Tier wires the tier over a rooms collection.
//
// breaker guards the Mongo read so a miss fails fast during an outage instead of
// waiting out Mongo's timeout. Nil means no guard. It covers only the Mongo
// read, never the Valkey one — an open breaker must still serve cached rooms.
// Argument order matches subauthcache.NewTier and atrest.NewL2DEKStore.
//
// rec records hit/miss/error; pass cachemetrics.For("roommeta", "l2") so every
// service reports the same series.
func NewL2Tier(client valkeyutil.Client, rooms *mongo.Collection, ttl time.Duration, breaker *circuitbreaker.Breaker, rec Recorder) *L2Tier {
	var opts []tierOption
	if breaker != nil {
		opts = append(opts, func(o *tierOpts) { o.guard = breaker.Do })
	}
	return newL2TierWithClock(client, rooms, ttl, rec, time.Now, opts...)
}

// newL2TierWithClock is NewL2Tier with an injected clock and raw options, for
// this package's own refresh-window and guard tests.
func newL2TierWithClock(client valkeyutil.Client, rooms *mongo.Collection, ttl time.Duration, rec Recorder, now func() time.Time, opts ...tierOption) *L2Tier {
	o := tierOpts{fetch: FetchFromMongo}
	for _, opt := range opts {
		opt(&o)
	}
	t := &L2Tier{client: client}
	t.tier = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[string, cachedMeta]{
		Client: client,
		TTL:    ttl,
		Label:  "room meta",
		Rec:    rec,
		Key:    MetaKey,
		Load: func(ctx context.Context, id string) (cachedMeta, bool, error) {
			meta, err := fetchGuarded(ctx, &o, rooms, id)
			switch {
			// A missing room is a decision, not a failure, and must reach the tier
			// as a confirmed absence: collapsing it into an error would make an
			// outage indistinguishable from a deletion, and the tier evicts on one
			// and keeps serving on the other.
			case errors.Is(err, mongo.ErrNoDocuments):
				return cachedMeta{}, false, nil
			case err != nil:
				return cachedMeta{}, false, err
			}
			return cachedMeta{Meta: meta}, true, nil
		},
		Stamp: func(e cachedMeta, ms int64) cachedMeta { e.CachedAt = ms; return e },
	}, now)
	return t
}

// Get resolves roomID.
func (t *L2Tier) Get(ctx context.Context, roomID string) (Meta, error) {
	entry, found, err := t.tier.Resolve(ctx, roomID)
	if err != nil {
		return Meta{}, fmt.Errorf("l2 read-through: %w", err)
	}
	if !found {
		// Rebuilt rather than threaded through the tier, which reports an absence
		// as a bool by design. The wording matches FetchFromMongo's so a caller
		// cannot tell which path produced it, and it stays errors.Is-checkable
		// against mongo.ErrNoDocuments, which callers branch on.
		return Meta{}, fmt.Errorf("l2 read-through: fetch room meta %s: %w", roomID, mongo.ErrNoDocuments)
	}
	return entry.Meta, nil
}

// cachedMeta is the L2 envelope. The embedded Stamp records when Mongo last
// confirmed the entry, which is what lets a reader tell a fresh entry from one
// due for re-validation — and what makes surviving an outage possible rather
// than waiting for the key to vanish. The field is declared rather
// than embedded so composite literals keep working; see valkeyutil.Entry.
type cachedMeta struct {
	Meta     Meta  `json:"meta"`
	CachedAt int64 `json:"cachedAt"`
}

// Stamped reports when Mongo last confirmed the entry.
func (c cachedMeta) Stamped() int64 { return c.CachedAt } //nolint:gocritic // hugeParam: interface-required value receiver, see Usable

// Usable requires both a non-empty room ID and a confirmation stamp.
//
// FetchFromMongo always populates the ID, so a zero Meta means the key holds
// something that is not a Meta — and serving it would hand out an empty SiteID
// and type, which downstream routing reads. The stamp check is what makes an
// entry written before the envelope existed (a bare Meta) read as a miss and
// reload, rather than being served with a zero CachedAt that would mark it
// permanently stale.
//
// The value receiver is required, and so is the gocritic exemption below it:
// valkeyutil.Entry is satisfied by the type the tier stores, and a value type's
// method set excludes pointer-receiver methods. The copy is two per cache read,
// against a Valkey round trip.
func (c cachedMeta) Usable() bool { return c.Meta.ID != "" && c.CachedAt != 0 } //nolint:gocritic // hugeParam: see the note above

// fetchGuarded runs FetchFromMongo, inside guard when one was supplied. A nil
// guard (the common case) calls straight through.
func fetchGuarded(ctx context.Context, o *tierOpts, rooms *mongo.Collection, roomID string) (Meta, error) {
	if o.guard == nil {
		return o.fetch(ctx, rooms, roomID)
	}
	var meta Meta
	err := o.guard(func() error {
		var e error
		meta, e = o.fetch(ctx, rooms, roomID)
		return e
	})
	if err != nil {
		return Meta{}, err
	}
	return meta, nil
}

// BustMeta best-effort deletes a room's L2 Meta entry. Called from the write
// site (room-worker) after an authoritative Mongo write to name/userCount.
// Fail-open: a nil client is a no-op and any Valkey error logs at warn and is
// swallowed — the configured L2 TTL reconciles a missed bust.
// Both key generations are dropped: see legacyMetaKey.
func BustMeta(ctx context.Context, client valkeyutil.Client, roomID string) {
	valkeyutil.BustKeys(ctx, client, "room meta", MetaKey(roomID), legacyMetaKey(roomID))
}

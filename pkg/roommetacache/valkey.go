package roommetacache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Recorder records the outcome of an L2 cache lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// MetaKey is the L2 (Valkey) key for a room's cached Meta. The {roomID}
// hash tag colocates it in the same cluster slot as the room's encryption
// key (pkg/roomkeystore), matching house convention for room-scoped keys.
func MetaKey(roomID string) string {
	return "room:{" + roomID + "}:meta"
}

// readL2 attempts the L2 (Valkey) read. An entry with no room ID is not usable:
// FetchFromMongo always populates it, so a zero Meta means the key holds
// something that is not a Meta — and serving it would hand out an empty SiteID
// and type, which downstream routing reads.
// readL2 requires both a non-empty ID and a confirmation stamp. The stamp check
// is what makes an entry written before the envelope existed (a bare Meta) read
// as a miss and reload, rather than being served with a zero CachedAt that would
// mark it permanently stale.
func readL2(ctx context.Context, client valkeyutil.Client, roomID string, rec Recorder) (cachedMeta, bool) {
	return valkeyutil.ReadCachedJSON(ctx, client, MetaKey(roomID), "room meta", rec,
		func(c *cachedMeta) bool { return c.Meta.ID != "" && c.CachedAt != 0 }, "room_id", roomID)
}

// ReadThroughOption configures a ReadThrough call.
type ReadThroughOption func(*readThroughOpts)

type readThroughOpts struct {
	guard func(func() error) error
	// fetch defaults to FetchFromMongo. Overridden only by this package's tests,
	// so the read-through's cache decisions can be exercised without a Mongo.
	fetch func(context.Context, *mongo.Collection, string) (Meta, error)
}

func withFetcherForTest(f func(context.Context, *mongo.Collection, string) (Meta, error)) ReadThroughOption {
	return func(o *readThroughOpts) { o.fetch = f }
}

// WithFetchGuard runs the Mongo fetch inside guard — typically a circuit
// breaker's Do — so a cold miss fast-fails during an outage instead of stalling
// on Mongo's own timeout.
//
// The guard deliberately wraps only the fetch, never the L2 read. Fencing the
// whole read-through would make an open breaker refuse cached rooms too,
// disabling the L2 at precisely the moment it is the only tier that can answer.
func WithFetchGuard(guard func(fn func() error) error) ReadThroughOption {
	return func(o *readThroughOpts) { o.guard = guard }
}

// ReadThrough resolves a room Meta through the L2 (Valkey) tier: GET on the
// cache key, and on miss (or any L2 error) fall back to Mongo and repopulate
// L2 with the given TTL. It is fail-open — a nil client or any Valkey error
// degrades to a direct Mongo read; only the Mongo result governs the returned
// error. Intended to be the terminal loader behind the L1 roommetacache.Cache.
//
// rec records L2 hit/miss/error outcomes; callers pass a shared
// cachemetrics.For("roommeta", "l2") so every service emits the same series.
// A nil client (L2 disabled) records nothing — there is no L2 to hit or miss.
func ReadThrough(ctx context.Context, client valkeyutil.Client, rooms *mongo.Collection, roomID string, ttl time.Duration, rec Recorder, opts ...ReadThroughOption) (Meta, error) {
	return readThroughAt(ctx, client, rooms, roomID, ttl, rec, time.Now(), opts...)
}

// cachedMeta is the L2 envelope. CachedAt records when Mongo last confirmed the
// entry, which is what lets a reader tell a fresh entry from one due for
// re-validation — and what makes surviving an outage possible rather than
// waiting for the key to vanish.
type cachedMeta struct {
	Meta     Meta  `json:"meta"`
	CachedAt int64 `json:"cachedAt"`
}

func readThroughAt(ctx context.Context, client valkeyutil.Client, rooms *mongo.Collection, roomID string, ttl time.Duration, rec Recorder, now time.Time, opts ...ReadThroughOption) (Meta, error) {
	o := readThroughOpts{fetch: FetchFromMongo}
	for _, opt := range opts {
		opt(&o)
	}

	if client == nil {
		return fetchGuarded(ctx, &o, rooms, roomID)
	}

	if entry, found := readL2(ctx, client, roomID, rec); found {
		// Fresh: serve as a pure read.
		if valkeyutil.Fresh(entry.CachedAt, now, ttl) {
			return entry.Meta, nil
		}
		// Stale: re-validate. The three outcomes are not interchangeable.
		//
		//   - Confirmed gone: Mongo answered, and the room no longer exists.
		//     Serving the cached entry here would keep a deleted room alive
		//     indefinitely, because every later read lands on this same branch and
		//     re-arms the deadline again — the entry outlives its TTL for as long
		//     as anyone reads it. Evict and fail, which is what the cold path
		//     below already does with the same error.
		//   - Unreachable: keep serving and re-arm the deadline. broadcast-worker
		//     treats a room-meta failure as fatal, so letting the entry lapse
		//     mid-outage stops delivery for the room entirely. EXPIRE rather than
		//     SET so an entry busted since the read stays busted.
		//   - Healthy: rewrite, picking up whatever a missed bust left behind.
		meta, err := fetchGuarded(ctx, &o, rooms, roomID)
		switch {
		case errors.Is(err, mongo.ErrNoDocuments):
			BustMeta(ctx, client, roomID)
			return Meta{}, fmt.Errorf("l2 read-through: %w", err)
		case err != nil:
			slideL2(ctx, client, roomID, ttl)
			return entry.Meta, nil //nolint:nilerr // fail-open by design; see above
		}
		writeL2(ctx, client, roomID, &meta, ttl, now)
		return meta, nil
	}

	meta, err := fetchGuarded(ctx, &o, rooms, roomID)
	if err != nil {
		return Meta{}, fmt.Errorf("l2 read-through: %w", err)
	}
	writeL2(ctx, client, roomID, &meta, ttl, now)
	return meta, nil
}

// writeL2 stores meta with a fresh confirmation stamp. Best-effort: the caller
// already has the value and the next read repopulates.
func writeL2(ctx context.Context, client valkeyutil.Client, roomID string, meta *Meta, ttl time.Duration, now time.Time) {
	entry := cachedMeta{Meta: *meta, CachedAt: now.UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, client, MetaKey(roomID), entry, ttl); err != nil {
		slog.WarnContext(ctx, "room meta L2 populate failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
}

// slideL2 re-arms the entry's deadline without rewriting it.
func slideL2(ctx context.Context, client valkeyutil.Client, roomID string, ttl time.Duration) {
	valkeyutil.SlideTTL(ctx, client, MetaKey(roomID), ttl, "room meta")
}

// fetchGuarded runs FetchFromMongo, inside guard when one was supplied. A nil
// guard (the common case) calls straight through.
func fetchGuarded(ctx context.Context, o *readThroughOpts, rooms *mongo.Collection, roomID string) (Meta, error) {
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
func BustMeta(ctx context.Context, client valkeyutil.Client, roomID string) {
	valkeyutil.BustKeys(ctx, client, "room meta", MetaKey(roomID))
}

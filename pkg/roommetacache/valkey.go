package roommetacache

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Recorder records the outcome of an L2 cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

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
func readL2(ctx context.Context, client valkeyutil.Client, roomID string, rec Recorder) (Meta, bool) {
	return valkeyutil.ReadCachedJSON(ctx, client, MetaKey(roomID), "room meta", rec,
		func(m *Meta) bool { return m.ID != "" }, "room_id", roomID)
}

// ReadThroughOption configures a ReadThrough call.
type ReadThroughOption func(*readThroughOpts)

type readThroughOpts struct {
	guard func(func() error) error
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
	var o readThroughOpts
	for _, opt := range opts {
		opt(&o)
	}

	if client == nil {
		return fetchGuarded(ctx, o.guard, rooms, roomID)
	}

	if meta, found := readL2(ctx, client, roomID, rec); found {
		return meta, nil
	}

	meta, err := fetchGuarded(ctx, o.guard, rooms, roomID)
	if err != nil {
		return Meta{}, fmt.Errorf("l2 read-through: %w", err)
	}
	if err := valkeyutil.SetJSONWithTTL(ctx, client, MetaKey(roomID), meta, ttl); err != nil {
		slog.WarnContext(ctx, "room meta L2 populate failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
	return meta, nil
}

// fetchGuarded runs FetchFromMongo, inside guard when one was supplied. A nil
// guard (the common case) calls straight through.
func fetchGuarded(ctx context.Context, guard func(func() error) error, rooms *mongo.Collection, roomID string) (Meta, error) {
	if guard == nil {
		return FetchFromMongo(ctx, rooms, roomID)
	}
	var meta Meta
	err := guard(func() error {
		var e error
		meta, e = FetchFromMongo(ctx, rooms, roomID)
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
	if client == nil {
		return
	}
	if err := client.Del(ctx, MetaKey(roomID)); err != nil {
		slog.WarnContext(ctx, "room meta L2 invalidate failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
}

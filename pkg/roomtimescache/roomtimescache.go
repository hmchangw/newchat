// Package roomtimescache is the shared L2 (Valkey) tier holding a room's last
// known createdAt, so history-service can floor a Cassandra bucket walk when
// MongoDB cannot answer.
//
// It is deliberately NOT a read-through cache. The entry is written only after
// a confirmed source-of-truth read and is read back only when that read fails,
// so a healthy request never sees a cached value and the tier introduces no new
// staleness on the hot path. That also means it needs no invalidation: nothing
// consults it while the source of truth is reachable, so it does not join the
// cache-fill-after-invalidation race (#336) that the read-through tiers share.
//
// createdAt is the only room time here, and its immutability is the whole reason
// it qualifies: staleness cannot make it wrong, so a cached copy is as good as a
// fresh one at any age.
//
// lastMsgAt is deliberately absent, and this is the load-bearing part. It is
// tempting — it looks like it says where the newest message is, which would let
// a degraded read bound or skip its bucket walk. It does not. unread-worker
// projects it from MESSAGES-CANONICAL on a consumer separate from the
// message-worker that writes the messages themselves, with MaxDeliver=-1 holding
// batches un-acked through a MongoDB outage, so the pointer lags Cassandra by an
// unbounded amount by design (docs/load-testing/failure/mongodb.md). It is
// therefore neither a ceiling (it would start the walk below everything written
// during the outage) nor a skip hint (no timestamp stored beside it can bound
// how stale it is, so every bucket it would authorize skipping may hold a row).
// Caching it would only make that trap easy to fall into.
package roomtimescache

import (
	"context"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Times is the L2 wire form. Epoch milliseconds; zero means "not recorded" and
// is preserved rather than collapsed to the epoch.
type Times struct {
	CreatedAt int64 `json:"createdAt,omitempty"`
}

// Recorder records the outcome of an L2 lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// Key is the L2 key for a room's times. The {roomID} hash-tag colocates it in
// the room's cluster slot, matching house convention.
func Key(roomID string) string { return "roomtimes:{" + roomID + "}" }

// Tier reads and writes the room-times entry. A nil client or non-positive TTL
// disables it: every method becomes a no-op and Fallback reports no entry.
type Tier struct {
	client  valkeyutil.Client
	ttl     time.Duration
	metrics Recorder
}

// NewTier builds the tier. rec may be nil — the shared read half records
// unconditionally, so a nil recorder is substituted rather than passed through
// to panic on the first lookup.
func NewTier(client valkeyutil.Client, ttl time.Duration, rec Recorder) *Tier {
	if rec == nil {
		rec = nopRecorder{}
	}
	return &Tier{client: client, ttl: ttl, metrics: rec}
}

// nopRecorder discards cache outcomes, for a deployment (or a test) that wires
// no metrics.
type nopRecorder struct{}

func (nopRecorder) Hit(context.Context)   {}
func (nopRecorder) Miss(context.Context)  {}
func (nopRecorder) Error(context.Context) {}

// enabled reports whether the tier is in play. A non-positive ttl bypasses it
// entirely: Valkey reads ttl==0 as "store forever", so honoring the repo's
// "0 disables the cache" convention any other way would store an entry that
// never expires.
func (t *Tier) enabled() bool { return t != nil && t.client != nil && t.ttl > 0 }

// Store records a createdAt confirmed against the source of truth — call it only
// where the answer is authoritative, never from a client-supplied hint.
// Best-effort: the caller already holds the authoritative answer, so a write
// failure costs only the next outage's floor.
func (t *Tier) Store(ctx context.Context, roomID string, createdAt time.Time) {
	if !t.enabled() {
		return
	}
	entry := Times{CreatedAt: millis(createdAt)}
	if err := valkeyutil.SetJSONWithTTL(ctx, t.client, Key(roomID), entry, t.ttl); err != nil {
		slog.WarnContext(ctx, "room times L2 write failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
}

// Fallback returns the last confirmed createdAt for a room, for a caller whose
// source-of-truth read has already failed. found=false means there is no usable
// entry — including every Valkey failure mode, since the caller is already on
// its fallback path and must not be handed a second failure to handle.
//
// Serving a fallback is by definition the moment the source of truth is down,
// so the entry's deadline is re-armed to outlive the outage. EXPIRE rather than
// SET: a write site may delete this key between the read and the re-arm, and
// re-Setting the value just read would resurrect it.
func (t *Tier) Fallback(ctx context.Context, roomID string) (createdAt time.Time, found bool) {
	if !t.enabled() {
		return time.Time{}, false
	}
	// An entry with no createdAt recorded bounds nothing, so it is not a usable
	// hit — treating it as one would hand the caller a zero indistinguishable
	// from having no entry at all.
	entry, ok := valkeyutil.ReadCachedJSON(ctx, t.client, Key(roomID), "room times", t.metrics,
		func(v *Times) bool { return v.CreatedAt != 0 },
		"room_id", roomID)
	if !ok {
		return time.Time{}, false
	}
	valkeyutil.SlideTTL(ctx, t.client, Key(roomID), t.ttl, "room times")
	return fromMillis(entry.CreatedAt), true
}

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMilli()
}

func fromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

package atrest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// L2Recorder records L2 (Valkey) hit/miss/error outcomes. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type L2Recorder = valkeyutil.CacheRecorder

// DEKKey is the L2 key for a room's wrapped DEK. The {roomID} hash-tag
// colocates it in the room's cluster slot, matching house convention.
func DEKKey(roomID string) string {
	return "dek:{" + roomID + "}"
}

// cachedDEK is the L2 wire form: the wrapped-DEK row plus the moment that row
// was last confirmed against the inner (Mongo) store. CachedAt is a cache
// bookkeeping field, unrelated to the row's own CreatedAt — it drives
// refresh-on-read (see l2DEKStore.serveHit).
//
// An entry written by an older build (a bare RoomDataKey) decodes to a zero Row
// here, which readL2 already treats as a miss, so the format change degrades to
// one extra inner fetch per room rather than to a wrong key.
type cachedDEK struct {
	Row RoomDataKey `json:"row"`
	// CachedAt is Unix milliseconds. Milliseconds, not nanos, keep the JSON
	// stable-looking and are far finer than any refresh interval.
	CachedAt int64 `json:"cachedAt"`
}

// l2DEKStore decorates a DEKStore with a Valkey L2 tier holding the
// Vault-WRAPPED DEK record. It exists so a room's key stays reachable while
// MongoDB is unavailable: the in-process DEK cache expires on a fixed TTL
// stamped at fetch time, so without an L2 an active room loses its key
// mid-outage and encrypt/decrypt start failing.
//
// Only ciphertext is stored — the wrapped DEK is exactly what Mongo holds, so
// an attacker with Valkey access still needs the Vault KEK.
//
// Fail-open: a nil client, a non-positive ttl, or any Valkey error degrades to
// the inner store; only the inner store's result governs the returned error.
// Positive-only: an absent row (nil, nil) is never cached, since that value is
// what drives lazy DEK creation in the cipher.
//
// Two guarantees, both driven by the entry's own age (see serveHit):
//
//   - Freshness. An entry is re-resolved from Mongo once it is older than
//     the refresh window, so a stale value — including one left behind by an
//     invalidation whose Del was swallowed — is corrected within that window.
//   - Outage survival. When that refresh fails, the entry's TTL is re-armed, so
//     a room that keeps being read survives an outage of any length instead of
//     expiring one TTL after its last populate.
type l2DEKStore struct {
	inner  DEKStore
	client valkeyutil.Client
	// ttl also fixes the refresh window, via valkeyutil.RefreshAfter. That
	// window MUST exceed the cipher's in-process DEK cache TTL
	// (ATREST_DEK_CACHE_TTL), because that L1 sits in front and does not slide:
	// this tier is consulted at most once per room per L1 TTL per pod. Were the
	// window the shorter of the two, every L2 hit would be older than it and the
	// refresh would degenerate into a Mongo read plus a full SET on every L1
	// miss — precisely the load the L2 exists to absorb. The 90m/1h defaults
	// stay comfortably apart.
	ttl     time.Duration
	breaker *circuitbreaker.Breaker
	metrics L2Recorder

	now func() time.Time // overridden in tests
}

// NewL2DEKStore wraps inner with a Valkey L2 tier. Pass a nil client (or a
// non-positive ttl) to disable the L2 and get inner's behavior unchanged.
// breaker guards every inner (Mongo) fetch — the cold read-through and the
// periodic refresh alike — so a fetch during an outage fast-fails instead of
// stalling; it must not be nil.
func NewL2DEKStore(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder) DEKStore {
	if rec == nil {
		rec = valkeyutil.NoopRecorder{}
	}
	return &l2DEKStore{
		inner: inner, client: client, ttl: ttl,
		breaker: breaker, metrics: rec,
		now: time.Now,
	}
}

func (s *l2DEKStore) l2Enabled() bool { return s.client != nil && s.ttl > 0 }

func (s *l2DEKStore) nowMilli() int64 { return s.now().UnixMilli() }

func (s *l2DEKStore) Get(ctx context.Context, roomID string) (*RoomDataKey, error) {
	if s.l2Enabled() {
		if entry, found := s.readL2(ctx, roomID); found {
			return s.serveHit(ctx, roomID, entry), nil
		}
	}

	row, err := s.fetchInner(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("dek l2 read-through for room %s: %w", roomID, err)
	}
	// A nil row means "no DEK yet" — never cached; the cipher creates one.
	if row != nil && s.l2Enabled() {
		s.writeL2(ctx, roomID, cachedDEK{Row: *row, CachedAt: s.nowMilli()}, "populate")
	}
	return row, nil
}

// serveHit resolves an L2 hit, and is where both of the tier's guarantees live.
//
// An entry confirmed within the refresh window is served as a pure read: no Mongo
// call, no write. That is the steady state, since the cipher's in-process cache
// means most L2 hits are already minutes apart.
//
// An older entry is re-resolved through the breaker, which is the only way the
// breaker ever observes Mongo's health on a hit-serving pod:
//
//   - Success replaces the entry and restamps CachedAt, so a missed
//     invalidation self-heals within one refresh window.
//   - Failure (a Mongo error, or ErrOpen when the breaker has already given up)
//     re-arms the TTL and leaves CachedAt alone, so the room stays alive and
//     the next read retries. The breaker throttles those retries: while open it
//     answers ErrOpen without touching Mongo, and lets a single probe through
//     once its cooldown elapses — which is how recovery is noticed on a pod
//     whose reads are all L2 hits.
func (s *l2DEKStore) serveHit(ctx context.Context, roomID string, entry cachedDEK) *RoomDataKey {
	if valkeyutil.Fresh(entry.CachedAt, s.now(), s.ttl) {
		// Re-arm on a fresh serve too, so the entry's survival does not depend on
		// the TTL of whatever L1 sits in front of it. The DEK L1 is an hour while
		// the refresh window opens at RefreshAfter(90m) = 67.5m, so every L1 miss
		// lands BEFORE the window: without this the entry would be served, never
		// re-armed, and expire at 90m — with the next L1 miss at 120m finding
		// nothing, exactly during the outage this tier exists to survive.
		//
		// Safe because the two clocks are separate: Fresh() is computed from
		// CachedAt, which only a real re-validation advances, so the staleness
		// bound is unchanged — this moves the eviction deadline, not the
		// revalidation one. And SlideTTL issues EXPIRE, which no-ops on an absent
		// key, so an entry invalidated since the read stays invalidated.
		s.slideL2(ctx, roomID)
		return &entry.Row
	}

	row, err := s.fetchInner(ctx, roomID)
	if err != nil {
		s.slideL2(ctx, roomID)
		return &entry.Row
	}
	if row == nil {
		// Mongo answered "no such row" for a room we hold a key for. Nothing
		// deletes DEK rows, so this is lag or an anomaly, and honoring it would
		// send the cipher off to mint a second DEK — orphaning every message
		// already encrypted under this one. Keep serving the cached key, and
		// restamp so the retry is once per interval rather than once per read.
		slog.WarnContext(ctx, "dek L2 refresh found no row, keeping cached key",
			"room_id", roomID)
		entry.CachedAt = s.nowMilli()
		s.writeL2(ctx, roomID, entry, "norow-restamp")
		return &entry.Row
	}
	s.writeL2(ctx, roomID, cachedDEK{Row: *row, CachedAt: s.nowMilli()}, "refresh")
	return row
}

// fetchInner runs the inner Get under the breaker, so both the read-through and
// the refresh feed the same health signal.
func (s *l2DEKStore) fetchInner(ctx context.Context, roomID string) (*RoomDataKey, error) {
	row, err := circuitbreaker.Do1(s.breaker, func() (*RoomDataKey, error) {
		return s.inner.Get(ctx, roomID)
	})
	if err != nil {
		return nil, fmt.Errorf("dek inner get: %w", err)
	}
	return row, nil
}

// Upsert delegates and then invalidates the L2 entry so a subsequent read
// re-resolves authoritatively.
func (s *l2DEKStore) Upsert(ctx context.Context, key RoomDataKey) error { //nolint:gocritic // hugeParam: value receiver required by the DEKStore interface
	if err := s.inner.Upsert(ctx, key); err != nil {
		return fmt.Errorf("dek l2 upsert for room %s: %w", key.ID, err)
	}
	s.invalidate(ctx, key.ID)
	return nil
}

// Replace delegates and then invalidates the L2 entry. This is a correctness
// requirement, not an optimization: KEK rotation rewraps the DEK, so a stale
// cached wrapped-DEK would fail to unwrap under the new KEK. A swallowed Del
// is no longer permanent — the refresh in serveHit reconciles it.
func (s *l2DEKStore) Replace(ctx context.Context, key RoomDataKey) error { //nolint:gocritic // hugeParam: value receiver required by the DEKStore interface
	if err := s.inner.Replace(ctx, key); err != nil {
		return fmt.Errorf("dek l2 replace for room %s: %w", key.ID, err)
	}
	s.invalidate(ctx, key.ID)
	return nil
}

// readL2 attempts the L2 read. An entry with no wrapped key is not usable:
// serving it would fail Unwrap for the entry's whole TTL.
func (s *l2DEKStore) readL2(ctx context.Context, roomID string) (cachedDEK, bool) {
	return valkeyutil.ReadCachedJSON(ctx, s.client, DEKKey(roomID), "dek", s.metrics,
		func(c *cachedDEK) bool { return len(c.Row.WrappedDEK) > 0 }, "room_id", roomID)
}

// slideL2 re-arms the entry's deadline with EXPIRE rather than re-writing it.
// Replace invalidates this key on KEK rotation so the pre-rotation wrapped DEK
// stops being served; a slide racing that Del would re-Set the ciphertext it had
// already read, bringing back a key that no longer unwraps for a full TTL.
// EXPIRE on an absent key is a no-op, so a lost race leaves it deleted.
//
// Best-effort: a failure is logged and swallowed — the value was already served,
// and the next successful refresh repopulates with a fresh deadline.
func (s *l2DEKStore) slideL2(ctx context.Context, roomID string) {
	valkeyutil.SlideTTL(ctx, s.client, DEKKey(roomID), s.ttl, "dek")
}

// writeL2 stores the entry with a full TTL. Best-effort: a failure is logged and
// swallowed — the caller already has the value, and the next read repopulates.
// phase is a coarse tag ("populate"/"refresh"/"norow-restamp"); the caller decides
// whether the entry's CachedAt advances, since only a confirmed fetch may reset
// the refresh clock.
func (s *l2DEKStore) writeL2(ctx context.Context, roomID string, entry cachedDEK, phase string) {
	if err := valkeyutil.SetJSONWithTTL(ctx, s.client, DEKKey(roomID), entry, s.ttl); err != nil {
		slog.WarnContext(ctx, "dek L2 write failed (TTL will reconcile)",
			"room_id", roomID, "phase", phase, "error", err)
	}
}

// invalidate best-effort deletes the L2 entry after an authoritative write.
func (s *l2DEKStore) invalidate(ctx context.Context, roomID string) {
	if !s.l2Enabled() {
		return
	}
	valkeyutil.BustKeys(ctx, s.client, "dek", DEKKey(roomID))
}

// DefaultL2Recorder is the shared metrics recorder for the DEK L2 tier, so
// every service emits the same cache="atrestdek",tier="l2" series.
func DefaultL2Recorder() L2Recorder { return cachemetrics.For("atrestdek", "l2") }

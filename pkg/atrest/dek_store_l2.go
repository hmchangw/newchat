package atrest

import (
	"context"
	"fmt"
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

// Stamped reports when the inner store last confirmed the row.
func (c cachedDEK) Stamped() int64 { return c.CachedAt }

// Usable rejects an entry with no wrapped key: serving it would fail Unwrap for
// the entry's whole TTL.
func (c cachedDEK) Usable() bool { return len(c.Row.WrappedDEK) > 0 }

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
	// ttl is retained only so l2Enabled and invalidate can answer without
	// reaching into the tier; the tier owns every other use of it.
	//
	// It also fixes the refresh window, via valkeyutil.RefreshAfter. That window
	// MUST exceed the cipher's in-process DEK cache TTL (ATREST_DEK_CACHE_TTL),
	// because that L1 sits in front and does not slide: this tier is consulted at
	// most once per room per L1 TTL per pod. Were the window the shorter of the
	// two, every L2 hit would be older than it and the refresh would degenerate
	// into a Mongo read plus a full SET on every L1 miss — precisely the load the
	// L2 exists to absorb. The 90m/1h defaults stay comfortably apart, and
	// SlideOnFresh below covers the gap that remains.
	ttl     time.Duration
	breaker *circuitbreaker.Breaker
	tier    valkeyutil.Tier[string, cachedDEK]
}

// NewL2DEKStore wraps inner with a Valkey L2 tier. Pass a nil client (or a
// non-positive ttl) to disable the L2 and get inner's behavior unchanged.
// breaker guards every inner (Mongo) fetch — the cold read-through and the
// periodic refresh alike — so a fetch during an outage fast-fails instead of
// stalling; it must not be nil.
func NewL2DEKStore(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder) DEKStore {
	return newL2DEKStoreWithClock(inner, client, ttl, breaker, rec, time.Now)
}

// newL2DEKStoreWithClock is NewL2DEKStore with an injected clock, for this
// package's own refresh-window tests.
func newL2DEKStoreWithClock(inner DEKStore, client valkeyutil.Client, ttl time.Duration, breaker *circuitbreaker.Breaker, rec L2Recorder, now func() time.Time) *l2DEKStore {
	s := &l2DEKStore{inner: inner, client: client, ttl: ttl, breaker: breaker}
	s.tier = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[string, cachedDEK]{
		Client: client,
		TTL:    ttl,
		Label:  "dek",
		Rec:    rec,
		Key:    DEKKey,
		Load:   s.loadEntry,
		Stamp:  func(e cachedDEK, ms int64) cachedDEK { e.CachedAt = ms; return e },
		// Both deviations from the default tier behaviour, and both load-bearing.
		//
		// SlideOnFresh: the DEK L1 is an hour while the refresh window opens at
		// RefreshAfter(90m) = 67.5m, so every L1 miss lands BEFORE the window.
		// Without a slide the entry would be served, never re-armed, and expire at
		// 90m — with the next L1 miss at 120m finding nothing, exactly during the
		// outage this tier exists to survive. Safe because Fresh() is computed from
		// CachedAt, which only a real re-validation advances: this moves the
		// eviction deadline, not the revalidation one.
		SlideOnFresh: true,
		// KeepOnAbsent: nothing deletes DEK rows, so "no such row" is lag or an
		// anomaly. Honoring it would evict, and the cipher would then mint a second
		// DEK — orphaning every message already encrypted under the first.
		KeepOnAbsent: true,
	}, now)
	return s
}

// loadEntry adapts the inner store to the tier's three-way contract. A nil row
// is "no DEK yet", which the cipher turns into lazy creation, so it must reach
// the tier as a confirmed absence rather than an error.
func (s *l2DEKStore) loadEntry(ctx context.Context, roomID string) (cachedDEK, bool, error) {
	row, err := s.fetchInner(ctx, roomID)
	if err != nil {
		return cachedDEK{}, false, err
	}
	if row == nil {
		return cachedDEK{}, false, nil
	}
	return cachedDEK{Row: *row}, true, nil
}

func (s *l2DEKStore) l2Enabled() bool { return s.client != nil && s.ttl > 0 }

// Get resolves a room's wrapped DEK. A nil row (no error) means "no DEK yet" and
// is never cached — that value is what drives lazy DEK creation in the cipher.
//
// The refresh-and-survive policy is valkeyutil.Tier's, shared with every other
// L2 tier in the repo; the two ways this tier differs from the default are
// declared as options in the constructor above.
func (s *l2DEKStore) Get(ctx context.Context, roomID string) (*RoomDataKey, error) {
	entry, found, err := s.tier.Resolve(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("dek l2 read-through for room %s: %w", roomID, err)
	}
	if !found {
		return nil, nil
	}
	return &entry.Row, nil
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

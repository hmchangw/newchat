// Package subauthcache is the shared L2 (Valkey) tier for the subscription
// authorization decision read on the hot path of message-gatekeeper (send) and
// history-service (load history). Both services front it with their own
// process-local L1 cache; sharing the L2 means a user active in either journey
// warms the other.
//
// It stores a single superset projection (SubAuth) so one L2 entry serves both
// consumers: gatekeeper reads ID+Roles, history reads HistorySharedSince.
// Positive-only: only confirmed subscribers are cached; "not subscribed" and
// loader errors are never stored. Fail-open: a nil client or any Valkey error
// degrades to the loader — only the loader's result governs the returned error.
package subauthcache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/circuitbreaker"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// SubAuth is the shared L2 projection of a subscription. Its presence in L2
// means "subscribed"; absence is never cached. json tags pin the wire format.
type SubAuth struct {
	ID                 string       `json:"id"`
	Account            string       `json:"account"`
	Roles              []model.Role `json:"roles,omitempty"`
	HistorySharedSince *int64       `json:"historySharedSince,omitempty"` // epoch millis; nil = full access
}

// cachedAuth is the L2 wire form: the decision plus the moment it was last
// confirmed against the source of truth. CachedAt is cache bookkeeping and
// drives refresh-on-read (see Tier.serveHit).
//
// An entry written by an older build (a bare SubAuth) decodes to a zero Auth
// here, which readL2 already treats as a miss, so the format change costs one
// extra load per (room, account) rather than serving a wrong decision.
type cachedAuth struct {
	Auth SubAuth `json:"auth"`
	// CachedAt is Unix milliseconds.
	CachedAt int64 `json:"cachedAt"`
}

// Recorder records the outcome of an L2 cache lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// Loader fetches a fresh SubAuth from the source of truth. It returns
// (auth, subscribed, err): subscribed=false is a confirmed non-subscriber (not
// an error). The caller injects the circuit breaker by wrapping FetchFromMongo
// in this closure.
type Loader func(ctx context.Context, roomID, account string) (SubAuth, bool, error)

// SubKey is the L2 key for a (roomID, account) subscription. The {roomID}
// hash-tag colocates it in the room's cluster slot, matching house convention.
func SubKey(roomID, account string) string {
	return "sub:{" + roomID + "}:" + account
}

// FetchFromMongo reads the subscription projection both services need. Returns
// (zero, false, nil) when the user is not subscribed (Mongo ErrNoDocuments).
func FetchFromMongo(ctx context.Context, subscriptions *mongo.Collection, roomID, account string) (SubAuth, bool, error) {
	var sub model.Subscription
	filter := bson.M{"u.account": account, "roomId": roomID}
	proj := options.FindOne().SetProjection(bson.M{"u._id": 1, "u.account": 1, "roles": 1, "historySharedSince": 1})
	if err := subscriptions.FindOne(ctx, filter, proj).Decode(&sub); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return SubAuth{}, false, nil
		}
		return SubAuth{}, false, fmt.Errorf("find subscription for %s in %s: %w", account, roomID, err)
	}
	return fromSubscription(&sub), true, nil
}

func fromSubscription(sub *model.Subscription) SubAuth {
	a := SubAuth{
		ID:      sub.User.ID,
		Account: sub.User.Account,
		Roles:   sub.Roles,
	}
	if sub.HistorySharedSince != nil {
		ms := sub.HistorySharedSince.UTC().UnixMilli()
		a.HistorySharedSince = &ms
	}
	return a
}

// BustSub best-effort deletes a (roomID, account) subscription's L2 entry.
// Called from write sites (room-worker, room-service, inbox-worker) after an
// authoritative Mongo write that would make a cached positive SubAuth wrong —
// member removed, role changed, or historySharedSince changed. Fail-open: a
// nil client is a no-op and any Valkey error logs at warn and is swallowed —
// the configured L2 TTL reconciles a missed bust. Mirrors roommetacache.BustMeta.
func BustSub(ctx context.Context, client valkeyutil.Client, roomID, account string) {
	BustSubs(ctx, client, roomID, []string{account})
}

// BustSubs best-effort deletes many (roomID, account) subscription L2 entries
// in a single round trip. Called from write sites that mutate every
// subscriber of a room at once — room-service's roomRestricted, room-worker's
// processRemoveOrg/reconcileTeamsRoom, inbox-worker's handleMemberRemoved and
// handleRoomVisibilityChanged — instead of one Del per account. Safe in Valkey
// cluster mode specifically because SubKey hash-tags on {roomID}, so every key
// for one room's subscribers lands in the same cluster slot and a multi-key
// DEL is valid there (no CROSSSLOT). Fail-open: a nil client or an empty
// accounts slice is a no-op, and a Valkey error logs at warn and is swallowed —
// the configured L2 TTL reconciles a missed bust.
func BustSubs(ctx context.Context, client valkeyutil.Client, roomID string, accounts []string) {
	if client == nil || len(accounts) == 0 {
		return
	}
	keys := make([]string, len(accounts))
	for i, account := range accounts {
		keys[i] = SubKey(roomID, account)
	}
	valkeyutil.BustKeys(ctx, client, "subauth", keys...)
}

// readL2 attempts the L2 read. An entry with no user ID is not usable: this
// entry's presence means "subscribed", so serving a zero value would grant
// access with no identity attached for the entry's whole TTL.
func readL2(ctx context.Context, client valkeyutil.Client, roomID, account string, rec Recorder) (cachedAuth, bool) {
	return valkeyutil.ReadCachedJSON(ctx, client, SubKey(roomID, account), "subauth", rec,
		func(c *cachedAuth) bool { return c.Auth.ID != "" }, "room_id", roomID)
}

// Tier resolves the subscription authorization decision through the shared L2
// in front of a breaker-guarded read of the source of truth.
//
// It exists so the outage policy lives in one place. Both consumers previously
// hand-assembled the same loader-wrapping-breaker rig in service code, where it
// could drift and could not be changed from the package that owns it.
//
// Two guarantees, both driven by the entry's own age (see serveHit) — the same
// mechanism pkg/atrest uses for the room data key, so the two cache tiers
// behave identically under an outage:
//
//   - Freshness. An entry past its refresh window is re-resolved, so a change
//     whose invalidation was swallowed is corrected within that window rather
//     than living out the full TTL. For an authorization decision that is the
//     difference between revoked access dying in minutes and dying in hours.
//   - Outage survival. When that refresh fails, the deadline is re-armed, so a
//     room that keeps being read stays reachable for an outage of any length.
type Tier struct {
	client valkeyutil.Client
	// ttl also fixes the refresh window, via valkeyutil.RefreshAfter. That
	// window must exceed the process-local L1 TTL in front of this tier (two
	// minutes in both services), or every L1 miss would pay a refresh.
	ttl     time.Duration
	metrics Recorder
	loader  Loader

	now func() time.Time // overridden in tests
}

// NewTier wires the tier over a subscriptions collection. A nil client (or a
// non-positive ttl) disables the L2 and every read falls through to the
// breaker-guarded loader. breaker must not be nil.
func NewTier(client valkeyutil.Client, subscriptions *mongo.Collection, ttl time.Duration, breaker *circuitbreaker.Breaker, rec Recorder) *Tier {
	return NewTierWithLoader(client, ttl, rec, func(ctx context.Context, roomID, account string) (SubAuth, bool, error) {
		var (
			auth       SubAuth
			subscribed bool
		)
		err := breaker.Do(func() error {
			var e error
			auth, subscribed, e = FetchFromMongo(ctx, subscriptions, roomID, account)
			return e
		})
		return auth, subscribed, err
	})
}

// NewTierWithLoader builds a Tier over an arbitrary source of truth instead of
// a Mongo collection, for callers that already own the fetch (and for tests).
func NewTierWithLoader(client valkeyutil.Client, ttl time.Duration, rec Recorder, loader Loader) *Tier {
	if rec == nil {
		rec = valkeyutil.NoopRecorder{}
	}
	return &Tier{
		client: client, ttl: ttl, metrics: rec, loader: loader,
		now: time.Now,
	}
}

// l2Enabled reports whether the L2 tier is in play. A non-positive ttl fully
// bypasses it: Valkey treats ttl==0 as "store forever", so honoring a config
// convention of "0 disables the cache" any other way would cache an
// authorization decision with no expiry.
func (t *Tier) l2Enabled() bool { return t.client != nil && t.ttl > 0 }

// Resolve returns the caller's subscription authorization for a room:
// (auth, true, nil) when subscribed, (zero, false, nil) for a confirmed
// non-subscriber, and an error only when the source of truth could not answer.
// Fail-open: a nil client or any Valkey error degrades to the loader, and only
// the loader's result governs the returned error.
func (t *Tier) Resolve(ctx context.Context, roomID, account string) (SubAuth, bool, error) {
	if t.l2Enabled() {
		if entry, found := readL2(ctx, t.client, roomID, account, t.metrics); found {
			return t.serveHit(ctx, roomID, account, entry)
		}
	}
	auth, subscribed, err := t.loader(ctx, roomID, account)
	if err != nil {
		return SubAuth{}, false, err
	}
	// Positive-only: a confirmed non-subscriber is never cached, so the cache can
	// only ever grant access the source of truth already granted.
	if subscribed && t.l2Enabled() {
		t.writeL2(ctx, roomID, account, auth)
	}
	return auth, subscribed, nil
}

// serveHit resolves an L2 hit, and is where both of the tier's guarantees live.
//
// An entry confirmed within the refresh window is served as a pure read. An older one
// is re-resolved, and the three outcomes differ in what they mean:
//
//   - Failure: the source of truth is unreachable. Re-arm the deadline and keep
//     serving, so the outage does not revoke a subscriber mid-flight.
//   - Not subscribed: the subscription is genuinely gone and an invalidation was
//     missed. Evict, so revoked access dies now rather than at the TTL.
//   - Subscribed: rewrite with a fresh CachedAt, picking up any role or
//     access-window change the missed invalidation left behind.
func (t *Tier) serveHit(ctx context.Context, roomID, account string, entry cachedAuth) (SubAuth, bool, error) {
	if valkeyutil.Fresh(entry.CachedAt, t.now(), t.ttl) {
		return entry.Auth, true, nil
	}

	auth, subscribed, err := t.loader(ctx, roomID, account)
	switch {
	case err != nil:
		// Swallowing the error is the point: this entry was confirmed by the
		// source of truth once, and an outage must not revoke a subscriber
		// mid-flight. The deadline is re-armed so it survives however long the
		// outage lasts, and a cold (uncached) read still fails closed.
		t.slideL2TTL(ctx, roomID, account)
		return entry.Auth, true, nil //nolint:nilerr // fail-open by design; see above
	case !subscribed:
		BustSub(ctx, t.client, roomID, account)
		return SubAuth{}, false, nil
	default:
		t.writeL2(ctx, roomID, account, auth)
		return auth, true, nil
	}
}

// writeL2 stores the decision with a fresh confirmation stamp. Best-effort: a
// failure is logged and swallowed, since the caller already has the value and
// the next read repopulates.
func (t *Tier) writeL2(ctx context.Context, roomID, account string, auth SubAuth) {
	entry := cachedAuth{Auth: auth, CachedAt: t.now().UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, t.client, SubKey(roomID, account), entry, t.ttl); err != nil {
		slog.WarnContext(ctx, "subauth L2 write failed (TTL will reconcile)",
			"room_id", roomID, "error", err)
	}
}

// slideL2TTL re-arms the entry's deadline with EXPIRE rather than re-writing it.
// A write site may bust this key between the GET that served the hit and this
// call; re-Setting the value we just read would resurrect the entry the bust
// deleted, restoring revoked access for a full TTL. EXPIRE on an absent key is
// a no-op, so a lost race simply leaves it deleted.
func (t *Tier) slideL2TTL(ctx context.Context, roomID, account string) {
	valkeyutil.SlideTTL(ctx, t.client, SubKey(roomID, account), t.ttl, "subauth")
}

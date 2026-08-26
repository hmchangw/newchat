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
// here, which Usable already treats as a miss, so the format change costs one
// extra load per (room, account) rather than serving a wrong decision.

type cachedAuth struct {
	Auth SubAuth `json:"auth"`
	// CachedAt is Unix milliseconds.
	CachedAt int64 `json:"cachedAt"`
}

// Stamped reports when the source of truth last confirmed the decision.
func (c cachedAuth) Stamped() int64 { return c.CachedAt }

// Usable rejects an entry with no user ID. This entry's presence means
// "subscribed", so serving a zero value would grant access with no identity
// attached for the entry's whole TTL.
func (c cachedAuth) Usable() bool { return c.Auth.ID != "" }

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

// subID is the tier's identifier: this cache is keyed on a (room, account) pair
// rather than a single id, which is the one way it differs in shape from the
// other L2 tiers.
type subID struct {
	roomID  string
	account string
}

// Tier resolves the subscription authorization decision through the shared L2
// in front of a breaker-guarded read of the source of truth.
//
// It exists so the outage policy lives in one place. Both consumers previously
// hand-assembled the same loader-wrapping-breaker rig in service code, where it
// could drift and could not be changed from the package that owns it.
//
// The refresh-and-survive policy is valkeyutil.Tier's, shared with every other
// L2 tier in the repo, so a room that keeps being read stays authorized through
// an outage of any length and a swallowed invalidation is corrected within one
// refresh window rather than living out the full TTL. For an authorization
// decision that second property is the difference between revoked access dying
// in minutes and dying in hours.
type Tier struct {
	// ttl also fixes the refresh window, via valkeyutil.RefreshAfter. That
	// window must exceed the process-local L1 TTL in front of this tier (two
	// minutes in both services), or every L1 miss would pay a refresh.
	inner  valkeyutil.Tier[subID, cachedAuth]
	client valkeyutil.Client
	loader Loader
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
	return newTierWithClock(client, ttl, rec, loader, time.Now)
}

// newTierWithClock is NewTierWithLoader with an injected clock, for this
// package's own refresh-window tests.
func newTierWithClock(client valkeyutil.Client, ttl time.Duration, rec Recorder, loader Loader, now func() time.Time) *Tier {
	t := &Tier{client: client, loader: loader}
	t.inner = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[subID, cachedAuth]{
		Client: client,
		TTL:    ttl,
		Label:  "subauth",
		Rec:    rec,
		Key:    func(id subID) string { return SubKey(id.roomID, id.account) },
		Load:   t.loadEntry,
		Stamp:  func(e cachedAuth, ms int64) cachedAuth { e.CachedAt = ms; return e },
	}, now)
	return t
}

// loadEntry adapts the Loader to the tier's three-way contract. A confirmed
// non-subscriber is an absence, not an error — collapsing the two would make an
// outage indistinguishable from a revocation, and the tier would evict on both.
func (t *Tier) loadEntry(ctx context.Context, id subID) (cachedAuth, bool, error) {
	auth, subscribed, err := t.loader(ctx, id.roomID, id.account)
	if err != nil {
		return cachedAuth{}, false, err
	}
	return cachedAuth{Auth: auth}, subscribed, nil
}

// Resolve returns the caller's subscription authorization for a room:
// (auth, true, nil) when subscribed, (zero, false, nil) for a confirmed
// non-subscriber, and an error only when the source of truth could not answer.
// Fail-open: a nil client or any Valkey error degrades to the loader, and only
// the loader's result governs the returned error.
//
// Positive-only, inherited from valkeyutil.Tier: a confirmed non-subscriber is
// never written, so the cache can only ever grant access the source of truth
// already granted.
func (t *Tier) Resolve(ctx context.Context, roomID, account string) (SubAuth, bool, error) {
	entry, subscribed, err := t.inner.Resolve(ctx, subID{roomID: roomID, account: account})
	if err != nil {
		return SubAuth{}, false, err
	}
	return entry.Auth, subscribed, nil
}

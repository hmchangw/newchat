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

// usableAuth rejects an entry with no user ID: its presence means "subscribed",
// so a zero value would grant access to nobody in particular for a full TTL.
func usableAuth(a *SubAuth) bool { return a.ID != "" }

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
	return "sub:{" + roomID + "}:" + account + ":" + cacheKeySchemaVersion
}

// cacheKeySchemaVersion namespaces keys by stored shape, as roomsubcache and
// roommetacache do, so a future change to the stored value misses these entries
// rather than decoding them into an all-zero SubAuth with no JSON error. No
// earlier generation exists to clear: this package is new, and the shapes
// numbered below it never ran outside this branch.
//
// The version trails the key so the {roomID} hash tag still groups a room's
// subscribers into one cluster slot.
const cacheKeySchemaVersion = "v2"

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
// in one round trip. Called from write sites that mutate every subscriber of a
// room at once — room-service's roomRestricted, room-worker's
// processRemoveOrg/reconcileTeamsRoom, inbox-worker's handleMemberRemoved and
// handleRoomVisibilityChanged — instead of one Del per account. Fail-open: a
// nil client or an empty accounts slice is a no-op, and a Valkey error logs at
// warn and is swallowed — the configured L2 TTL reconciles a missed bust.
func BustSubs(ctx context.Context, client valkeyutil.Client, roomID string, accounts []string) {
	if client == nil || len(accounts) == 0 {
		return
	}
	keys := make([]string, 0, len(accounts))
	for _, account := range accounts {
		keys = append(keys, SubKey(roomID, account))
	}
	valkeyutil.BustKeys(ctx, client, "subauth", keys...)
}

// subID is the tier's key: a (room, account) pair rather than a single id.
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
// Caching policy is valkeyutil.Tier's: a room that keeps being read stays
// authorized through an outage, and a missed invalidation is corrected within
// one reload window instead of a full TTL — for access control, minutes not
// hours.
type Tier struct {
	// ttl also fixes the refresh window, via valkeyutil.RefreshAfter. That
	// window must exceed the process-local L1 TTL in front of this tier (two
	// minutes in both services), or every L1 miss would pay a refresh.
	inner  valkeyutil.Tier[subID, SubAuth]
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
	t := &Tier{loader: loader}
	t.inner = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[subID, SubAuth]{
		Client: client,
		TTL:    ttl,
		Label:  "subauth",
		Rec:    rec,
		Key:    func(id subID) string { return SubKey(id.roomID, id.account) },
		Load:   t.loadEntry,
		Valid:  usableAuth,
	}, now)
	return t
}

// loadEntry adapts Loader to the tier's three results. A confirmed
// non-subscriber is "missing", not an error, or an outage looks like a removal.
func (t *Tier) loadEntry(ctx context.Context, id subID) (SubAuth, bool, error) {
	return t.loader(ctx, id.roomID, id.account)
}

// Resolve returns the caller's authorization for a room: (auth, true, nil) when
// subscribed, (zero, false, nil) when confirmed not, and an error only when the
// source could not answer. A non-subscriber is never cached, so the cache can
// only grant access MongoDB already granted.
func (t *Tier) Resolve(ctx context.Context, roomID, account string) (SubAuth, bool, error) {
	return t.inner.Resolve(ctx, subID{roomID: roomID, account: account})
}

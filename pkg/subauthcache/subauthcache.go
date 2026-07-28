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

// Recorder records L2 hit/miss/error outcomes. cachemetrics.Recorder satisfies it.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

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

// ReadThrough resolves a SubAuth through the L2 (Valkey) tier: GET on the cache
// key, and on miss (or any L2 error) fall back to loader and repopulate L2 with
// ttl when the loader reports subscribed=true. Fail-open — a nil client or any
// Valkey error degrades to loader; only loader's result governs the error.
func ReadThrough(ctx context.Context, client valkeyutil.Client, loader Loader, roomID, account string, ttl time.Duration, rec Recorder) (SubAuth, bool, error) {
	if client != nil {
		if auth, found := readL2(ctx, client, roomID, account, rec); found {
			return auth, true, nil
		}
	}
	auth, subscribed, err := loader(ctx, roomID, account)
	if err != nil {
		return SubAuth{}, false, err
	}
	if subscribed && client != nil {
		if err := valkeyutil.SetJSONWithTTL(ctx, client, SubKey(roomID, account), auth, ttl); err != nil {
			slog.WarnContext(ctx, "subauth L2 populate failed (TTL will reconcile)",
				"room_id", roomID, "error", err)
		}
	}
	return auth, subscribed, nil
}

// readL2 attempts the L2 read; records the outcome. Returns found=true only on
// a hit. A clean miss records Miss; any other error records Error and returns
// found=false so the caller falls through to the loader.
func readL2(ctx context.Context, client valkeyutil.Client, roomID, account string, rec Recorder) (SubAuth, bool) {
	var cached SubAuth
	err := valkeyutil.GetJSON(ctx, client, SubKey(roomID, account), &cached)
	if err == nil {
		rec.Hit(ctx)
		return cached, true
	}
	if errors.Is(err, valkeyutil.ErrCacheMiss) {
		rec.Miss(ctx)
		return SubAuth{}, false
	}
	rec.Error(ctx)
	slog.WarnContext(ctx, "subauth L2 read failed, falling back to loader",
		"room_id", roomID, "error", err)
	return SubAuth{}, false
}

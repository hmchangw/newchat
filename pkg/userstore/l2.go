package userstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// idKey and accountKey are the two key spaces a user is reachable through.
// Both hold the whole record rather than one aliasing the other: an alias would
// cost a second round-trip per lookup, and in cluster mode the two keys hash to
// different slots anyway, so the "cheap" indirection buys nothing.
func idKey(id string) string           { return "user:id:" + id }
func accountKey(account string) string { return "user:acct:" + account }

// cachedUser is the L2 envelope. CachedAt drives refresh-on-read: it records
// when the source of truth last confirmed the record, which is what decides
// whether a hit is served straight or re-validated.
type cachedUser struct {
	User     model.User `json:"user"`
	CachedAt int64      `json:"cachedAt"`
}

// l2Store is a read-through Valkey tier over a UserStore.
//
// It exists for availability, not throughput — the pod-local L1 in front of it
// already handles load. What the L1 cannot do is answer for a user this pod has
// never seen, or survive a restart, and during a Mongo outage those are exactly
// the lookups that decide whether a mention resolves or is persisted as plain
// text forever.
//
// Staleness is bounded by ttl while the source is healthy, matching the L1's
// own bound, so adding this tier does not widen the window in normal operation.
// When the source is unreachable the deadline is re-armed instead (see
// serveHit), so warm entries outlive an outage longer than the TTL.
type l2Store struct {
	inner        UserStore
	client       valkeyutil.Client // nil disables the tier entirely
	ttl          time.Duration
	refreshAfter time.Duration
	metrics      Recorder
	now          func() time.Time
}

// NewL2Store wraps store with a Valkey read-through tier. A nil client (or a
// non-positive ttl) returns store unchanged, so callers can wire it
// unconditionally in deployments with no Valkey.
//
// Compose it inside the L1 and, where present, inside the breaker:
//
//	NewCache(NewL2Store(NewBreakerStore(NewMongoStore(col), b), vk, ttl, rec), ...)
//
// so an open breaker still serves both cache tiers.
func NewL2Store(store UserStore, client valkeyutil.Client, ttl time.Duration, rec Recorder) UserStore {
	return newL2StoreWithClock(store, client, ttl, rec, time.Now)
}

func newL2StoreWithClock(store UserStore, client valkeyutil.Client, ttl time.Duration, rec Recorder, now func() time.Time) UserStore {
	if client == nil || ttl <= 0 {
		return store
	}
	if rec == nil {
		rec = cachemetrics.For("user", "l2")
	}
	return &l2Store{
		inner:        store,
		client:       client,
		ttl:          ttl,
		refreshAfter: ttl / 4 * 3,
		metrics:      rec,
		now:          now,
	}
}

// Bust drops both key spaces for a user. Callers that mutate a user record
// should invoke it so a rename or transfer is not served from L2 until the TTL
// lapses — a stale display name here does not just render wrong, message-worker
// persists it onto the Cassandra message row. Either argument may be empty.
func Bust(ctx context.Context, client valkeyutil.Client, userID, account string) {
	if client == nil {
		return
	}
	keys := make([]string, 0, 2)
	if userID != "" {
		keys = append(keys, idKey(userID))
	}
	if account != "" {
		keys = append(keys, accountKey(account))
	}
	if len(keys) == 0 {
		return
	}
	if err := client.Del(ctx, keys...); err != nil {
		slog.WarnContext(ctx, "user L2 bust failed (TTL will reconcile)",
			"user_id", userID, "error", err)
	}
}

func (l *l2Store) FindUserByID(ctx context.Context, id string) (*model.User, error) {
	return l.resolve(ctx, idKey(id), func(ctx context.Context) (*model.User, error) {
		return l.inner.FindUserByID(ctx, id)
	})
}

func (l *l2Store) FindUserByAccount(ctx context.Context, account string) (*model.User, error) {
	return l.resolve(ctx, accountKey(account), func(ctx context.Context) (*model.User, error) {
		return l.inner.FindUserByAccount(ctx, account)
	})
}

// resolve serves key from L2 when it can, otherwise loads and populates.
func (l *l2Store) resolve(ctx context.Context, key string, load func(context.Context) (*model.User, error)) (*model.User, error) {
	if entry, found := l.readL2(ctx, key); found {
		return l.serveHit(ctx, key, &entry, load)
	}
	u, err := load(ctx)
	if err != nil {
		return nil, err
	}
	l.write(ctx, u)
	return u, nil
}

// serveHit decides what an L2 hit means.
//
// Confirmed within refreshAfter, it is served as a pure read. Older than that,
// it is re-validated — and the failure branch is the whole point of the tier: a
// record the source confirmed once must not vanish because the source is
// currently down, so the deadline is re-armed and the cached user is served.
// A cold (uncached) lookup still fails, since a user cannot be invented.
func (l *l2Store) serveHit(ctx context.Context, key string, entry *cachedUser, load func(context.Context) (*model.User, error)) (*model.User, error) {
	if l.now().Sub(time.UnixMilli(entry.CachedAt)) < l.refreshAfter {
		return &entry.User, nil
	}
	u, err := load(ctx)
	switch {
	case errors.Is(err, ErrUserNotFound):
		// The user is genuinely gone, not unreachable. Drop both keys so the
		// deletion takes effect now rather than at the TTL.
		Bust(ctx, l.client, entry.User.ID, entry.User.Account)
		return nil, err
	case err != nil:
		// Swallowing the error is deliberate: this record was confirmed once and
		// an outage must not un-resolve it mid-flight. EXPIRE (not SET) re-arms
		// the deadline, so a key evicted meanwhile stays evicted instead of being
		// resurrected with a stale record.
		l.slide(ctx, key)
		return &entry.User, nil //nolint:nilerr // fail-open by design; see above
	default:
		l.write(ctx, u)
		return u, nil
	}
}

// FindUsersByAccounts serves what L2 holds and forwards only the misses.
// A store failure still returns the hits, matching Cache.FindUsersByAccounts —
// pkg/mention relies on that partial answer to resolve what it can during an
// outage.
func (l *l2Store) FindUsersByAccounts(ctx context.Context, accounts []string) ([]model.User, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(accounts))
	hits := make([]model.User, 0, len(accounts))
	missing := make([]string, 0, len(accounts))
	for _, a := range accounts {
		if _, dup := seen[a]; dup {
			continue
		}
		seen[a] = struct{}{}
		// Served without refresh-on-read: this is the bulk mention path, and one
		// re-validation per stale account would put the outage latency back that
		// the tier exists to remove. The TTL still bounds staleness.
		if entry, found := l.readL2(ctx, accountKey(a)); found {
			hits = append(hits, entry.User)
			continue
		}
		missing = append(missing, a)
	}
	if len(missing) == 0 {
		return hits, nil
	}
	fresh, err := l.inner.FindUsersByAccounts(ctx, missing)
	for i := range fresh {
		l.write(ctx, &fresh[i])
	}
	if err != nil {
		return append(hits, fresh...), fmt.Errorf("l2 find users by accounts: %w", err)
	}
	return append(hits, fresh...), nil
}

func (l *l2Store) readL2(ctx context.Context, key string) (cachedUser, bool) {
	return valkeyutil.ReadCachedJSON[cachedUser](ctx, l.client, key, "user",
		l.metrics, func(c *cachedUser) bool { return c.User.ID != "" }, "key", key)
}

// write stores the record under both key spaces with a fresh confirmation
// stamp. Best-effort: the caller already has the value, and the next read
// repopulates. Absence is never cached — a negative entry would outlive the
// user actually being created.
func (l *l2Store) write(ctx context.Context, u *model.User) {
	if u == nil || u.ID == "" {
		return
	}
	entry := cachedUser{User: *u, CachedAt: l.now().UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, l.client, idKey(u.ID), entry, l.ttl); err != nil {
		slog.WarnContext(ctx, "user L2 write failed (TTL will reconcile)", "user_id", u.ID, "error", err)
	}
	if u.Account == "" {
		return
	}
	if err := valkeyutil.SetJSONWithTTL(ctx, l.client, accountKey(u.Account), entry, l.ttl); err != nil {
		slog.WarnContext(ctx, "user L2 write failed (TTL will reconcile)", "user_id", u.ID, "error", err)
	}
}

// slide re-arms the entry's deadline with EXPIRE rather than re-writing it, so
// an entry busted or expired since the read is not resurrected.
func (l *l2Store) slide(ctx context.Context, key string) {
	if _, err := l.client.Expire(ctx, key, l.ttl); err != nil {
		slog.WarnContext(ctx, "user L2 TTL slide failed (entry keeps its current deadline)",
			"key", key, "error", err)
	}
}

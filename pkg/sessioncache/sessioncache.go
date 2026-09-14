// Package sessioncache keeps bot session validation working while MongoDB is
// unreachable.
//
// Every authenticated bot request validates its token, and that validation is a
// MongoDB read. During an outage the read fails, so each request is rejected as
// an invalid token and every bot goes dark — even though the message pipeline
// behind them is healthy. This tier serves validations that MongoDB already
// confirmed, so bots authenticated before the outage keep working through it.
//
// # Security posture
//
// The tier is positive-only: only a confirmed session is ever written, so it can
// never grant access MongoDB had not already granted. A cold token still fails
// closed, and an outage is reported as an error rather than as "no such
// session", so it can't be mistaken for a decision.
//
// The accepted trade-off is revocation lag: a session revoked while MongoDB is
// unreachable keeps working until its entry clears. That was weighed
// deliberately — a stale session beats every bot being down for the length of
// the outage. Wiring revocation to Bust is the follow-up that closes it. While
// MongoDB is healthy, a revoked session is dropped at its first re-validation
// rather than lingering for the whole TTL.
//
// Nothing secret is stored. The raw bearer token never reaches this package —
// callers pass sessiontoken.Hash output, which is not invertible — and the
// cached value carries only the principal fields the validate response returns.
package sessioncache

import (
	"context"
	"errors"
	"time"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/session"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Recorder records the outcome of an L2 cache lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// Loader resolves a session by its hashed token from the source of truth.
type Loader func(ctx context.Context, hash string) (*session.Session, error)

// Key is the Valkey key for a hashed token. The hash — never the token — is the
// key material: it arrives already hashed, and a reader of Valkey therefore
// learns no credential they could authenticate with.
func Key(hash string) string { return "session:" + hash + ":" + cacheKeySchemaVersion }

// cacheKeySchemaVersion namespaces keys by stored shape, so a future change to
// the stored value misses these entries instead of decoding them as the wrong
// shape. No earlier generation exists to clear: this package is new, and the
// shapes numbered below it never ran outside this branch.
const cacheKeySchemaVersion = "v2"

// usableSession rejects an entry with no session ID: it would authenticate
// nobody in particular for a full TTL.
func usableSession(v *session.Session) bool { return v.ID != "" }

// Cache is a read-through Valkey tier over a session Loader.
type Cache struct {
	load Loader
	tier valkeyutil.Tier[string, session.Session]
}

// New returns a Cache over load. A nil client (or a non-positive ttl) makes
// every lookup go straight to load, so callers can wire it unconditionally in
// deployments with no Valkey.
func New(load Loader, client valkeyutil.Client, ttl time.Duration) *Cache {
	return newWithClock(load, client, ttl, time.Now)
}

func newWithClock(load Loader, client valkeyutil.Client, ttl time.Duration, now func() time.Time) *Cache {
	c := &Cache{load: load}
	c.tier = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[string, session.Session]{
		Client: client,
		TTL:    ttl,
		Label:  "session",
		Rec:    cachemetrics.For("session", "l2"),
		Key:    Key,
		Load:   c.loadEntry,
		Valid:  usableSession,
	}, now)
	return c
}

// loadEntry adapts Loader to the tier's three results. ErrNotFound must arrive
// as "missing", not as an error, or a revocation and an outage look the same.
func (c *Cache) loadEntry(ctx context.Context, hash string) (session.Session, bool, error) {
	s, err := c.load(ctx, hash)
	switch {
	case errors.Is(err, session.ErrNotFound):
		return session.Session{}, false, nil
	case err != nil:
		return session.Session{}, false, err
	case s == nil:
		return session.Session{}, false, nil
	}
	return *s, true, nil
}

// Bust drops a session's entry. Revocation paths should call it so a revoked
// token stops working immediately rather than at the TTL; until they do,
// revocation is reconciled by re-validation while MongoDB is healthy.
func Bust(ctx context.Context, client valkeyutil.Client, hash string) {
	if hash == "" {
		return
	}
	valkeyutil.BustKeys(ctx, client, "session", Key(hash))
}

// BustMany invalidates every supplied session hash. Empty hashes are skipped,
// and a nil client is a no-op, so a caller need not branch on either.
//
// This is the revocation hook for the bulk deletes on session.Store, which
// return the _ids they removed for exactly this purpose. Without it a revoked
// token keeps authenticating from cache until its refresh window elapses — and
// during a source outage the TTL slide re-arms it on every read, so it never
// elapses at all.
func BustMany(ctx context.Context, client valkeyutil.Client, hashes []string) {
	keys := make([]string, 0, len(hashes)*2)
	for _, h := range hashes {
		if h != "" {
			keys = append(keys, Key(h))
		}
	}
	if len(keys) == 0 {
		return
	}
	valkeyutil.BustKeys(ctx, client, "session", keys...)
}

// FindByHash resolves a session for an already-hashed token. It returns
// session.ErrNotFound only when MongoDB said so, so a caller cannot mistake an
// outage for an invalid token. Caching policy is valkeyutil.Tier's.
func (c *Cache) FindByHash(ctx context.Context, hash string) (*session.Session, error) {
	sess, found, err := c.tier.Resolve(ctx, hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, session.ErrNotFound
	}
	return &sess, nil
}

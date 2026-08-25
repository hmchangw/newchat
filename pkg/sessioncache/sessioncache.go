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
	"log/slog"
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
func Key(hash string) string { return "session:" + hash }

// cachedSession is the stored envelope. CachedAt records when MongoDB last
// confirmed the session, which is what separates an entry that can be served
// directly from one due for re-validation.
type cachedSession struct {
	Session  session.Session `json:"session"`
	CachedAt int64           `json:"cachedAt"`
}

// Cache is a read-through Valkey tier over a session Loader.
type Cache struct {
	load    Loader
	client  valkeyutil.Client // nil disables the tier entirely
	ttl     time.Duration
	metrics Recorder
	now     func() time.Time
}

// New returns a Cache over load. A nil client (or a non-positive ttl) makes
// every lookup go straight to load, so callers can wire it unconditionally in
// deployments with no Valkey.
func New(load Loader, client valkeyutil.Client, ttl time.Duration) *Cache {
	return newWithClock(load, client, ttl, time.Now)
}

func newWithClock(load Loader, client valkeyutil.Client, ttl time.Duration, now func() time.Time) *Cache {
	return &Cache{
		load:    load,
		client:  client,
		ttl:     ttl,
		metrics: cachemetrics.For("session", "l2"),
		now:     now,
	}
}

func (c *Cache) enabled() bool { return c.client != nil && c.ttl > 0 }

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
	keys := make([]string, 0, len(hashes))
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

// FindByHash resolves a session for an already-hashed token.
//
// It returns session.ErrNotFound only when the source of truth said so — an
// outage surfaces as a different error, so a caller cannot read "MongoDB is
// down" as "this token is invalid".
func (c *Cache) FindByHash(ctx context.Context, hash string) (*session.Session, error) {
	if !c.enabled() {
		return c.load(ctx, hash)
	}
	if entry, found := c.read(ctx, hash); found {
		return c.serveHit(ctx, hash, &entry)
	}
	s, err := c.load(ctx, hash)
	if err != nil {
		return nil, err
	}
	c.write(ctx, hash, s)
	return s, nil
}

// serveHit decides what a cache hit means.
//
// Confirmed within the refresh window, it is served as a pure read. Older than
// that it is re-validated, and the three outcomes differ in what they mean:
//
//   - Not found: the session was genuinely revoked. Evict and reject, so
//     revocation takes effect now rather than at the TTL.
//   - Failure: MongoDB is unreachable. Re-arm the deadline and keep serving —
//     this is the branch the whole tier exists for. EXPIRE rather than SET, so
//     an entry busted since the read stays busted.
//   - Success: rewrite with a fresh stamp, picking up any change.
func (c *Cache) serveHit(ctx context.Context, hash string, entry *cachedSession) (*session.Session, error) {
	if valkeyutil.Fresh(entry.CachedAt, c.now(), c.ttl) {
		return &entry.Session, nil
	}
	s, err := c.load(ctx, hash)
	switch {
	case errors.Is(err, session.ErrNotFound):
		Bust(ctx, c.client, hash)
		return nil, err
	case err != nil:
		c.slide(ctx, hash)
		return &entry.Session, nil //nolint:nilerr // fail-open by design; see above
	default:
		c.write(ctx, hash, s)
		return s, nil
	}
}

// read returns a usable entry, or reports a miss. An entry without a
// confirmation stamp or an ID is unusable and reloads rather than being served.
func (c *Cache) read(ctx context.Context, hash string) (cachedSession, bool) {
	return valkeyutil.ReadCachedJSON[cachedSession](ctx, c.client, Key(hash), "session",
		c.metrics, func(e *cachedSession) bool { return e.Session.ID != "" && e.CachedAt != 0 })
}

// write stores the session with a fresh confirmation stamp. Best-effort: the
// caller already has the value and the next read repopulates. Absence is never
// written — a negative entry would outlive the session being created.
func (c *Cache) write(ctx context.Context, hash string, s *session.Session) {
	if s == nil || s.ID == "" {
		return
	}
	entry := cachedSession{Session: *s, CachedAt: c.now().UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, c.client, Key(hash), entry, c.ttl); err != nil {
		slog.WarnContext(ctx, "session L2 write failed (TTL will reconcile)", "error", err)
	}
}

// slide re-arms the entry's deadline without rewriting it.
func (c *Cache) slide(ctx context.Context, hash string) {
	valkeyutil.SlideTTL(ctx, c.client, Key(hash), c.ttl, "session")
}

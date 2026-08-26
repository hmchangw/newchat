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
func Key(hash string) string { return "session:" + hash }

// cachedSession is the stored envelope. The embedded Stamp records when MongoDB
// last confirmed the session, which is what separates an entry that can be
// served directly from one due for re-validation. The field is declared rather than
// embedded so composite literals keep working; see valkeyutil.Entry.
type cachedSession struct {
	Session  session.Session `json:"session"`
	CachedAt int64           `json:"cachedAt"`
}

// Stamped reports when MongoDB last confirmed the session.
func (c cachedSession) Stamped() int64 { return c.CachedAt } //nolint:gocritic // hugeParam: interface-required value receiver, see Usable

// Usable rejects an entry with no session ID or no confirmation stamp: serving
// one would authenticate an identity-less principal for the rest of its TTL, and
// an unstamped entry (written before the envelope existed) would read as
// permanently stale.//
// The value receiver is required, and so is the gocritic exemption below it:
// valkeyutil.Entry is satisfied by the type the tier stores, and a value type's
// method set excludes pointer-receiver methods. The copy is two per cache read,
// against a Valkey round trip.
func (c cachedSession) Usable() bool { return c.Session.ID != "" && c.CachedAt != 0 } //nolint:gocritic // hugeParam: see the note above

// Cache is a read-through Valkey tier over a session Loader.
type Cache struct {
	load Loader
	tier valkeyutil.Tier[string, cachedSession]
	// client is retained only so Bust has something to hand BustKeys; the tier
	// owns every other use of it.
	client valkeyutil.Client
}

// New returns a Cache over load. A nil client (or a non-positive ttl) makes
// every lookup go straight to load, so callers can wire it unconditionally in
// deployments with no Valkey.
func New(load Loader, client valkeyutil.Client, ttl time.Duration) *Cache {
	return newWithClock(load, client, ttl, time.Now)
}

func newWithClock(load Loader, client valkeyutil.Client, ttl time.Duration, now func() time.Time) *Cache {
	c := &Cache{load: load, client: client}
	c.tier = valkeyutil.NewTierWithClock(valkeyutil.TierConfig[string, cachedSession]{
		Client: client,
		TTL:    ttl,
		Label:  "session",
		Rec:    cachemetrics.For("session", "l2"),
		Key:    Key,
		Load:   c.loadEntry,
		Stamp:  func(e cachedSession, ms int64) cachedSession { e.CachedAt = ms; return e },
	}, now)
	return c
}

// loadEntry adapts the session Loader to the tier's three-way contract.
// ErrNotFound is a decision — the session is genuinely gone — and must reach the
// tier as a confirmed absence rather than an error, or an outage and a
// revocation would be indistinguishable and the tier would evict on both.
func (c *Cache) loadEntry(ctx context.Context, hash string) (cachedSession, bool, error) {
	s, err := c.load(ctx, hash)
	switch {
	case errors.Is(err, session.ErrNotFound):
		return cachedSession{}, false, nil
	case err != nil:
		return cachedSession{}, false, err
	case s == nil:
		return cachedSession{}, false, nil
	}
	return cachedSession{Session: *s}, true, nil
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
// The refresh-and-survive policy behind this — serve a fresh entry, re-validate
// a stale one, slide on failure, evict on a confirmed revocation — is
// valkeyutil.Tier's, shared with every other L2 tier in the repo.
func (c *Cache) FindByHash(ctx context.Context, hash string) (*session.Session, error) {
	entry, found, err := c.tier.Resolve(ctx, hash)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, session.ErrNotFound
	}
	return &entry.Session, nil
}

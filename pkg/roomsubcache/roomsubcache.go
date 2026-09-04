// Package roomsubcache caches the member list of a room in Valkey so
// fan-out workers (e.g. notification-worker) can avoid a Mongo round-trip
// for every published message.
//
// The cache stores the fan-out path's per-member input set — see Member.
// Entries are written with a caller-supplied TTL and may be eagerly
// invalidated via Invalidate; staleness is otherwise bounded by the TTL.
//
// # Shared key
//
// The key is per room and carries no service namespace, so every service that
// caches a room shares one entry. Two consequences bind all of them:
//
//   - Writers must fill every Member field. NewMongoLoader is the only
//     sanctioned production loader; a partial writer would silently unmute
//     muted users and widen history access windows for the services that gate
//     on Muted and HistorySharedSince.
//   - Readers should configure the same TTL (ROOMSUBCACHE_TTL), since whichever
//     service writes an entry sets the staleness bound every other one gets.
//
// notification-worker additionally invalidates on membership and mute changes;
// services that only read benefit from that without wiring anything.
package roomsubcache

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

// Recorder records the outcome of an L2 cache lookup. An alias of
// valkeyutil.CacheRecorder: every tier in this repo records against one
// interface, and cachemetrics.Recorder satisfies it.
type Recorder = valkeyutil.CacheRecorder

// DefaultMaxValueBytes caps the size of a cached blob accepted by Get.
// Sized to comfortably accommodate ~250k members at ~64B/member; serves
// as defense-in-depth against a compromised Valkey writer trying to OOM
// the reader. Configurable per-instance via WithMaxValueBytes.
const DefaultMaxValueBytes = 16 * 1024 * 1024

// Member is the model.Subscription projection needed by the fan-out path.
// Extra fields use omitempty so a plain member's JSON stays {id, account}.
type Member struct {
	ID                 string         `json:"id"`
	Account            string         `json:"account"`
	RoomType           model.RoomType `json:"roomType,omitempty"`
	IsBot              bool           `json:"isBot,omitempty"`
	Muted              bool           `json:"muted,omitempty"`
	HistorySharedSince *int64         `json:"historySharedSince,omitempty"`
	// HomeSiteID is the member's HOME site, resolved from the users collection
	// (users.siteId) by the loader at cache-fill time. It is deliberately NOT
	// model.Subscription.SiteID — that field is the ROOM's home site (see
	// docs/client-api.md, Subscription schema), which at the room's own site is
	// identical for every member and useless for routing. notification-worker
	// groups survivors by HomeSiteID for the per-site badge-count RPC. Empty
	// when the account is missing from the users collection — such members
	// degrade (no badge count); cacheKeySchemaVersion was bumped to v3 so
	// pre-fix entries (whose siteId carried the room's site) miss instead of
	// misrouting the RPC forever.
	HomeSiteID string `json:"homeSiteId,omitempty"`
	// IsSubscribed is the subscription's own flag, denormalised for the fan-out
	// path. Safe to serve from a TTL'd entry because it is stable: an explicit
	// subscribe/unsubscribe flips it, unlike lastSeenAt/hasMention/threadUnread,
	// which roomlist-worker rewrites on every message and which therefore must
	// never be cached here.
	IsSubscribed bool `json:"isSubscribed,omitempty"`
}

// Cache stores and retrieves a room's member list.
//
// Get reports ok=false for anything that is not a servable entry — an absent
// key, an unreadable one, a foreign or pre-envelope value — and records and
// logs which it was. It returns no error because the caller has the same move
// for all of them: go to the source of truth. An empty (non-nil) slice IS a
// hit and must not be confused with a miss; callers negative-cache empty rooms
// by Set-ing nil.
type Cache interface {
	Get(ctx context.Context, roomID string) (Entry, bool)
	Set(ctx context.Context, roomID string, members []Member, ttl time.Duration) error
	// Slide re-arms an entry's deadline without rewriting it, so a room stays
	// resolvable while the source of truth is unreachable. It uses EXPIRE, which
	// no-ops on an absent key — an entry invalidated since the read must stay
	// invalidated rather than be resurrected.
	//
	// Slide and Invalidate are best-effort and return nothing: both run after
	// the caller has already committed to an outcome (serving a stale entry, or
	// an authoritative write), so there is no decision left for an error to
	// inform. Failures are logged by pkg/valkeyutil, which owns the policy.
	Slide(ctx context.Context, roomID string, ttl time.Duration)
	Invalidate(ctx context.Context, roomID string)
}

// Entry is a cached member list plus the moment the source of truth last
// confirmed it. CachedAt drives refresh-on-read: it is what lets a reader tell
// a recently-confirmed entry from one that should be re-validated, and it is
// why a Mongo outage can extend an entry rather than lose it.
//
// It is the shared valkeyutil.Box, not a private struct, so this tier cannot
// drift from the four that go through valkeyutil.Tier — the envelope, the
// stamp rule and the outcome table are the same ones or they are not shared at
// all. The member list is therefore Entry.V.
type Entry = valkeyutil.Box[[]Member]

type valkeyCache struct {
	client        valkeyutil.Client
	maxValueBytes int
	metrics       Recorder
	now           func() time.Time
}

// Option configures a valkeyCache at construction.
type Option func(*valkeyCache)

// WithMaxValueBytes overrides the maximum blob size Get will accept.
// Use to tighten the cap in deployments with smaller realistic rooms, or
// to loosen it for unusually large ones. A value <= 0 disables the cap.
func WithMaxValueBytes(n int) Option {
	return func(c *valkeyCache) { c.maxValueBytes = n }
}

// WithMetrics overrides the hit/miss/error recorder. Defaults to the
// package-default cachemetrics recorder tagged cache="roomsub",tier="l2".
func WithMetrics(r Recorder) Option {
	return func(c *valkeyCache) { c.metrics = r }
}

// NewValkeyCache returns a Cache backed by the given Valkey client.
func NewValkeyCache(client valkeyutil.Client, opts ...Option) Cache {
	c := &valkeyCache{
		client:        client,
		maxValueBytes: DefaultMaxValueBytes,
		metrics:       cachemetrics.For("roomsub", "l2"),
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// cacheKeySchemaVersion namespaces cache keys by the Member wire shape.
// Bump whenever a Member field is added/changed such that an old cached
// entry would silently decode with a zero-valued new field forever (Valkey
// has no schema check) — the version segment makes such entries miss so
// they get repopulated from Mongo with the current shape. Bumped to v2 when
// SiteID was added; bumped to v3 when SiteID (the room's home site — a bug)
// was replaced by HomeSiteID (the member's home site, see Member.HomeSiteID)
// so pre-fix entries miss instead of decoding with the wrong semantics.
//
// Bumped to v4 when the stored value stopped being a bare []Member and became
// the Entry{Members, CachedAt} envelope. That is a change of JSON kind, array
// to object, so reusing v3 would have each binary in a rolling deploy handed
// the other's shape — an unmarshal error, not a silent zero value.
//
// Bumped to v5 when IsSubscribed was added. A v4 entry decodes cleanly under
// the v5 shape, which is precisely the danger: every member would read as
// unsubscribed for the full TTL with nothing to signal it.
const cacheKeySchemaVersion = "v5"

// legacyCacheKeySchemaVersions are the superseded generations, newest first.
// Only invalidation touches them: a rolling deploy can still have a binary of
// an older generation live, and a bust must clear that generation's key too or
// that binary keeps serving a member list for a room whose membership just
// changed. Drop an entry once no binary of that generation can be running.
var legacyCacheKeySchemaVersions = []string{"v4", "v3"}

func cacheKey(roomID string) string {
	return "room:" + cacheKeySchemaVersion + ":" + roomID + ":subs"
}

// allGenerationKeys returns the current key followed by every superseded one,
// in the order Invalidate deletes them.
func allGenerationKeys(roomID string) []string {
	keys := make([]string, 0, 1+len(legacyCacheKeySchemaVersions))
	keys = append(keys, cacheKey(roomID))
	for _, v := range legacyCacheKeySchemaVersions {
		keys = append(keys, "room:"+v+":"+roomID+":subs")
	}
	return keys
}

// Get returns the cached member list for roomID. On absence it returns
// a wrapped valkeyutil.ErrCacheMiss — callers should branch with
// errors.Is. An empty cached value is a hit and returns a non-nil empty
// slice, distinguishable from a miss. Returns an error if roomID is
// empty or if the cached blob exceeds the configured size cap.
func (c *valkeyCache) Get(ctx context.Context, roomID string) (Entry, bool) {
	if roomID == "" {
		return Entry{}, false
	}
	raw, err := c.client.Get(ctx, cacheKey(roomID))
	// The cap is checked on the raw blob, before the decode, because that is the
	// point of it: a compromised writer of this shared key must not be able to
	// make every reader of the room allocate an arbitrary member list.
	if err == nil && c.maxValueBytes > 0 && len(raw) > c.maxValueBytes {
		c.metrics.Error(ctx)
		slog.WarnContext(ctx, "roomsubcache L2 entry exceeds the size cap, falling back to the source of truth",
			"roomId", roomID, "bytes", len(raw), "max", c.maxValueBytes)
		return Entry{}, false
	}
	if err != nil {
		// DecodeCachedJSON reads an empty raw as an absent key, which is the
		// outcome table's clean-miss branch — the same one GetJSON lands on.
		raw = ""
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			c.metrics.Error(ctx)
			slog.WarnContext(ctx, "roomsubcache L2 read failed, falling back to the source of truth",
				"roomId", roomID, "error", err)
			return Entry{}, false
		}
	}
	// A pre-envelope entry (a bare []Member array) fails to decode here, and an
	// entry with no stamp fails Usable — both reload as a miss rather than being
	// served as an empty member list, which would silently drop fan-out.
	entry, ok := valkeyutil.DecodeCachedJSON(ctx, raw, "roomsubcache", c.metrics,
		func(b *Entry) bool { return b.Usable(nil) }, "roomId", roomID)
	if !ok {
		return Entry{}, false
	}
	if entry.V == nil {
		entry.V = []Member{}
	}
	return entry, true
}

// Slide re-arms the entry's deadline. See Cache.Slide.
func (c *valkeyCache) Slide(ctx context.Context, roomID string, ttl time.Duration) {
	if roomID == "" {
		return
	}
	valkeyutil.SlideTTL(ctx, c.client, cacheKey(roomID), ttl, "roomsubcache")
}

// Set stores members under roomID with the given TTL. A nil members
// slice is stored as an empty list (so Get returns []Member{} rather
// than nil on the next read), which doubles as a negative cache for
// empty/deleted rooms. A ttl of 0 stores the entry without expiry —
// callers who want bounded staleness must pass a non-zero TTL. Returns
// an error if roomID is empty.
func (c *valkeyCache) Set(ctx context.Context, roomID string, members []Member, ttl time.Duration) error {
	if roomID == "" {
		return errors.New("roomsubcache: empty roomID")
	}
	if members == nil {
		members = []Member{}
	}
	entry := Entry{V: members, CachedAt: c.now().UnixMilli()}
	if err := valkeyutil.SetJSONWithTTL(ctx, c.client, cacheKey(roomID), entry, ttl); err != nil {
		return fmt.Errorf("set cached subscriptions for room %s: %w", roomID, err)
	}
	return nil
}

// Invalidate removes the cached entry for roomID in every key generation — see
// legacyCacheKeySchemaVersions. Those keys carry no hash tag and so land in
// different cluster slots; the client pipelines one DEL per key, so no delete
// can take another down with a CROSSSLOT rejection.
//
// Best-effort, like every other tier's invalidation: a bust runs after the
// authoritative write has committed, and BustKeys is what strips the caller's
// cancellation and bounds the call, so a request that finishes the instant the
// write lands cannot skip the DEL and leave a member list stale for a full TTL.
func (c *valkeyCache) Invalidate(ctx context.Context, roomID string) {
	if roomID == "" {
		return
	}
	valkeyutil.BustKeys(ctx, c.client, "roomsubcache", allGenerationKeys(roomID)...)
}

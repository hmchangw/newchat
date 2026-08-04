// Package roomsubcache caches the member list of a room in Valkey so
// fan-out workers (e.g. notification-worker) can avoid a Mongo round-trip
// for every published message.
//
// The cache stores the fan-out path's per-member input set — see Member.
// Entries are written with a caller-supplied TTL and may be eagerly
// invalidated via Invalidate; staleness is otherwise bounded by the TTL.
package roomsubcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Recorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

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
}

// Cache stores and retrieves a room's member list.
//
// Get returns valkeyutil.ErrCacheMiss when the room has no cached entry.
// An empty (non-nil) slice is a valid cache hit and must not be confused
// with a miss — callers can negative-cache empty rooms by Set-ing nil.
type Cache interface {
	Get(ctx context.Context, roomID string) ([]Member, error)
	Set(ctx context.Context, roomID string, members []Member, ttl time.Duration) error
	Invalidate(ctx context.Context, roomID string) error
}

type valkeyCache struct {
	client        valkeyutil.Client
	maxValueBytes int
	metrics       Recorder
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
const cacheKeySchemaVersion = "v3"

func cacheKey(roomID string) string {
	return "room:" + cacheKeySchemaVersion + ":" + roomID + ":subs"
}

// Get returns the cached member list for roomID. On absence it returns
// a wrapped valkeyutil.ErrCacheMiss — callers should branch with
// errors.Is. An empty cached value is a hit and returns a non-nil empty
// slice, distinguishable from a miss. Returns an error if roomID is
// empty or if the cached blob exceeds the configured size cap.
func (c *valkeyCache) Get(ctx context.Context, roomID string) ([]Member, error) {
	if roomID == "" {
		return nil, errors.New("roomsubcache: empty roomID")
	}
	raw, err := c.client.Get(ctx, cacheKey(roomID))
	if err != nil {
		if errors.Is(err, valkeyutil.ErrCacheMiss) {
			c.metrics.Miss(ctx)
		} else {
			c.metrics.Error(ctx)
		}
		return nil, fmt.Errorf("get cached subscriptions for room %s: %w", roomID, err)
	}
	if c.maxValueBytes > 0 && len(raw) > c.maxValueBytes {
		c.metrics.Error(ctx)
		return nil, fmt.Errorf("get cached subscriptions for room %s: blob exceeds max %d bytes (got %d)", roomID, c.maxValueBytes, len(raw))
	}
	members := []Member{}
	if err := json.Unmarshal([]byte(raw), &members); err != nil {
		c.metrics.Error(ctx)
		return nil, fmt.Errorf("get cached subscriptions for room %s: unmarshal: %w", roomID, err)
	}
	c.metrics.Hit(ctx)
	return members, nil
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
	if err := valkeyutil.SetJSONWithTTL(ctx, c.client, cacheKey(roomID), members, ttl); err != nil {
		return fmt.Errorf("set cached subscriptions for room %s: %w", roomID, err)
	}
	return nil
}

// Invalidate removes the cached entry for roomID. Intended for a future
// membership-change event listener; not called by the cache itself,
// which relies on TTL expiry. Returns an error if roomID is empty.
func (c *valkeyCache) Invalidate(ctx context.Context, roomID string) error {
	if roomID == "" {
		return errors.New("roomsubcache: empty roomID")
	}
	if err := c.client.Del(ctx, cacheKey(roomID)); err != nil {
		return fmt.Errorf("invalidate cached subscriptions for room %s: %w", roomID, err)
	}
	return nil
}

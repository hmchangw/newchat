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

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

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
}

// Option configures a valkeyCache at construction.
type Option func(*valkeyCache)

// WithMaxValueBytes overrides the maximum blob size Get will accept.
// Use to tighten the cap in deployments with smaller realistic rooms, or
// to loosen it for unusually large ones. A value <= 0 disables the cap.
func WithMaxValueBytes(n int) Option {
	return func(c *valkeyCache) { c.maxValueBytes = n }
}

// NewValkeyCache returns a Cache backed by the given Valkey client.
func NewValkeyCache(client valkeyutil.Client, opts ...Option) Cache {
	c := &valkeyCache{client: client, maxValueBytes: DefaultMaxValueBytes}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func cacheKey(roomID string) string {
	return "room:" + roomID + ":subs"
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
		return nil, fmt.Errorf("get cached subscriptions for room %s: %w", roomID, err)
	}
	if c.maxValueBytes > 0 && len(raw) > c.maxValueBytes {
		return nil, fmt.Errorf("get cached subscriptions for room %s: blob exceeds max %d bytes (got %d)", roomID, c.maxValueBytes, len(raw))
	}
	members := []Member{}
	if err := json.Unmarshal([]byte(raw), &members); err != nil {
		return nil, fmt.Errorf("get cached subscriptions for room %s: unmarshal: %w", roomID, err)
	}
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

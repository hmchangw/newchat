// Package roommetacache provides a process-local LRU+TTL cache for room
// metadata that is read on the per-message hot path of multiple services.
//
// The cached fields (Type, Name, SiteID, UserCount) change rarely; reading
// them from MongoDB on every published message produces measurable wasted
// load. This package centralizes the cache so message-gatekeeper and
// broadcast-worker share a uniform shape and behavior.
//
// Freshness is TTL-bounded at L1. An L2 (Valkey) tier (see valkey.go:
// ReadThrough / BustMeta) is shared across replicas and survives restarts;
// room-worker actively busts the L2 entry on writes to name/userCount. The
// L1 LRU remains TTL-only — Invalidate exists but the cross-process bust is
// L2-scoped, so L1 staleness is bounded by ROOM_META_CACHE_TTL. See the spec
// at docs/superpowers/specs/2026-05-18-message-pipeline-mongo-caching-design.md.
package roommetacache

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/model"
)

// fetchTimeout bounds the detached shared load so a hung backend cannot leak
// the singleflight goroutine or pin the in-flight key. See the design spec.
const fetchTimeout = 10 * time.Second

// Meta is the cached projection of a room document. Both consumers
// (gatekeeper and broadcast-worker) use these four fields and nothing
// else from the room. The json tags pin the L2 (Valkey) wire format.
type Meta struct {
	ID        string         `json:"id"`
	Type      model.RoomType `json:"type"`
	Name      string         `json:"name"`
	SiteID    string         `json:"siteId"`
	UserCount int            `json:"userCount"`
}

// Loader fetches a fresh Meta for the given roomID. The cache calls
// Loader on miss; a non-nil error short-circuits the cache (the error
// is returned to the caller and nothing is cached).
type Loader func(ctx context.Context, roomID string) (Meta, error)

// Cache is an LRU+TTL cache of room Meta values, deduped via
// singleflight on miss.
type Cache struct {
	lru    *lru.LRU[string, Meta]
	loader Loader
	sf     singleflight.Group

	metrics Recorder
}

// Option configures a Cache at construction.
type Option func(*Cache)

// WithMetrics overrides the L1 hit/miss/error recorder. Defaults to the
// package-default cachemetrics recorder tagged cache="roommeta",tier="l1"
// (distinct from the tier="l2" series ReadThrough records).
func WithMetrics(r Recorder) Option {
	return func(c *Cache) { c.metrics = r }
}

// New constructs a Cache with the given capacity, TTL, and loader.
// size and ttl must both be positive; loader must be non-nil.
func New(size int, ttl time.Duration, loader Loader, opts ...Option) (*Cache, error) {
	if size <= 0 {
		return nil, fmt.Errorf("roommetacache: size must be positive, got %d", size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("roommetacache: ttl must be positive, got %v", ttl)
	}
	if loader == nil {
		return nil, fmt.Errorf("roommetacache: loader must not be nil")
	}
	c := &Cache{
		lru:     lru.NewLRU[string, Meta](size, nil, ttl),
		loader:  loader,
		metrics: cachemetrics.For("roommeta", "l1"),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Get returns the cached Meta for roomID. On miss it calls the configured
// loader (deduped via singleflight) and caches the result. Loader errors
// are returned to the caller and not cached.
func (c *Cache) Get(ctx context.Context, roomID string) (Meta, error) {
	if v, ok := c.lru.Get(roomID); ok {
		c.metrics.Hit(ctx)
		return v, nil
	}

	resCh := c.sf.DoChan(roomID, func() (interface{}, error) {
		// Recheck inside singleflight in case a sibling populated the entry while we waited.
		if cached, ok := c.lru.Get(roomID); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		loaded, err := c.loader(fetchCtx, roomID)
		if err != nil {
			return Meta{}, err
		}
		c.lru.Add(roomID, loaded)
		return loaded, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			c.metrics.Error(ctx)
			return Meta{}, fmt.Errorf("get room meta for %q: %w", roomID, res.Err)
		}
		c.metrics.Miss(ctx)
		return res.Val.(Meta), nil
	case <-ctx.Done():
		c.metrics.Error(ctx)
		return Meta{}, ctx.Err()
	}
}

// Invalidate removes any cached entry for roomID. Safe to call when
// no entry exists; in that case it is a no-op. Included from v1 even
// though no caller uses it, so future event-driven invalidation work
// plugs in without an interface change.
func (c *Cache) Invalidate(roomID string) {
	c.lru.Remove(roomID)
}

// MetaProvider is the minimal interface implemented by any type that
// can fetch a Meta. WrapStore consumes this constraint.
type MetaProvider interface {
	GetRoomMeta(ctx context.Context, roomID string) (Meta, error)
}

// Wrapper caches GetRoomMeta calls for an underlying MetaProvider. The
// inner provider is stored in the exported S field so callers can access
// all other methods of the underlying store via S.
//
// Note: Go's type system does not allow embedding a type parameter, so
// method promotion is not automatic. Callers that need the full service
// Store interface should compose a local struct that embeds the inner
// store for its other methods and delegates GetRoomMeta to a *Wrapper.
// See newCachedMetaStore in broadcast-worker/metacache.go for the pattern.
type Wrapper[S MetaProvider] struct {
	// S is the underlying MetaProvider. Exported so service adapters can
	// embed it alongside the *Wrapper.
	S     S
	cache *Cache
}

// WrapStore builds a Wrapper[S] that caches GetRoomMeta calls. size and
// ttl are passed directly to the underlying Cache and must be positive.
func WrapStore[S MetaProvider](inner S, size int, ttl time.Duration) (*Wrapper[S], error) {
	loader := func(ctx context.Context, roomID string) (Meta, error) {
		return inner.GetRoomMeta(ctx, roomID)
	}
	cache, err := New(size, ttl, loader)
	if err != nil {
		return nil, err
	}
	return &Wrapper[S]{S: inner, cache: cache}, nil
}

// GetRoomMeta serves from the cache, falling through to the underlying
// MetaProvider on miss.
func (w *Wrapper[S]) GetRoomMeta(ctx context.Context, roomID string) (Meta, error) {
	return w.cache.Get(ctx, roomID)
}

// FetchFromMongo runs the canonical projected FindOne against a rooms
// collection and decodes into Meta. Both gatekeeper and broadcast-worker
// store implementations call this so the projection key set stays in one
// place. The returned error wraps mongo.ErrNoDocuments on miss and is
// safe to errors.Is-check.
func FetchFromMongo(ctx context.Context, rooms *mongo.Collection, roomID string) (Meta, error) {
	opts := options.FindOne().SetProjection(bson.M{
		"type":      1,
		"name":      1,
		"siteId":    1,
		"userCount": 1,
	})
	var doc struct {
		ID        string         `bson:"_id"`
		Type      model.RoomType `bson:"type"`
		Name      string         `bson:"name"`
		SiteID    string         `bson:"siteId"`
		UserCount int            `bson:"userCount"`
	}
	if err := rooms.FindOne(ctx, bson.M{"_id": roomID}, opts).Decode(&doc); err != nil {
		return Meta{}, fmt.Errorf("fetch room meta %s: %w", roomID, err)
	}
	return Meta{
		ID:        doc.ID,
		Type:      doc.Type,
		Name:      doc.Name,
		SiteID:    doc.SiteID,
		UserCount: doc.UserCount,
	}, nil
}

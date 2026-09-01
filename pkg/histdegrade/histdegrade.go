// Package histdegrade stores and reads the per-site "message history is degraded"
// marker. message-worker sets it when Cassandra history writes start failing and
// clears it once the replay backlog drains; history-service reads it to stamp
// incompleteSince on history responses, and message-worker reads it to hold back
// stale thread badges and unresolvable quotes during the recovery drain.
package histdegrade

import (
	"context"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/singleflight"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/hmchangw/chat/pkg/cachemetrics"
)

// Marker is the per-site degradation record. Absence of the document means healthy.
type Marker struct {
	SiteID        string `json:"siteId"        bson:"_id"`
	DegradedSince int64  `json:"degradedSince" bson:"degradedSince"` // UTC millis
	UpdatedAt     int64  `json:"updatedAt"     bson:"updatedAt"`     // UTC millis
}

// Store persists markers in MongoDB.
type Store struct {
	col *mongo.Collection
}

func NewStore(db *mongo.Database) *Store {
	return &Store{col: db.Collection("history_degradations")}
}

// Set marks the site degraded. DegradedSince is written with $setOnInsert so the
// first failure's timestamp wins: concurrent workers and multiple pods racing on
// the same outage converge on one start time rather than ratcheting it forward.
func (s *Store) Set(ctx context.Context, siteID string, sinceMillis int64) error {
	_, err := s.col.UpdateOne(ctx,
		bson.M{"_id": siteID},
		bson.M{
			"$setOnInsert": bson.M{"degradedSince": sinceMillis},
			"$set":         bson.M{"updatedAt": sinceMillis},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("set history degraded marker for %s: %w", siteID, err)
	}
	return nil
}

// Clear removes the marker. Clearing an already-clear site is not an error.
func (s *Store) Clear(ctx context.Context, siteID string) error {
	if _, err := s.col.DeleteOne(ctx, bson.M{"_id": siteID}); err != nil {
		return fmt.Errorf("clear history degraded marker for %s: %w", siteID, err)
	}
	return nil
}

// Get returns the site's marker, or (nil, nil) when the site is healthy.
func (s *Store) Get(ctx context.Context, siteID string) (*Marker, error) {
	var m Marker
	err := s.col.FindOne(ctx, bson.M{"_id": siteID},
		options.FindOne().SetProjection(bson.M{"degradedSince": 1, "updatedAt": 1}),
	).Decode(&m)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get history degraded marker for %s: %w", siteID, err)
	}
	return &m, nil
}

// GetFunc loads a site's marker. *Store.Get satisfies it.
type GetFunc func(ctx context.Context, siteID string) (*Marker, error)

// Recorder records the outcome of a cache lookup. cachemetrics.Recorder
// satisfies it; tests substitute a spy.
type Recorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// CachedReader fronts a GetFunc with an LRU+TTL cache and collapses concurrent
// misses through singleflight. The marker changes at most twice per incident, so a
// history read is not worth a Mongo round-trip per call — and without the miss
// collapse every history RPC in flight when the TTL rolls would issue its own
// FindOne, a synchronized stampede every ttl on the busiest read path in the
// service. Same shape as pkg/userstore.Cache and pkg/roommetacache.Cache.
type CachedReader struct {
	get     GetFunc
	entries *lru.LRU[string, *int64]
	sf      singleflight.Group
	metrics Recorder
}

// CachedReaderOption configures a CachedReader at construction.
type CachedReaderOption func(*CachedReader)

// WithMetrics overrides the hit/miss/error recorder. Defaults to the package
// recorder tagged cache="history_degraded",tier="l1".
func WithMetrics(r Recorder) CachedReaderOption {
	return func(c *CachedReader) { c.metrics = r }
}

// cacheSize bounds the entry count. Callers bind one site for the process
// lifetime, so this only exists so a key is never unbounded by construction.
const cacheSize = 16

// minTTL floors the cache TTL. See NewCachedReader.
const minTTL = time.Millisecond

// fetchTimeout bounds the detached shared load so a hung Mongo cannot leak the
// singleflight goroutine or pin the in-flight key.
const fetchTimeout = 10 * time.Second

func NewCachedReader(get GetFunc, ttl time.Duration, opts ...CachedReaderOption) *CachedReader {
	if ttl < minTTL {
		// The expirable LRU builds a ttl/2 ticker and panics on a non-positive
		// interval, so a zero-valued config would take the process down at startup
		// rather than merely caching badly.
		ttl = minTTL
	}
	r := &CachedReader{
		get:     get,
		entries: lru.NewLRU[string, *int64](cacheSize, nil, ttl),
		metrics: cachemetrics.For("history_degraded", "l1"),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// DegradedSince returns the site's degraded-since timestamp (UTC millis), or nil
// when the site is healthy. Errors are returned to the caller and never cached.
func (r *CachedReader) DegradedSince(ctx context.Context, siteID string) (*int64, error) {
	if v, ok := r.entries.Get(siteID); ok {
		r.metrics.Hit(ctx)
		return v, nil
	}
	resCh := r.sf.DoChan(siteID, func() (interface{}, error) {
		if cached, ok := r.entries.Get(siteID); ok {
			return cached, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), fetchTimeout)
		defer cancel()
		marker, err := r.get(fetchCtx, siteID)
		if err != nil {
			return nil, err
		}
		var since *int64
		if marker != nil {
			v := marker.DegradedSince
			since = &v
		}
		r.entries.Add(siteID, since)
		return since, nil
	})
	select {
	case res := <-resCh:
		if res.Err != nil {
			r.metrics.Error(ctx)
			return nil, fmt.Errorf("read history degraded marker: %w", res.Err)
		}
		r.metrics.Miss(ctx)
		since, _ := res.Val.(*int64)
		return since, nil
	case <-ctx.Done():
		r.metrics.Error(ctx)
		return nil, ctx.Err()
	}
}

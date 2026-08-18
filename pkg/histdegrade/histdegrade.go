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
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

type cacheEntry struct {
	since     *int64
	expiresAt time.Time
}

// CachedReader fronts a GetFunc with a short TTL. The marker changes at most twice
// per incident, so a history read is not worth a Mongo round-trip per call.
type CachedReader struct {
	get GetFunc
	ttl time.Duration

	mu      sync.Mutex
	entries map[string]cacheEntry
}

func NewCachedReader(get GetFunc, ttl time.Duration) *CachedReader {
	return &CachedReader{get: get, ttl: ttl, entries: make(map[string]cacheEntry)}
}

// DegradedSince returns the site's degraded-since timestamp (UTC millis), or nil
// when the site is healthy. Errors are returned to the caller and never cached.
func (r *CachedReader) DegradedSince(ctx context.Context, siteID string) (*int64, error) {
	now := time.Now()

	r.mu.Lock()
	entry, ok := r.entries[siteID]
	r.mu.Unlock()
	if ok && now.Before(entry.expiresAt) {
		return entry.since, nil
	}

	marker, err := r.get(ctx, siteID)
	if err != nil {
		return nil, fmt.Errorf("read history degraded marker: %w", err)
	}
	var since *int64
	if marker != nil {
		v := marker.DegradedSince
		since = &v
	}

	r.mu.Lock()
	r.entries[siteID] = cacheEntry{since: since, expiresAt: now.Add(r.ttl)}
	r.mu.Unlock()

	return since, nil
}

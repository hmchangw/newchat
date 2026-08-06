package cassrepo

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/hmchangw/chat/history-service/internal/models"
)

// loadSealedBucket reads the COMPLETE (room, bucket) partition in clustering
// order (created_at DESC, message_id DESC) and decrypts each row, for population
// into the per-bucket cache. It bounds the read with LIMIT maxRows+1: a result
// of more than maxRows means the bucket is oversized, so it returns
// (nil, true, nil) and the caller declines to cache it, falling back to a
// bounded live query for that walk step.
func (r *Repository) loadSealedBucket(ctx context.Context, roomID string, bucket int64, maxRows int) (msgs []models.Message, oversized bool, err error) {
	q := r.session.Query(
		messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC LIMIT ?`,
		roomID, bucket, maxRows+1,
	).WithContext(ctx)

	iter := q.Iter()
	rows, scanErr := r.scanMessagesUpTo(ctx)(iter, maxRows+1)
	if closeErr := iter.Close(); closeErr != nil {
		return nil, false, fmt.Errorf("load sealed bucket %d: %w", bucket, closeErr)
	}
	if scanErr != nil {
		return nil, false, fmt.Errorf("load sealed bucket %d: %w", bucket, scanErr)
	}
	if len(rows) > maxRows {
		return nil, true, nil
	}
	return rows, false, nil
}

// bustBucket invalidates the per-bucket cache entry for the message's bucket
// after a mutation. No-op when caching is disabled. Callers pass the createdAt
// of the row whose messages_by_room partition changed (the message itself, or a
// thread parent whose tcount was recomputed).
func (r *Repository) bustBucket(ctx context.Context, roomID string, createdAt time.Time) {
	if r.bucketCache == nil {
		return
	}
	r.bucketCache.Bust(ctx, roomID, r.bucket.Of(createdAt))
}

// sliceBounded applies, on a created_at-DESC slice, the in-memory equivalents of
// the walk's row predicates and caps the result at limit:
//   - before (exclusive upper): drop rows with created_at >= *before — mirrors
//     the first-bucket CQL `created_at < ?`.
//   - since (exclusive lower): drop rows with created_at <= *since — mirrors the
//     floor-bucket CQL `created_at > ?`.
//
// A nil bound is unbounded on that side. The returned slice is a sub-slice view
// of rows; callers that mutate must copy (the cache already decodes a fresh
// slice per Get, so the walker's filtering is safe). Comparisons are strict,
// matching Cassandra's `<`/`>` on the created_at clustering column.
func sliceBounded(rows []models.Message, before, since *time.Time, limit int) []models.Message {
	start := 0
	if before != nil {
		// First index whose created_at < before; earlier rows (created_at >= before) are dropped.
		start = sort.Search(len(rows), func(i int) bool { return rows[i].CreatedAt.Before(*before) })
	}
	end := len(rows)
	if since != nil {
		// First index whose created_at <= since; that row and later (older) rows are dropped.
		end = sort.Search(len(rows), func(i int) bool { return !rows[i].CreatedAt.After(*since) })
	}
	if start >= end {
		return []models.Message{}
	}
	out := rows[start:end]
	if limit >= 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

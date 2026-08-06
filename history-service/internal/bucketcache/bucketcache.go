// Package bucketcache is an L1 (in-process) + L2 (Valkey) store of whole sealed
// message buckets for history-service, keyed by (roomID, bucket). The cassrepo
// walker consults it for every sealed bucket a LoadHistory read crosses, so the
// multi-bucket walk is computed once per partition and reused across reads,
// users, and page sizes.
//
// Values are gob-encoded []models.Message (NOT JSON: models.Message.Reactions
// is a struct-keyed, marshal-only map that JSON cannot round-trip). Get decodes
// a fresh slice on every call, so the walker's in-memory bounds filtering and
// the service layer's later in-place redaction never mutate a cached blob.
//
// Every Valkey interaction is fail-open: a nil client or any Valkey error
// degrades to a miss (Get) or is swallowed (Put/Bust); the caller falls back to
// a live Cassandra read. There is no generation counter — one key per bucket, so
// invalidation is a plain DEL. New messages never touch a sealed bucket, so
// ordinary message flow requires no invalidation at all.
package bucketcache

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Key is the L2 (Valkey) key for a room's sealed bucket. The {roomID} hash tag
// colocates a room's buckets in the same cluster slot.
func Key(roomID string, bucket int64) string {
	return "hist:{" + roomID + "}:bkt:" + strconv.FormatInt(bucket, 10)
}

// Cache stores whole sealed buckets across an L1 LRU+TTL and an L2 Valkey tier.
type Cache struct {
	valkey valkeyutil.Client // nil disables L2 (and, with no L1 loader, all caching)
	l1     *lru.LRU[string, []byte]
	ttl    time.Duration
	l1Rec  cachemetrics.Recorder
	l2Rec  cachemetrics.Recorder
}

// NewCache builds a bucket cache with an L1 of l1Size entries expiring after
// ttl, backed by the given Valkey client (which may be nil to disable L2).
// l1Size and ttl must be positive.
func NewCache(valkey valkeyutil.Client, l1Size int, ttl time.Duration) (*Cache, error) {
	if l1Size <= 0 {
		return nil, fmt.Errorf("bucketcache: l1 size must be positive, got %d", l1Size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("bucketcache: ttl must be positive, got %v", ttl)
	}
	return &Cache{
		valkey: valkey,
		l1:     lru.NewLRU[string, []byte](l1Size, nil, ttl),
		ttl:    ttl,
		l1Rec:  cachemetrics.For("history_bucket", "l1"),
		l2Rec:  cachemetrics.For("history_bucket", "l2"),
	}, nil
}

// Get returns the cached bucket (freshly decoded) and true on hit, or (nil,
// false) on miss or any fail-open degradation.
func (c *Cache) Get(ctx context.Context, roomID string, bucket int64) ([]models.Message, bool) {
	key := Key(roomID, bucket)

	if blob, ok := c.l1.Get(key); ok {
		if msgs, err := decode(blob); err == nil {
			c.l1Rec.Hit(ctx)
			return msgs, true
		}
		c.l1.Remove(key) // corrupt entry
	}
	c.l1Rec.Miss(ctx)

	if c.valkey == nil {
		return nil, false
	}
	blob, ok := c.l2Get(ctx, key)
	if !ok {
		c.l2Rec.Miss(ctx)
		return nil, false
	}
	msgs, err := decode(blob)
	if err != nil {
		c.l2Rec.Miss(ctx)
		return nil, false
	}
	c.l1.Add(key, blob)
	c.l2Rec.Hit(ctx)
	return msgs, true
}

// Put caches msgs (the complete bucket) under (roomID, bucket). Best-effort:
// encode/Valkey errors are logged and swallowed; L1 is populated independently
// of the L2 write so it still absorbs reads when Valkey is unavailable.
func (c *Cache) Put(ctx context.Context, roomID string, bucket int64, msgs []models.Message) {
	blob, err := encode(msgs)
	if err != nil {
		slog.WarnContext(ctx, "bucketcache: encode failed, not caching", "error", err)
		return
	}
	key := Key(roomID, bucket)
	c.l1.Add(key, blob)
	if c.valkey == nil {
		return
	}
	if err := c.valkey.Set(ctx, key, string(blob), c.ttl); err != nil {
		slog.WarnContext(ctx, "bucketcache: L2 populate failed (TTL will reconcile)", "error", err)
	}
}

// Bust removes a bucket from this instance's L1 and from L2 (DEL). Best-effort:
// a nil client skips the DEL and any Valkey error is swallowed. Other replicas'
// L1 reconcile within ttl.
func (c *Cache) Bust(ctx context.Context, roomID string, bucket int64) {
	key := Key(roomID, bucket)
	c.l1.Remove(key)
	if c.valkey == nil {
		return
	}
	if err := c.valkey.Del(ctx, key); err != nil {
		slog.WarnContext(ctx, "bucketcache: L2 invalidate failed (TTL will reconcile)", "room_id", roomID, "bucket", bucket, "error", err)
	}
}

// l2Get reads a raw cache blob from the L2 (Valkey) tier. It returns
// (nil, false) on a miss or any transport error; a genuine cache miss is
// silent while other errors are logged at warn (fail-open — the caller then
// loads live).
func (c *Cache) l2Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := c.valkey.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			slog.WarnContext(ctx, "bucketcache: L2 read failed", "error", err)
		}
		return nil, false
	}
	return []byte(val), true
}

// encode gob-encodes a bucket's messages for cache storage. gob (not JSON) is
// used because models.Message.Reactions is a struct-keyed, marshal-only map that
// JSON cannot round-trip.
func encode(msgs []models.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(msgs); err != nil {
		return nil, fmt.Errorf("gob encode bucket: %w", err)
	}
	return buf.Bytes(), nil
}

// decode reverses encode, returning a freshly-allocated slice on every call so a
// cached blob is never aliased into a caller that mutates it in place.
func decode(blob []byte) ([]models.Message, error) {
	var msgs []models.Message
	if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&msgs); err != nil {
		return nil, fmt.Errorf("gob decode bucket: %w", err)
	}
	return msgs, nil
}

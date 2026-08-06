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
	"sync"
	"sync/atomic"
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
// L1 is bounded by total encoded bytes, not entry count: a bucket that holds few
// large messages costs the same budget as many small buckets, so the per-replica
// memory ceiling is explicit regardless of bucket density.
type Cache struct {
	valkey   valkeyutil.Client // nil disables L2 (and, with no L1 loader, all caching)
	l1       *lru.LRU[string, []byte]
	maxBytes int64
	curBytes atomic.Int64 // sum of len(blob) currently held in L1
	putMu    sync.Mutex   // serializes L1 writes + the evict-to-budget loop; reads (Get) are unlocked
	ttl      time.Duration
	l1Rec    cachemetrics.Recorder
	l2Rec    cachemetrics.Recorder
}

// NewCache builds a bucket cache whose L1 holds up to maxBytes of encoded bucket
// data (evicting least-recently-used buckets to stay under budget) with each
// entry also expiring after ttl, backed by the given Valkey client (which may be
// nil to disable L2). maxBytes and ttl must be positive.
func NewCache(valkey valkeyutil.Client, maxBytes int64, ttl time.Duration) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("bucketcache: max bytes must be positive, got %d", maxBytes)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("bucketcache: ttl must be positive, got %v", ttl)
	}
	c := &Cache{
		valkey:   valkey,
		maxBytes: maxBytes,
		ttl:      ttl,
		l1Rec:    cachemetrics.For("history_bucket", "l1"),
		l2Rec:    cachemetrics.For("history_bucket", "l2"),
	}
	// size=0 disables the LRU's count-based eviction; we evict by bytes instead.
	// The eviction callback fires on every removal path (RemoveOldest, Remove, and
	// TTL expiry via the library's background reaper), so it is the single point
	// that keeps curBytes in sync. It runs under the LRU's own lock, so it must
	// stay lock-free — an atomic add satisfies that.
	c.l1 = lru.NewLRU(0, func(_ string, blob []byte) {
		c.curBytes.Add(-int64(len(blob)))
	}, ttl)
	return c, nil
}

// l1Store inserts blob under key and evicts least-recently-used buckets until L1
// is back under maxBytes. It Removes any existing entry first (rather than
// letting Add replace it in place, which would not fire the eviction callback)
// so curBytes always reflects exactly what L1 holds.
func (c *Cache) l1Store(key string, blob []byte) {
	c.putMu.Lock()
	defer c.putMu.Unlock()
	c.l1.Remove(key) // decrements curBytes via onEvict if key was present (even if expired)
	c.l1.Add(key, blob)
	c.curBytes.Add(int64(len(blob)))
	for c.curBytes.Load() > c.maxBytes {
		if _, _, ok := c.l1.RemoveOldest(); !ok {
			break // L1 empty (a single blob larger than the whole budget) — nothing more to shed
		}
	}
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
	c.l1Store(key, blob)
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
	c.l1Store(key, blob)
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

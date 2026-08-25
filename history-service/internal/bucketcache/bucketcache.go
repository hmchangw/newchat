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
// A bucket too dense to cache gets a one-byte "oversized" marker under the same
// key (PutOversized), so the walker learns that verdict from the cache instead
// of re-running the full-bucket probe on every read.
//
// Every Valkey interaction is fail-open: a nil client or any Valkey error
// degrades to a miss (Get) or is swallowed (Put/PutOversized/Bust); the caller
// falls back to a live Cassandra read. There is no generation counter — one key
// per bucket, holding either the rows or the marker, so invalidation is a plain
// DEL. New messages never touch a sealed bucket, so ordinary message flow
// requires no invalidation at all.
//
// # Invalidation boundary
//
// Bust covers the mutations history-service itself performs: edit, delete, pin,
// unpin, and reaction add/remove. It does NOT cover writes into a sealed bucket
// made by anything else, and those exist — most routinely, message-worker and
// bot-message-worker updating a thread parent's tcount / thread_last_msg_at /
// thread_room_id, since a parent is often old enough to have sealed. Those reads
// are stale until the entry expires.
//
// So the TTL, not the mutation, is the visibility bound for a sealed bucket.
// That is why the cache is off unless explicitly enabled; the four known gaps
// and what they cost are enumerated on config.Config.BucketCacheOptIn, which is
// what an operator reads before turning it on.
//
// Note that a Bust reaches only this process's L1 plus the shared Valkey key.
// Get returns on an L1 hit before consulting L2, so a sibling replica holding
// the bucket keeps serving its own copy until that entry's TTL — invalidating
// the shared tier alone does not make a mutation visible fleet-wide.
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

// Lookup is what a Get found under a bucket's key.
type Lookup int

const (
	// Miss: nothing usable cached — the caller loads the bucket from Cassandra.
	Miss Lookup = iota
	// Hit: the returned slice is the complete bucket.
	Hit
	// Oversized: the bucket is known to hold more rows than the cache accepts,
	// so it was deliberately never stored. The caller skips the full-bucket
	// probe that would rediscover this and queries live directly.
	Oversized
)

// Stored values carry a one-byte tag so a cached bucket and an oversized marker
// can share one key (and so a bust clears whichever is there with a single DEL).
// An unknown tag decodes to Miss, which also makes a rolling deploy over
// untagged entries written by an older build a plain cache miss.
const (
	// tagBucket is the current cached-bucket layout. Tag 1 was the previous one
	// (a bare gob []models.Message, which lost a zero TCount — see bucketBlob);
	// it is retired rather than reused, so entries an older build left in Valkey
	// fall to interpret's unknown-tag case and degrade to a plain miss.
	tagBucket    byte = 3
	tagOversized byte = 2
)

// oversizedMarker is the entire stored value for a known-oversized bucket: one
// byte, so a dense room costs the byte budget almost nothing to remember.
var oversizedMarker = []byte{tagOversized}

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

// minTTL is the smallest TTL golang-lru can service. Its reaper ticks every
// ttl/100, and time.NewTicker panics on a non-positive interval, so anything
// under 100ns truncates to a zero interval and takes the process down from a
// goroutine we cannot recover in. (A ttl of exactly 0 is safe — the library
// remaps it to a no-eviction sentinel and never starts the reaper — but we
// reject that separately as a misconfiguration.)
const minTTL = 100 * time.Nanosecond

// NewCache builds a bucket cache whose L1 holds up to maxBytes of encoded bucket
// data (evicting least-recently-used buckets to stay under budget) with each
// entry also expiring after ttl, backed by the given Valkey client (which may be
// nil to disable L2). maxBytes must be positive and ttl at least minTTL.
func NewCache(valkey valkeyutil.Client, maxBytes int64, ttl time.Duration) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("bucketcache: max bytes must be positive, got %d", maxBytes)
	}
	if ttl < minTTL {
		return nil, fmt.Errorf("bucketcache: ttl must be at least %v, got %v", minTTL, ttl)
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

// Get reports what is cached for a bucket: Hit with the freshly-decoded rows,
// Oversized (no rows) for a bucket previously found too dense to cache, or Miss
// on a cold key or any fail-open degradation. An Oversized answer counts as a
// tier hit in the metrics — the cache did answer, saving the probe — even though
// the caller still reads Cassandra for the rows it serves.
func (c *Cache) Get(ctx context.Context, roomID string, bucket int64) ([]models.Message, Lookup) {
	key := Key(roomID, bucket)

	if blob, ok := c.l1.Get(key); ok {
		if msgs, res, ok := interpret(blob); ok {
			c.l1Rec.Hit(ctx)
			return msgs, res
		}
		c.l1.Remove(key) // corrupt entry
	}
	c.l1Rec.Miss(ctx)

	if c.valkey == nil {
		return nil, Miss
	}
	blob, ok := c.l2Get(ctx, key)
	if !ok {
		c.l2Rec.Miss(ctx)
		return nil, Miss
	}
	msgs, res, ok := interpret(blob)
	if !ok {
		c.l2Rec.Miss(ctx)
		return nil, Miss
	}
	c.l1Store(key, blob)
	c.l2Rec.Hit(ctx)
	return msgs, res
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

// PutOversized records that a bucket holds more rows than the cache accepts, so
// later reads skip the full-bucket probe that discovered it and go straight to a
// bounded live query. Best-effort, like Put. The marker shares the bucket's key
// and TTL: it expires on its own, Bust clears it, and a later Put supersedes it
// (a bucket that shrinks back under the cap is re-cached on the next read after
// the mutation that shrank it busted the key).
func (c *Cache) PutOversized(ctx context.Context, roomID string, bucket int64) {
	key := Key(roomID, bucket)
	c.l1Store(key, oversizedMarker)
	if c.valkey == nil {
		return
	}
	if err := c.valkey.Set(ctx, key, string(oversizedMarker), c.ttl); err != nil {
		slog.WarnContext(ctx, "bucketcache: L2 oversized marker failed (TTL will reconcile)", "error", err)
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

// interpret reads a stored value as the lookup it represents. ok is false for an
// empty, unknown-tag, or corrupt value; every such case degrades to a miss so a
// bad entry costs a live read rather than failing the request.
func interpret(blob []byte) (msgs []models.Message, res Lookup, ok bool) {
	if len(blob) == 0 {
		return nil, Miss, false
	}
	switch blob[0] {
	case tagOversized:
		return nil, Oversized, true
	case tagBucket:
		msgs, err := decode(blob[1:])
		if err != nil {
			return nil, Miss, false
		}
		return msgs, Hit, true
	default:
		return nil, Miss, false
	}
}

// bucketBlob is the gob body of a cached bucket.
//
// TCounts carries Message.TCount out of band. gob flattens a pointer to the
// value it points at and then omits any field whose value is the zero value, so
// a *int pointing at 0 — a thread parent whose replies have all been deleted —
// decodes back as nil. Since `json:"tcount,omitempty"` on a pointer emits
// "tcount":0 for the pointer and drops the field entirely for nil, that would
// serialize the same message differently depending on whether the read was
// served from cache.
//
// Only TCount needs this. The other pointer fields are timestamps and structs
// whose zero value never occurs in a stored row, so gob's omission is
// unobservable there — but a new pointer field whose zero value IS meaningful
// would need the same treatment.
type bucketBlob struct {
	Rows    []models.Message
	TCounts []optionalInt
}

// optionalInt is a *int in a shape gob round-trips: presence lives in its own
// field, so a zero Val is no longer indistinguishable from absence.
type optionalInt struct {
	Set bool
	Val int
}

// encode gob-encodes a bucket's messages for cache storage behind a tag byte.
// gob (not JSON) is used because models.Message.Reactions is a struct-keyed,
// marshal-only map that JSON cannot round-trip.
func encode(msgs []models.Message) ([]byte, error) {
	tcounts := make([]optionalInt, len(msgs))
	for i := range msgs {
		if c := msgs[i].TCount; c != nil {
			tcounts[i] = optionalInt{Set: true, Val: *c}
		}
	}
	var buf bytes.Buffer
	buf.WriteByte(tagBucket)
	if err := gob.NewEncoder(&buf).Encode(bucketBlob{Rows: msgs, TCounts: tcounts}); err != nil {
		return nil, fmt.Errorf("gob encode bucket: %w", err)
	}
	return buf.Bytes(), nil
}

// decode reverses encode's gob body (the tag byte already stripped), returning a
// freshly-allocated slice on every call so a cached blob is never aliased into a
// caller that mutates it in place.
func decode(blob []byte) ([]models.Message, error) {
	var body bucketBlob
	if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&body); err != nil {
		return nil, fmt.Errorf("gob decode bucket: %w", err)
	}
	for i := range body.Rows {
		if i >= len(body.TCounts) {
			break
		}
		if c := body.TCounts[i]; c.Set {
			v := c.Val
			body.Rows[i].TCount = &v
		} else {
			body.Rows[i].TCount = nil
		}
	}
	return body.Rows, nil
}

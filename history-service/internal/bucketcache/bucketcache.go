// Package bucketcache is a Valkey-backed store of whole sealed message buckets
// for history-service, keyed by (roomID, bucket). The cassrepo walker consults
// it for every sealed bucket a LoadHistory read crosses, so the multi-bucket
// walk is computed once per partition and reused across reads, users, and page
// sizes.
//
// Values are gob-encoded (NOT JSON: models.Message.Reactions is a struct-keyed,
// marshal-only map that JSON cannot round-trip). Get decodes a fresh slice on
// every call, so the walker's in-memory bounds filtering and the service layer's
// later in-place redaction never mutate a cached blob.
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
// # One tier, deliberately
//
// There is no in-process L1. An earlier revision had one, and it made Bust
// unable to reach sibling replicas: a pod holding the bucket in its own memory
// kept serving pre-mutation rows until that entry's TTL, whatever happened in
// Valkey. That is a correctness hole in exchange for a network hop — and the
// hop is the cheap half, because an L1 caches the encoded blob, so every Get
// pays the gob decode on either tier (see BenchmarkDecode). Dropping the tier
// makes DEL authoritative fleet-wide, which is also what pkg/badgecache and
// pkg/roomsubcache do. pkg/roommetacache keeps an L1 and documents the same
// staleness it cannot close.
//
// # Invalidation boundary
//
// Bust covers the mutations history-service itself performs: edit, delete, pin,
// unpin, and reaction add/remove. It does NOT cover writes into a sealed bucket
// made by anything else, and those exist — most routinely, message-worker and
// bot-message-worker updating a thread parent's tcount / thread_last_msg_at /
// thread_room_id, since a parent is often old enough to have sealed. Those reads
// are stale until the entry expires. With a single shared tier a DEL from those
// writers would now close the gap; adding it is tracked separately.
//
// The four gaps an operator should weigh before enabling this are enumerated on
// config.Config.BucketCacheOptIn.
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

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// Key is the Valkey key for a room's sealed bucket. The {roomID} hash tag
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
// An unknown tag decodes to Miss, which also makes a rolling deploy over entries
// written by an older build a plain cache miss.
const (
	// tagBucket is the current cached-bucket layout. Tag 1 was the previous one
	// (a bare gob []models.Message, which lost a zero TCount — see bucketBlob);
	// it is retired rather than reused, so entries an older build left in Valkey
	// fall to interpret's unknown-tag case and degrade to a plain miss.
	tagBucket    byte = 3
	tagOversized byte = 2
)

// oversizedMarker is the entire stored value for a known-oversized bucket: one
// byte, so a dense room costs almost nothing to remember.
var oversizedMarker = []byte{tagOversized}

// Cache stores whole sealed buckets in Valkey, shared across replicas.
type Cache struct {
	valkey valkeyutil.Client // nil disables caching entirely
	ttl    time.Duration
	rec    cachemetrics.Recorder
}

// NewCache builds a bucket cache over the given Valkey client (which may be nil
// to disable caching), with each entry expiring after ttl. ttl must be positive.
func NewCache(valkey valkeyutil.Client, ttl time.Duration) (*Cache, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("bucketcache: ttl must be positive, got %v", ttl)
	}
	return &Cache{
		valkey: valkey,
		ttl:    ttl,
		rec:    cachemetrics.For("history_bucket", "valkey"),
	}, nil
}

// Get reports what is cached for a bucket: Hit with the freshly-decoded rows,
// Oversized (no rows) for a bucket previously found too dense to cache, or Miss
// on a cold key or any fail-open degradation. An Oversized answer counts as a
// hit in the metrics — the cache did answer, saving the probe — even though the
// caller still reads Cassandra for the rows it serves.
func (c *Cache) Get(ctx context.Context, roomID string, bucket int64) ([]models.Message, Lookup) {
	if c.valkey == nil {
		return nil, Miss
	}
	blob, ok := c.read(ctx, Key(roomID, bucket))
	if !ok {
		c.rec.Miss(ctx)
		return nil, Miss
	}
	msgs, res, ok := interpret(blob)
	if !ok {
		c.rec.Miss(ctx)
		return nil, Miss
	}
	c.rec.Hit(ctx)
	return msgs, res
}

// Put caches msgs (the complete bucket) under (roomID, bucket). Best-effort:
// encode/Valkey errors are logged and swallowed.
func (c *Cache) Put(ctx context.Context, roomID string, bucket int64, msgs []models.Message) {
	if c.valkey == nil {
		return
	}
	blob, err := encode(msgs)
	if err != nil {
		slog.WarnContext(ctx, "bucketcache: encode failed, not caching", "error", err)
		return
	}
	if err := c.valkey.Set(ctx, Key(roomID, bucket), string(blob), c.ttl); err != nil {
		slog.WarnContext(ctx, "bucketcache: populate failed (TTL will reconcile)", "error", err)
	}
}

// PutOversized records that a bucket holds more rows than the cache accepts, so
// later reads skip the full-bucket probe that discovered it and go straight to a
// bounded live query. Best-effort, like Put. The marker shares the bucket's key
// and TTL: it expires on its own, Bust clears it, and a later Put supersedes it
// (a bucket that shrinks back under the cap is re-cached on the next read after
// the mutation that shrank it busted the key).
func (c *Cache) PutOversized(ctx context.Context, roomID string, bucket int64) {
	if c.valkey == nil {
		return
	}
	if err := c.valkey.Set(ctx, Key(roomID, bucket), string(oversizedMarker), c.ttl); err != nil {
		slog.WarnContext(ctx, "bucketcache: oversized marker failed (TTL will reconcile)", "error", err)
	}
}

// Bust removes a bucket from the cache (DEL). Best-effort: a nil client skips
// the DEL and any Valkey error is swallowed. Because there is no per-replica
// tier, the delete is authoritative for every reader immediately.
func (c *Cache) Bust(ctx context.Context, roomID string, bucket int64) {
	if c.valkey == nil {
		return
	}
	if err := c.valkey.Del(ctx, Key(roomID, bucket)); err != nil {
		slog.WarnContext(ctx, "bucketcache: invalidate failed (TTL will reconcile)", "room_id", roomID, "bucket", bucket, "error", err)
	}
}

// read fetches a raw cache blob. It returns (nil, false) on a miss or any
// transport error; a genuine miss is silent while other errors are logged at
// warn (fail-open — the caller then loads live).
func (c *Cache) read(ctx context.Context, key string) ([]byte, bool) {
	val, err := c.valkey.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			slog.WarnContext(ctx, "bucketcache: read failed", "error", err)
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

// Package msgpagecache adds an L1 (in-process) + L2 (Valkey) read-through cache
// in front of history-service's Cassandra message-page reads. It decorates
// service.MessageReader and caches only the timestamp-anchored LoadHistory
// reads (GetMessagesBefore / GetMessagesBetweenDesc) whose walk stays in sealed
// buckets — the current (hot) bucket is always read live. Every other
// MessageReader method passes straight through.
//
// Because the cache key is compound (a room has many `before` anchors), entries
// cannot be enumerated for deletion; invalidation is a per-room generation
// counter in Valkey (hist:{roomID}:gen) that every key is namespaced by. A
// mutation calls Bump to increment it, orphaning the room's cached pages.
//
// Cache blobs are gob-encoded, NOT JSON: models.Message.Reactions is a
// struct-keyed map with a lossy, marshal-only MarshalJSON that cannot be
// unmarshalled, so a JSON round-trip would corrupt or fail. Decoding a fresh
// page from the blob on every hit also makes the cache copy-safe against the
// service layer's in-place redaction of the returned messages.
//
// Every Valkey interaction is fail-open: a nil client or any Valkey error
// degrades to the live Cassandra read (or, for Bump, is swallowed); only the
// backing read governs the returned error.
package msgpagecache

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
	"golang.org/x/sync/singleflight"

	"github.com/hmchangw/chat/history-service/internal/cassrepo"
	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/history-service/internal/service"
	"github.com/hmchangw/chat/pkg/cachemetrics"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// now returns the current time; overridable in tests so bucket boundaries are
// deterministic.
var now = func() time.Time { return time.Now().UTC() }

func genKey(roomID string) string { return "hist:{" + roomID + "}:gen" }

func pageKey(roomID string, gen uint64, method, bounds string, pageSize int) string {
	return "hist:{" + roomID + "}:pg:" + strconv.FormatUint(gen, 10) + ":" + method + ":" + bounds + ":" + strconv.Itoa(pageSize)
}

// boundsBefore keys a GetMessagesBefore read. `before` is the exact query bound
// (and the cross-user collision point); `floor` only terminates the walk, so it
// is quantized to its bucket to keep a drifting now-historyFloor out of the key.
func boundsBefore(before, floor time.Time, sizer msgbucket.Sizer) string {
	return "b=" + strconv.FormatInt(before.UnixMilli(), 10) + ":f=" + strconv.FormatInt(sizer.Of(floor), 10)
}

// boundsBetween keys a GetMessagesBetweenDesc read. Both bounds are exact:
// `before` is the upper bound and `since` (the user's access window) is the
// lower bound of the query, so it changes the result set, not just termination.
func boundsBetween(since, before time.Time) string {
	return "b=" + strconv.FormatInt(before.UnixMilli(), 10) + ":s=" + strconv.FormatInt(since.UnixMilli(), 10)
}

// Reader decorates a service.MessageReader with an L1+L2 cache for the sealed
// LoadHistory reads. All non-overridden methods pass through via the embed.
type Reader struct {
	service.MessageReader
	valkey valkeyutil.Client
	sizer  msgbucket.Sizer
	l1     *lru.LRU[string, []byte]
	genL1  *lru.LRU[string, uint64]
	sf     singleflight.Group
	ttl    time.Duration
	l1Rec  cachemetrics.Recorder
	l2Rec  cachemetrics.Recorder
}

// NewReader wraps inner with an L1 (size l1Size, entries expiring after ttl) and
// an L2 Valkey read-through. genTTL bounds how long a per-room generation is
// cached in L1 (the cross-replica invalidation window). A nil valkey disables
// caching (every read delegates). l1Size, ttl and genTTL must be positive.
func NewReader(inner service.MessageReader, valkey valkeyutil.Client, sizer msgbucket.Sizer, l1Size int, ttl, genTTL time.Duration) (*Reader, error) {
	if l1Size <= 0 {
		return nil, fmt.Errorf("msgpagecache: l1 size must be positive, got %d", l1Size)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("msgpagecache: ttl must be positive, got %v", ttl)
	}
	if genTTL <= 0 {
		return nil, fmt.Errorf("msgpagecache: gen ttl must be positive, got %v", genTTL)
	}
	return &Reader{
		MessageReader: inner,
		valkey:        valkey,
		sizer:         sizer,
		l1:            lru.NewLRU[string, []byte](l1Size, nil, ttl),
		genL1:         lru.NewLRU[string, uint64](l1Size, nil, genTTL),
		ttl:           ttl,
		l1Rec:         cachemetrics.For("history_msg_page", "l1"),
		l2Rec:         cachemetrics.For("history_msg_page", "l2"),
	}, nil
}

func (r *Reader) currentBucket() int64 { return r.sizer.Of(now()) }

// sealed reports whether a walk whose upper bound is `before` stays entirely in
// sealed buckets (strictly older than the current one) and is therefore safe to
// cache. The hot current bucket is always read live.
func (r *Reader) sealed(before time.Time) bool {
	return r.valkey != nil && r.sizer.Of(before) < r.currentBucket()
}

func (r *Reader) GetMessagesBefore(ctx context.Context, roomID string, before, floor time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	if !r.sealed(before) {
		return r.MessageReader.GetMessagesBefore(ctx, roomID, before, floor, pr)
	}
	return r.readThrough(ctx, roomID, "before", boundsBefore(before, floor, r.sizer), pr.PageSize,
		func(ctx context.Context) (cassrepo.Page[models.Message], error) {
			return r.MessageReader.GetMessagesBefore(ctx, roomID, before, floor, pr)
		})
}

func (r *Reader) GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, pr cassrepo.PageRequest) (cassrepo.Page[models.Message], error) {
	if !r.sealed(before) {
		return r.MessageReader.GetMessagesBetweenDesc(ctx, roomID, since, before, pr)
	}
	return r.readThrough(ctx, roomID, "between", boundsBetween(since, before), pr.PageSize,
		func(ctx context.Context) (cassrepo.Page[models.Message], error) {
			return r.MessageReader.GetMessagesBetweenDesc(ctx, roomID, since, before, pr)
		})
}

// flightResult is the singleflight payload. blob is non-nil in the normal path
// so each waiter decodes its own copy (copy-safe); page is the fallback used
// only when encoding failed and the value could not be cached.
type flightResult struct {
	blob []byte
	page cassrepo.Page[models.Message]
}

func (r *Reader) readThrough(
	ctx context.Context,
	roomID, method, bounds string,
	pageSize int,
	load func(context.Context) (cassrepo.Page[models.Message], error),
) (cassrepo.Page[models.Message], error) {
	key := pageKey(roomID, r.gen(ctx, roomID), method, bounds, pageSize)

	if blob, ok := r.l1.Get(key); ok {
		if page, err := decodePage(blob); err == nil {
			r.l1Rec.Hit(ctx)
			return page, nil
		}
		r.l1.Remove(key) // corrupt entry — drop and fall through
	}
	r.l1Rec.Miss(ctx)

	v, err, _ := r.sf.Do(key, func() (any, error) {
		// A sibling flight may have populated L1 already.
		if blob, ok := r.l1.Get(key); ok {
			return &flightResult{blob: blob}, nil
		}
		if blob, ok := r.l2Get(ctx, key); ok {
			r.l1.Add(key, blob)
			r.l2Rec.Hit(ctx)
			return &flightResult{blob: blob}, nil
		}
		r.l2Rec.Miss(ctx)

		page, lerr := load(ctx)
		if lerr != nil {
			return nil, lerr
		}
		blob, eerr := encodePage(page)
		if eerr != nil {
			slog.WarnContext(ctx, "msgpagecache: encode failed, serving uncached", "error", eerr)
			return &flightResult{page: page}, nil
		}
		r.l1.Add(key, blob)
		r.l2Set(ctx, key, blob)
		return &flightResult{blob: blob}, nil
	})
	if err != nil {
		return cassrepo.Page[models.Message]{}, err
	}

	fr := v.(*flightResult)
	if fr.blob == nil {
		return fr.page, nil // uncacheable fallback (encode failure)
	}
	page, derr := decodePage(fr.blob)
	if derr != nil {
		// Should never happen (we just encoded it); re-read live as a last resort.
		slog.WarnContext(ctx, "msgpagecache: decode of cached blob failed, reloading", "error", derr)
		return load(ctx)
	}
	return page, nil
}

// gen resolves a room's cache generation, fronting the Valkey read with a
// short-TTL L1. A missing counter reads as 0.
func (r *Reader) gen(ctx context.Context, roomID string) uint64 {
	gk := genKey(roomID)
	if g, ok := r.genL1.Get(gk); ok {
		return g
	}
	g := r.l2Gen(ctx, gk)
	r.genL1.Add(gk, g)
	return g
}

func (r *Reader) l2Gen(ctx context.Context, gk string) uint64 {
	val, err := r.valkey.Get(ctx, gk)
	if err != nil {
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			slog.WarnContext(ctx, "msgpagecache: gen read failed, treating as 0", "error", err)
		}
		return 0
	}
	g, perr := strconv.ParseUint(val, 10, 64)
	if perr != nil {
		slog.WarnContext(ctx, "msgpagecache: gen parse failed, treating as 0", "value", val, "error", perr)
		return 0
	}
	return g
}

func (r *Reader) l2Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := r.valkey.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, valkeyutil.ErrCacheMiss) {
			slog.WarnContext(ctx, "msgpagecache: L2 read failed", "error", err)
		}
		return nil, false
	}
	return []byte(val), true
}

func (r *Reader) l2Set(ctx context.Context, key string, blob []byte) {
	if err := r.valkey.Set(ctx, key, string(blob), r.ttl); err != nil {
		slog.WarnContext(ctx, "msgpagecache: L2 populate failed (TTL will reconcile)", "error", err)
	}
}

// Bump increments a room's generation counter, orphaning every cached page for
// that room. Best-effort: a nil client is a no-op and any Valkey error is
// logged and swallowed (the page TTL reconciles a missed bump). The bumping
// instance's gen L1 is updated immediately; other replicas reconcile within
// their gen L1 TTL.
func (r *Reader) Bump(ctx context.Context, roomID string) {
	if r.valkey == nil {
		return
	}
	gk := genKey(roomID)
	n, err := r.valkey.IncrEx(ctx, gk, 0)
	if err != nil {
		slog.WarnContext(ctx, "msgpagecache: gen bump failed (TTL will reconcile)", "room_id", roomID, "error", err)
		return
	}
	if n >= 0 {
		r.genL1.Add(gk, uint64(n))
	}
}

func encodePage(p cassrepo.Page[models.Message]) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return nil, fmt.Errorf("gob encode page: %w", err)
	}
	return buf.Bytes(), nil
}

func decodePage(blob []byte) (cassrepo.Page[models.Message], error) {
	var p cassrepo.Page[models.Message]
	if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&p); err != nil {
		return cassrepo.Page[models.Message]{}, fmt.Errorf("gob decode page: %w", err)
	}
	return p, nil
}

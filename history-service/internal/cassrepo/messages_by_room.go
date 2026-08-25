package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/history-service/internal/bucketcache"
	"github.com/hmchangw/chat/history-service/internal/models"
)

const baseColumns = "room_id, created_at, message_id, thread_room_id, sender, " +
	"msg, mentions, attachments, card, card_action, tshow, tcount, thread_last_msg_at, " +
	"thread_parent_id, thread_parent_created_at, quoted_parent_message, " +
	"visible_to, reactions, deleted, " +
	"type, sys_msg_data, site_id, edited_at, updated_at, pinned_at, " +
	"enc_payload, enc_meta"

const messageByRoomQuery = "SELECT " + baseColumns + " FROM messages_by_room"

// startBucketFromCursor returns the walk's start bucket and any in-bucket pageState from the cursor.
// Out-of-range cursor buckets are rejected to prevent tampered cursors from consuming maxBuckets empty reads.
func startBucketFromCursor(pageReq PageRequest, direction walkDirection, defaultBucket, floorBucket int64) (int64, []byte, error) {
	if pageReq.Cursor == nil {
		return defaultBucket, nil, nil
	}
	encoded := pageReq.Cursor.Encode()
	if encoded == "" {
		return defaultBucket, nil, nil
	}
	bucket, pageState, err := decodeBucketCursor(encoded)
	if err != nil {
		return 0, nil, fmt.Errorf("start bucket from cursor: %w", err)
	}
	switch direction {
	case walkDesc:
		// Legitimate range: floorBucket <= bucket <= defaultBucket.
		if bucket > defaultBucket || bucket < floorBucket {
			return defaultBucket, nil, nil
		}
	case walkAsc:
		// Legitimate range: defaultBucket <= bucket <= floorBucket (ASC's
		// "floor" is the ceiling).
		if bucket < defaultBucket || bucket > floorBucket {
			return defaultBucket, nil, nil
		}
	}
	return bucket, pageState, nil
}

// scanRawMessagesUpTo consumes up to remaining rows from iter via structScan
// WITHOUT decrypting, so rows keep enc_payload/enc_meta exactly as Cassandra
// stores them. The per-bucket cache populates from this: caching the sealed
// (encrypted) representation keeps decrypted message bodies out of Valkey, so
// the cache inherits the at-rest posture of the table it mirrors instead of
// creating a plaintext copy in a second datastore. Readers decrypt what they
// serve — see decryptRows.
func scanRawMessagesUpTo(iter *gocql.Iter, remaining int) ([]models.Message, error) {
	out := make([]models.Message, 0, remaining)
	for len(out) < remaining {
		var m models.Message
		ok, err := structScan(iter, &m)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		out = append(out, m)
	}
	return out, nil
}

// scanMessagesUpTo returns a fillPage scan callback that consumes up to
// remaining rows from iter via structScan and decrypts any enc_payload rows in
// place via r.decryptIfNeeded. A decrypt (or scan) error aborts the walk:
// fillPage discards the accumulated rows and propagates the error to the caller.
func (r *Repository) scanMessagesUpTo(ctx context.Context) func(iter *gocql.Iter, remaining int) ([]models.Message, error) {
	return func(iter *gocql.Iter, remaining int) ([]models.Message, error) {
		out := make([]models.Message, 0, remaining)
		for len(out) < remaining {
			var m models.Message
			ok, err := structScan(iter, &m)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if err := r.decryptIfNeeded(ctx, &m); err != nil {
				return nil, err
			}
			out = append(out, m)
		}
		return out, nil
	}
}

// fillMessagePage runs a bucketed message-table walk with the repository's
// shared walk parameters (bucket sizer, max-walk depth, fan-out, row scan) bound
// in, so each reader supplies only what varies: direction, bounds, and the
// per-bucket query builder. A new walk-level parameter is then a one-line change
// here rather than an edit at every reader.
func (r *Repository) fillMessagePage(ctx context.Context, direction walkDirection, startBucket, floorBucket int64, pageReq PageRequest, initialPageState []byte, queryFn bucketQueryFn) (Page[models.Message], error) {
	res, err := fillPage[models.Message](
		ctx, r.bucket, direction, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, r.walkFanout, queryFn, r.scanMessagesUpTo(ctx),
	)
	if err != nil {
		return Page[models.Message]{}, err
	}
	return res.toPage(), nil
}

// cachedDescFetcher wraps a live bucketFetcher so sealed buckets (strictly older
// than the current bucket) are served from the per-bucket cache — the whole
// bucket loaded once, then bounds-filtered in memory — while the current/hot
// bucket, oversized buckets, and (caching disabled) all buckets fall through to
// live. A bucket found oversized is marked as such in the cache, so the probe
// that discovers it is paid once per TTL rather than on every read. `before` is the DESC upper bound (applied only on the first bucket);
// `since` is the optional lower bound (floor bucket only, for BetweenDesc). These
// in-memory bounds mirror, on the cached rows, the exact predicates the live
// queryFns apply in CQL — keep the two in lockstep, or a cache hit and a cache
// miss would return different rows.
//
// A bucket served from memory has no gocql page state, so a truncated cached
// page reports bucketPage.hasMore with a nil resumeState. The walker keeps
// HasNext truthful from hasMore and anchors the cursor at the bucket rather than
// inside it, so the returned NextCursor is a bucket anchor, NOT an intra-bucket
// resume token — replaying it re-reads the bucket from its upper bound. No
// caller of the two DESC methods does: LoadHistory re-anchors by `before` and
// the surrounding-message reads consume only Page.HasNext, and
// fillMessagePageCachedDesc keeps any cursored request on the live fetcher so
// the invariant is enforced at the method. (The cursor-consuming ASC methods are
// deliberately left uncached.)
func (r *Repository) cachedDescFetcher(roomID string, before time.Time, since *time.Time, floorBucket int64, live bucketFetcher[models.Message]) bucketFetcher[models.Message] {
	if r.bucketCache == nil {
		return live
	}
	currentBucket := r.bucket.Of(r.now())
	return func(ctx context.Context, bucket int64, firstBucket bool, pageState []byte, limit int) (bucketPage[models.Message], error) {
		if bucket >= currentBucket {
			return live(ctx, bucket, firstBucket, pageState, limit)
		}
		full, res := r.bucketCache.Get(ctx, roomID, bucket)
		if res == bucketcache.Oversized {
			return live(ctx, bucket, firstBucket, pageState, limit)
		}
		if res == bucketcache.Miss {
			loaded, oversized, err := r.loadSealedBucket(ctx, roomID, bucket, r.maxCacheRows)
			if err != nil {
				return bucketPage[models.Message]{}, err
			}
			if oversized {
				r.bucketCache.PutOversized(ctx, roomID, bucket)
				return live(ctx, bucket, firstBucket, pageState, limit)
			}
			r.bucketCache.Put(ctx, roomID, bucket, loaded)
			full = loaded
		}
		var upper *time.Time
		if firstBucket {
			upper = &before
		}
		var lower *time.Time
		if since != nil && bucket == floorBucket {
			lower = since
		}
		// Bound first, decrypt second. The bounds and the limit read only
		// created_at, which stays plaintext as a clustering column, so a page of
		// 20 out of a 2000-row bucket pays for 20 decrypts rather than 2000.
		// Decrypting in place is safe despite sliceBounded returning a view:
		// `full` is always ours — a fresh per-Get decode on a hit, and on a miss
		// our own load, which Put has already encoded into the cache by now.
		rows, more := sliceBounded(full, upper, lower, limit)
		if err := r.decryptRows(ctx, rows); err != nil {
			return bucketPage[models.Message]{}, err
		}
		// resumeState stays nil — there is no gocql state for a bucket served
		// from memory — but `more` still has to reach the walker, or a page
		// capped at `limit` reads as a drained bucket and the walk terminates
		// with rows unread. See bucketPage.
		return bucketPage[models.Message]{rows: rows, hasMore: more}, nil
	}
}

// hasResumeCursor reports whether pageReq carries a non-empty pagination cursor
// (a resume) rather than an initial timestamp-anchored request.
func hasResumeCursor(pageReq PageRequest) bool {
	return pageReq.Cursor != nil && pageReq.Cursor.Encode() != ""
}

// fillMessagePageCachedDesc is fillMessagePage for the two DESC LoadHistory reads,
// with the per-bucket cache spliced in front of the live fetcher — but ONLY for
// initial, cursor-less requests. A resume cursor encodes an opaque gocql page
// state into a specific bucket; if that bucket was current when the cursor was
// minted and has since sealed, the cache can't honor that state and would repeat
// or skip rows. So a cursored request stays on the live fetcher, which resumes
// correctly regardless of sealing. LoadHistory and the surrounding-message reads
// never send a cursor here, so this makes that invariant self-enforcing at the
// method rather than dependent on every caller.
func (r *Repository) fillMessagePageCachedDesc(ctx context.Context, roomID string, before time.Time, since *time.Time, startBucket, floorBucket int64, pageReq PageRequest, initialPageState []byte, queryFn bucketQueryFn) (Page[models.Message], error) {
	live := gocqlBucketFetcher(queryFn, r.scanMessagesUpTo(ctx))
	fetch := live
	if !hasResumeCursor(pageReq) {
		fetch = r.cachedDescFetcher(roomID, before, since, floorBucket, live)
	}
	res, err := walkBuckets(
		ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, r.walkFanout, fetch,
	)
	if err != nil {
		return Page[models.Message]{}, err
	}
	return res.toPage(), nil
}

// GetMessagesBefore returns a DESC page of messages older than `before`, down to
// `floor`. With the per-bucket cache enabled the returned NextCursor is a
// bucket-boundary cursor only, NOT a valid intra-bucket resume token — callers
// paginate by re-anchoring `before` (LoadHistory) or read only Page.HasNext
// (surrounding-message reads), never by feeding NextCursor back.
func (r *Repository) GetMessagesBefore(ctx context.Context, roomID string, before time.Time, floor time.Time, pageReq PageRequest) (Page[models.Message], error) {
	floorBucket := r.bucket.Of(floor)
	startBucket, initialPageState, err := startBucketFromCursor(pageReq, walkDesc, r.bucket.Of(before), floorBucket)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages before: %w", err)
	}

	queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
		if firstBucket {
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, before,
			)
		}
		return r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC`,
			roomID, bucket,
		)
	}

	page, err := r.fillMessagePageCachedDesc(ctx, roomID, before, nil, startBucket, floorBucket, pageReq, initialPageState, queryFn)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages before: %w", err)
	}
	return page, nil
}

func (r *Repository) GetMessagesBetweenDesc(ctx context.Context, roomID string, since, before time.Time, pageReq PageRequest) (Page[models.Message], error) {
	floorBucket := r.bucket.Of(since)
	startBucket, initialPageState, err := startBucketFromCursor(pageReq, walkDesc, r.bucket.Of(before), floorBucket)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages between desc: %w", err)
	}

	queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
		atFloor := bucket == floorBucket
		switch {
		case firstBucket && atFloor:
			// Single-bucket walk: both upper (before) and lower (since) bounds apply.
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at > ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, since, before,
			)
		case firstBucket:
			// Top of walk: upper bound only.
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at < ? ORDER BY created_at DESC`,
				roomID, bucket, before,
			)
		case atFloor:
			// Bottom of walk: lower bound only — without this, rows with
			// created_at <= since in the floor bucket would leak through.
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at > ? ORDER BY created_at DESC`,
				roomID, bucket, since,
			)
		default:
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at DESC`,
				roomID, bucket,
			)
		}
	}

	page, err := r.fillMessagePageCachedDesc(ctx, roomID, before, &since, startBucket, floorBucket, pageReq, initialPageState, queryFn)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages between desc: %w", err)
	}
	return page, nil
}

func (r *Repository) GetMessagesAfter(ctx context.Context, roomID string, after time.Time, ceiling time.Time, pageReq PageRequest) (Page[models.Message], error) {
	ceilingBucket := r.bucket.Of(ceiling)
	startBucket, initialPageState, err := startBucketFromCursor(pageReq, walkAsc, r.bucket.Of(after), ceilingBucket)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages after: %w", err)
	}

	queryFn := func(bucket int64, firstBucket bool) *gocql.Query {
		if firstBucket {
			return r.session.Query(
				messageByRoomQuery+` WHERE room_id = ? AND bucket = ? AND created_at > ? ORDER BY created_at ASC`,
				roomID, bucket, after,
			)
		}
		return r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at ASC`,
			roomID, bucket,
		)
	}

	page, err := r.fillMessagePage(ctx, walkAsc, startBucket, ceilingBucket, pageReq, initialPageState, queryFn)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages after: %w", err)
	}
	return page, nil
}

func (r *Repository) GetAllMessagesAsc(ctx context.Context, roomID string, floor time.Time, ceiling time.Time, pageReq PageRequest) (Page[models.Message], error) {
	ceilingBucket := r.bucket.Of(ceiling)
	startBucket, initialPageState, err := startBucketFromCursor(pageReq, walkAsc, r.bucket.Of(floor), ceilingBucket)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get all messages asc: %w", err)
	}

	queryFn := func(bucket int64, _ bool) *gocql.Query {
		return r.session.Query(
			messageByRoomQuery+` WHERE room_id = ? AND bucket = ? ORDER BY created_at ASC`,
			roomID, bucket,
		)
	}

	page, err := r.fillMessagePage(ctx, walkAsc, startBucket, ceilingBucket, pageReq, initialPageState, queryFn)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get all messages asc: %w", err)
	}
	return page, nil
}

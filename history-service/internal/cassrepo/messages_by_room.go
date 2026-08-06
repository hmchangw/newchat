package cassrepo

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"

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

// liveMessageFetcher adapts a per-bucket queryFn into a bucketFetcher that runs
// the paged Cassandra query and scans+decrypts up to `remaining` rows. This is
// the behavior the walker had inline before the fetcher refactor.
func (r *Repository) liveMessageFetcher(queryFn bucketQueryFn) bucketFetcher[models.Message] {
	return func(ctx context.Context, bucket int64, firstBucket bool, remaining int, pageState []byte) ([]models.Message, []byte, error) {
		q := queryFn(bucket, firstBucket).WithContext(ctx).PageSize(remaining)
		if pageState != nil {
			q = q.PageState(pageState)
		}
		iter := q.Iter()
		rows, scanErr := r.scanMessagesUpTo(ctx)(iter, remaining)
		nextPageState := iter.PageState()
		if err := iter.Close(); err != nil {
			return nil, nil, fmt.Errorf("scan bucket %d: %w", bucket, err)
		}
		if scanErr != nil {
			return nil, nil, fmt.Errorf("scan bucket %d: %w", bucket, scanErr)
		}
		return rows, nextPageState, nil
	}
}

// cachedDescFetcher wraps a live fetcher so sealed buckets (strictly older than
// the current bucket) are served from the per-bucket cache — the whole bucket
// loaded once, then bounds-filtered in memory — while the current/hot bucket,
// oversized buckets, and (when caching is disabled) all buckets fall through to
// live. `before` is the DESC upper bound (applied only on the first bucket);
// `since` is the optional lower bound (applied only on the floor bucket, for
// BetweenDesc). These in-memory bounds mirror, on the cached rows, the exact
// predicates the live queryFns below apply in CQL (`created_at < before` on the
// first bucket, `created_at > since` on the floor bucket) — keep the two in
// lockstep, or a cache hit and a cache miss would return different rows.
//
// It returns a nil resume page-state even when it truncates a cached bucket to
// `remaining` rows. That makes fillPage emit a boundary NextCursor that skips
// the bucket's unconsumed rows, which is safe ONLY because no caller of the two
// DESC methods paginates via NextCursor: LoadHistory re-anchors by `before`
// timestamp, and the surrounding-message reads use only Page.HasNext. A real
// intra-bucket resume cursor (a row offset) would be required before any caller
// could resume from the returned cursor — this is why the cursor-consuming ASC
// methods are deliberately left uncached.
func (r *Repository) cachedDescFetcher(roomID string, before time.Time, since *time.Time, floorBucket int64, live bucketFetcher[models.Message]) bucketFetcher[models.Message] {
	if r.bucketCache == nil {
		return live
	}
	currentBucket := r.bucket.Of(r.now())
	return func(ctx context.Context, bucket int64, firstBucket bool, remaining int, pageState []byte) ([]models.Message, []byte, error) {
		if bucket >= currentBucket {
			return live(ctx, bucket, firstBucket, remaining, pageState)
		}
		full, ok := r.bucketCache.Get(ctx, roomID, bucket)
		if !ok {
			loaded, oversized, err := r.loadSealedBucket(ctx, roomID, bucket, r.maxCacheRows)
			if err != nil {
				return nil, nil, err
			}
			if oversized {
				return live(ctx, bucket, firstBucket, remaining, pageState)
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
		return sliceBounded(full, upper, lower, remaining), nil, nil
	}
}

// GetMessagesBefore returns a DESC page of messages older than `before`, down to
// `floor`. With the per-bucket cache enabled the returned NextCursor is a
// bucket-boundary cursor only and is NOT a valid intra-bucket resume token —
// callers paginate by re-anchoring `before` (LoadHistory) or read only
// Page.HasNext (surrounding-message reads), never by feeding NextCursor back.
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

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState,
		r.cachedDescFetcher(roomID, before, nil, floorBucket, r.liveMessageFetcher(queryFn)),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages before: %w", err)
	}
	return res.toPage(), nil
}

// GetMessagesBetweenDesc returns a DESC page of messages in (since, before). The
// same NextCursor caveat as GetMessagesBefore applies when caching is enabled:
// the cursor is a bucket boundary, not an intra-bucket resume token.
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

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState,
		r.cachedDescFetcher(roomID, before, &since, floorBucket, r.liveMessageFetcher(queryFn)),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages between desc: %w", err)
	}
	return res.toPage(), nil
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

	// Intentionally uncached (unlike the DESC reads): the ASC/LoadNextMessages
	// path round-trips a real NextCursor, so it needs an intra-bucket offset
	// resume that cachedDescFetcher's nil-page-state shortcut can't provide.
	res, err := fillPage[models.Message](
		ctx, r.bucket, walkAsc, startBucket, ceilingBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, r.liveMessageFetcher(queryFn),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages after: %w", err)
	}
	return res.toPage(), nil
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

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkAsc, startBucket, ceilingBucket, r.maxBuckets,
		// Intentionally uncached, same reason as GetMessagesAfter: the ASC path
		// round-trips a real NextCursor that the cached fetcher can't resume.
		pageReq.PageSize, initialPageState, r.liveMessageFetcher(queryFn),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get all messages asc: %w", err)
	}
	return res.toPage(), nil
}

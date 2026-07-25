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
		pageReq.PageSize, initialPageState, queryFn, r.scanMessagesUpTo(ctx),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get messages before: %w", err)
	}
	return res.toPage(), nil
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

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkDesc, startBucket, floorBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, queryFn, r.scanMessagesUpTo(ctx),
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

	res, err := fillPage[models.Message](
		ctx, r.bucket, walkAsc, startBucket, ceilingBucket, r.maxBuckets,
		pageReq.PageSize, initialPageState, queryFn, r.scanMessagesUpTo(ctx),
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
		pageReq.PageSize, initialPageState, queryFn, r.scanMessagesUpTo(ctx),
	)
	if err != nil {
		return Page[models.Message]{}, fmt.Errorf("get all messages asc: %w", err)
	}
	return res.toPage(), nil
}

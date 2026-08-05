package cassrepo

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Bucket cursor wire format (base64-encoded): [bucket: 8B BE int64][pageStateLen: 2B BE uint16][pageState: N bytes].
// Empty string decodes to (bucket=0, pageState=nil); walker substitutes its own startBucket when the cursor is absent.
const bucketCursorHeaderBytes = 8 + 2

// maxEncodedPageState is the largest pageState fitting within maxCursorBytes after the header; 502 is safely below uint16 max.
const maxEncodedPageState = maxCursorBytes - bucketCursorHeaderBytes

func encodeBucketCursor(bucket int64, pageState []byte) (string, error) {
	if len(pageState) > maxEncodedPageState {
		return "", fmt.Errorf("encode bucket cursor: pageState length %d exceeds maximum %d", len(pageState), maxEncodedPageState)
	}
	buf := make([]byte, bucketCursorHeaderBytes+len(pageState))
	// #nosec G115 -- lossless int64->uint64 bit reinterpretation for fixed-width framing; reversed in decodeBucketCursor
	binary.BigEndian.PutUint64(buf[0:8], uint64(bucket))
	// #nosec G115 -- len(pageState) is bounded <= maxEncodedPageState (502) by the guard above, well below math.MaxUint16
	binary.BigEndian.PutUint16(buf[8:10], uint16(len(pageState)))
	copy(buf[bucketCursorHeaderBytes:], pageState)
	return base64.StdEncoding.EncodeToString(buf), nil
}

func decodeBucketCursor(encoded string) (int64, []byte, error) {
	if encoded == "" {
		return 0, nil, nil
	}
	if len(encoded) > base64.StdEncoding.EncodedLen(maxCursorBytes) {
		return 0, nil, fmt.Errorf("decode bucket cursor: encoded length %d exceeds maximum", len(encoded))
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, nil, fmt.Errorf("decode bucket cursor: %w", err)
	}
	if len(raw) > maxCursorBytes {
		return 0, nil, fmt.Errorf("decode bucket cursor: decoded length %d exceeds maximum %d", len(raw), maxCursorBytes)
	}
	if len(raw) < bucketCursorHeaderBytes {
		return 0, nil, fmt.Errorf("decode bucket cursor: truncated framing (%d bytes)", len(raw))
	}
	// #nosec G115 -- inverse of the lossless uint64(bucket) framing in encodeBucketCursor; exact round-trip
	bucket := int64(binary.BigEndian.Uint64(raw[0:8]))
	psLen := int(binary.BigEndian.Uint16(raw[8:10]))
	if bucketCursorHeaderBytes+psLen != len(raw) {
		return 0, nil, fmt.Errorf("decode bucket cursor: declared pageState length %d does not match available %d", psLen, len(raw)-bucketCursorHeaderBytes)
	}
	var pageState []byte
	if psLen > 0 {
		pageState = make([]byte, psLen)
		copy(pageState, raw[bucketCursorHeaderBytes:bucketCursorHeaderBytes+psLen])
	}
	return bucket, pageState, nil
}

// walkDirection controls bucket traversal in fillPage.
type walkDirection int

const (
	walkDesc walkDirection = -1 // Prev — newest to oldest
	walkAsc  walkDirection = +1 // Next — oldest to newest
)

// pageResult is fillPage's output; NextCursor is "" when the walk has reached a terminal state.
type pageResult[T any] struct {
	Rows       []T
	NextCursor string
	HasNext    bool
}

func (r pageResult[T]) toPage() Page[T] {
	return Page[T]{Data: r.Rows, NextCursor: r.NextCursor, HasNext: r.HasNext}
}

// bucketQueryFn builds a query for the given bucket; firstBucket is true only on the first walk step,
// letting callers apply a per-call predicate (e.g. created_at < before) only where needed.
type bucketQueryFn func(bucket int64, firstBucket bool) *gocql.Query

// bucketFetcher fetches up to `remaining` rows for one bucket, honoring the
// walk's per-call bounds. firstBucket is true only on the first walk step (apply
// an upper-bound predicate only there); initialPageState resumes this bucket
// (non-nil only when draining a bucket across calls or resuming the first bucket
// from a cursor). It returns the rows plus the page state to resume this bucket
// (nil when exhausted or served whole). The live implementation runs the
// Cassandra query (see liveMessageFetcher); a cached implementation may serve a
// sealed bucket from memory (see cachedDescFetcher).
type bucketFetcher[T any] func(ctx context.Context, bucket int64, firstBucket bool, remaining int, initialPageState []byte) (rows []T, nextPageState []byte, err error)

// fillPage walks buckets in the given direction starting at startBucket,
// issuing one query per bucket and accumulating rows into out until pageSize
// is reached or maxBuckets is exhausted. The first bucket may resume from a
// caller-supplied gocql page state; later buckets always start fresh.
//
// fetch returns up to `remaining` rows for one bucket and the page state to
// resume THAT bucket (nil when the bucket is exhausted or was served whole). A
// non-nil error aborts the walk immediately — the bucket advance and any further
// fetches are skipped and the accumulated rows discarded — so fetch signals a
// fatal per-row error (e.g. a decrypt failure) up to the caller.
//
// floorBucket bounds the walk: DESC stops when bucket < floorBucket; ASC stops
// when bucket > floorBucket. To disable floor-based termination, callers pass
// math.MinInt64 (DESC) or math.MaxInt64 (ASC).
func fillPage[T any](
	ctx context.Context,
	sizer msgbucket.Sizer,
	direction walkDirection,
	startBucket int64,
	floorBucket int64,
	maxBuckets int,
	pageSize int,
	initialPageState []byte,
	fetch bucketFetcher[T],
) (pageResult[T], error) {
	out := make([]T, 0, pageSize)
	bucket := startBucket
	pageState := initialPageState
	walked := 0

	advance := func() {
		if direction == walkDesc {
			bucket = sizer.Prev(bucket)
		} else {
			bucket = sizer.Next(bucket)
		}
	}

	floorCrossed := func(b int64) bool {
		if direction == walkDesc {
			return b < floorBucket
		}
		return b > floorBucket
	}

	for len(out) < pageSize && walked < maxBuckets {
		if floorCrossed(bucket) {
			return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
		}

		// A fetch error (e.g. a per-row decrypt failure) is fatal: discard the
		// accumulated rows and abort the walk so a later bucket can't overwrite
		// the error or serve a partial page.
		rows, nextPageState, err := fetch(ctx, bucket, walked == 0, pageSize-len(out), pageState)
		if err != nil {
			return pageResult[T]{}, err
		}
		out = append(out, rows...)

		if len(nextPageState) > 0 && len(out) < pageSize {
			// More rows in this bucket but page not yet full — continue draining.
			pageState = nextPageState
			continue
		}
		if len(nextPageState) > 0 && len(out) >= pageSize {
			// Page filled mid-bucket — cursor resumes here on next call.
			cursor, encErr := encodeBucketCursor(bucket, nextPageState)
			if encErr != nil {
				return pageResult[T]{}, fmt.Errorf("encode resume cursor at bucket %d: %w", bucket, encErr)
			}
			return pageResult[T]{
				Rows:       out,
				NextCursor: cursor,
				HasNext:    true,
			}, nil
		}

		pageState = nil
		advance()
		walked++
	}

	if floorCrossed(bucket) {
		return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
	}
	// maxBuckets or pageSize reached at a bucket boundary — cursor points to the next bucket.
	cursor, encErr := encodeBucketCursor(bucket, nil)
	if encErr != nil {
		return pageResult[T]{}, fmt.Errorf("encode resume cursor at bucket %d: %w", bucket, encErr)
	}
	return pageResult[T]{
		Rows:       out,
		NextCursor: cursor,
		HasNext:    true,
	}, nil
}

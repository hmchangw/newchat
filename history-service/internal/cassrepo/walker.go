package cassrepo

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

// Bucket cursor wire format (then base64-encoded for transport):
//
//	[bucket: 8 bytes BE int64][pageStateLen: 2 bytes BE uint16][pageState: N bytes]
//
// Empty input string decodes to (bucket=0, pageState=nil), interpreted by the
// walker as "start from caller-supplied startBucket with a fresh in-bucket
// query". The bucket=0 sentinel is unambiguous because the walker passes its
// own startBucket when the cursor is empty.
const bucketCursorHeaderBytes = 8 + 2

// maxEncodedPageState is the largest pageState that fits inside maxCursorBytes
// once the framing header is accounted for. It also doubles as the math.MaxUint16
// safety bound: maxCursorBytes (512) - bucketCursorHeaderBytes (10) = 502, well
// below 65535, so the uint16 length field is sufficient.
const maxEncodedPageState = maxCursorBytes - bucketCursorHeaderBytes

func encodeBucketCursor(bucket int64, pageState []byte) (string, error) {
	if len(pageState) > maxEncodedPageState {
		return "", fmt.Errorf("encode bucket cursor: pageState length %d exceeds maximum %d", len(pageState), maxEncodedPageState)
	}
	buf := make([]byte, bucketCursorHeaderBytes+len(pageState))
	binary.BigEndian.PutUint64(buf[0:8], uint64(bucket))
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

// pageResult is fillPage's output. NextCursor is the empty string when the walk
// has reached a terminal state (floor/ceiling crossed, or both page filled and
// no more rows in current bucket).
type pageResult[T any] struct {
	Rows       []T
	NextCursor string
	HasNext    bool
}

// toPage projects the walker's pageResult into the cassrepo public Page[T] type.
func (r pageResult[T]) toPage() Page[T] {
	return Page[T]{Data: r.Rows, NextCursor: r.NextCursor, HasNext: r.HasNext}
}

// bucketQueryFn returns a freshly-prepared gocql.Query bound to the given
// bucket value. Implementations are produced by each public read function
// (e.g. GetMessagesBefore creates a factory that interpolates bucket and
// the per-call predicate into the SELECT statement).
//
// firstBucket is true on the first invocation only; the factory may use this
// to apply a per-call predicate (e.g. created_at < before) only on the first
// bucket walked. Later buckets are entirely on one side of the boundary and
// do not need the predicate.
type bucketQueryFn func(bucket int64, firstBucket bool) *gocql.Query

// fillPage walks buckets in the given direction starting at startBucket,
// issuing one query per bucket and accumulating rows into out until pageSize
// is reached or maxBuckets is exhausted. The first bucket may resume from a
// caller-supplied gocql page state; later buckets always start fresh.
//
// scan must consume up to `remaining` rows from iter and return them; it is
// responsible for stopping when full.
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
	queryFn bucketQueryFn,
	scan func(iter *gocql.Iter, remaining int) []T,
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

		q := queryFn(bucket, walked == 0).WithContext(ctx)
		q = q.PageSize(pageSize - len(out))
		if pageState != nil {
			q = q.PageState(pageState)
		}

		iter := q.Iter()
		rows := scan(iter, pageSize-len(out))
		out = append(out, rows...)
		nextPageState := iter.PageState()
		if err := iter.Close(); err != nil {
			return pageResult[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, err)
		}

		if len(nextPageState) > 0 && len(out) < pageSize {
			// Bucket has more rows but page wasn't filled yet — continue draining same bucket.
			pageState = nextPageState
			continue
		}
		if len(nextPageState) > 0 && len(out) >= pageSize {
			// Page filled mid-bucket — return cursor pointing at this bucket so caller resumes here.
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

		// Bucket exhausted; advance.
		pageState = nil
		advance()
		walked++
	}

	if floorCrossed(bucket) {
		return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
	}
	// maxBuckets reached or pageSize reached at bucket boundary — non-terminal cursor at next bucket.
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

package cassrepo

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/gocql/gocql"
	"golang.org/x/sync/errgroup"

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

// walkConfig bounds a bucket walk. concurrency<=1 (or escalateAfter<=0) keeps
// the walk strictly serial — byte-identical to the original one-bucket-per-RTT
// behavior. With concurrency>1 the walk escalates to a parallel empty-bucket
// skip after escalateAfter consecutive empty buckets (see walkBuckets).
type walkConfig struct {
	maxBuckets    int // hard cap on buckets traversed before returning a non-terminal cursor
	concurrency   int // max buckets probed concurrently once escalated; <=1 disables parallelism
	escalateAfter int // consecutive empty buckets that trigger the parallel skip; <=0 disables
}

// bucketResult is one bucket's contribution to a page. nextPageState is non-nil
// iff the bucket was truncated at the requested limit with rows still remaining
// — the signal that the page filled mid-bucket and must resume here.
type bucketResult[T any] struct {
	rows          []T
	nextPageState []byte
}

// bucketFetcher reads up to `limit` rows from a single bucket, resuming from
// pageState (nil = start of bucket). It fully drains the bucket up to `limit`,
// so a non-nil nextPageState means "limit reached, more rows remain here".
// firstBucket lets the gocql adapter apply the first-step predicate. A non-nil
// error aborts the whole walk.
type bucketFetcher[T any] func(ctx context.Context, bucket int64, firstBucket bool, pageState []byte, limit int) (bucketResult[T], error)

// walkBuckets walks buckets in the given direction from startBucket, calling
// fetch once per bucket and accumulating rows until pageSize is reached, the
// floor is crossed, or maxBuckets is exhausted.
//
// Serial mode reproduces the original one-bucket-at-a-time walk exactly. Once
// cfg.escalateAfter consecutive empty buckets are seen (and cfg.concurrency>1),
// it escalates: it probes up to cfg.concurrency buckets CONCURRENTLY to skip
// runs of empty buckets quickly. Crucially, the parallel path only ever commits
// to skipping EMPTY buckets — the first bucket that yields rows is handed back
// to the serial path, so the page contents and resume cursor always come from a
// single exact-limit read. This keeps parallel output byte-identical to serial
// (proven by the differential test) while collapsing N sequential RTTs over
// sparse history into ~N/concurrency rounds.
//
// floorBucket bounds the walk: DESC stops when bucket < floorBucket; ASC stops
// when bucket > floorBucket. To disable floor-based termination, callers pass
// math.MinInt64 (DESC) or math.MaxInt64 (ASC).
func walkBuckets[T any](
	ctx context.Context,
	sizer msgbucket.Sizer,
	direction walkDirection,
	startBucket int64,
	floorBucket int64,
	cfg walkConfig,
	pageSize int,
	initialPageState []byte,
	fetch bucketFetcher[T],
) (pageResult[T], error) {
	out := make([]T, 0, pageSize)
	bucket := startBucket
	pageState := initialPageState
	walked := 0
	emptyRun := 0
	parallel := false

	step := func(b int64) int64 {
		if direction == walkDesc {
			return sizer.Prev(b)
		}
		return sizer.Next(b)
	}
	floorCrossed := func(b int64) bool {
		if direction == walkDesc {
			return b < floorBucket
		}
		return b > floorBucket
	}
	parallelEnabled := cfg.concurrency > 1 && cfg.escalateAfter > 0

	for len(out) < pageSize && walked < cfg.maxBuckets {
		if floorCrossed(bucket) {
			return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
		}

		if parallel {
			// Build a batch of buckets to probe concurrently, bounded by the
			// remaining bucket budget and the floor.
			batch := make([]int64, 0, cfg.concurrency)
			for b := bucket; len(batch) < cfg.concurrency && walked+len(batch) < cfg.maxBuckets && !floorCrossed(b); b = step(b) {
				batch = append(batch, b)
			}
			if len(batch) == 0 {
				break // floor or maxBuckets reached at a bucket boundary
			}

			results := make([]bucketResult[T], len(batch))
			g, gctx := errgroup.WithContext(ctx)
			for i := range batch {
				g.Go(func() error {
					// firstBucket is always false here: escalation needs >=1 prior
					// empty bucket, so walked>0 and this is never the first step.
					res, err := fetch(gctx, batch[i], false, nil, pageSize)
					if err != nil {
						return err
					}
					results[i] = res
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return pageResult[T]{}, err
			}

			firstData := -1
			for i := range results {
				if len(results[i].rows) > 0 {
					firstData = i
					break
				}
			}
			if firstData < 0 {
				// Whole batch empty: commit to skipping all of it, stay parallel.
				for range batch {
					bucket = step(bucket)
				}
				walked += len(batch)
				continue
			}
			// Skip the empties before the data bucket; hand the data bucket back
			// to the serial path (the speculatively-read rows beyond it are
			// discarded — they get re-walked exactly once with precise limits).
			for j := 0; j < firstData; j++ {
				bucket = step(bucket)
			}
			walked += firstData
			parallel = false
			emptyRun = 0
			pageState = nil
			continue
		}

		// Serial step.
		res, err := fetch(ctx, bucket, walked == 0, pageState, pageSize-len(out))
		if err != nil {
			return pageResult[T]{}, err
		}
		out = append(out, res.rows...)
		if res.nextPageState != nil {
			// Page filled mid-bucket — cursor resumes here on next call.
			cursor, encErr := encodeBucketCursor(bucket, res.nextPageState)
			if encErr != nil {
				return pageResult[T]{}, fmt.Errorf("encode resume cursor at bucket %d: %w", bucket, encErr)
			}
			return pageResult[T]{Rows: out, NextCursor: cursor, HasNext: true}, nil
		}

		if len(res.rows) == 0 {
			emptyRun++
		} else {
			emptyRun = 0
		}
		pageState = nil
		bucket = step(bucket)
		walked++
		if parallelEnabled && emptyRun >= cfg.escalateAfter {
			parallel = true
		}
	}

	if floorCrossed(bucket) {
		return pageResult[T]{Rows: out, NextCursor: "", HasNext: false}, nil
	}
	// maxBuckets or pageSize reached at a bucket boundary — cursor points to the next bucket.
	cursor, encErr := encodeBucketCursor(bucket, nil)
	if encErr != nil {
		return pageResult[T]{}, fmt.Errorf("encode resume cursor at bucket %d: %w", bucket, encErr)
	}
	return pageResult[T]{Rows: out, NextCursor: cursor, HasNext: true}, nil
}

// fillPage adapts a gocql query/scan pair to walkBuckets: it builds a
// bucketFetcher that drains one bucket up to `limit` rows across gocql pages,
// then delegates the bucket traversal (serial or escalating-parallel per cfg)
// to walkBuckets. The fetcher preserves the original semantics — a fatal scan
// error (e.g. a per-row decrypt failure) aborts the whole walk.
func fillPage[T any](
	ctx context.Context,
	sizer msgbucket.Sizer,
	direction walkDirection,
	startBucket int64,
	floorBucket int64,
	cfg walkConfig,
	pageSize int,
	initialPageState []byte,
	queryFn bucketQueryFn,
	scan func(iter *gocql.Iter, remaining int) ([]T, error),
) (pageResult[T], error) {
	fetch := func(fctx context.Context, bucket int64, firstBucket bool, pageState []byte, limit int) (bucketResult[T], error) {
		rows := make([]T, 0, limit)
		ps := pageState
		for len(rows) < limit {
			q := queryFn(bucket, firstBucket).WithContext(fctx).PageSize(limit - len(rows))
			if ps != nil {
				q = q.PageState(ps)
			}
			iter := q.Iter()
			got, scanErr := scan(iter, limit-len(rows))
			rows = append(rows, got...)
			nextPageState := iter.PageState()
			if err := iter.Close(); err != nil {
				return bucketResult[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, err)
			}
			if scanErr != nil {
				return bucketResult[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, scanErr)
			}
			if len(nextPageState) > 0 && len(rows) < limit {
				// More rows in this bucket but limit not yet reached — keep draining.
				ps = nextPageState
				continue
			}
			if len(nextPageState) > 0 {
				// Limit reached mid-bucket — signal a resume point.
				return bucketResult[T]{rows: rows, nextPageState: nextPageState}, nil
			}
			// Bucket exhausted.
			return bucketResult[T]{rows: rows, nextPageState: nil}, nil
		}
		return bucketResult[T]{rows: rows, nextPageState: nil}, nil
	}

	return walkBuckets[T](ctx, sizer, direction, startBucket, floorBucket, cfg, pageSize, initialPageState, fetch)
}

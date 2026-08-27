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

// bucketPage is one bucket's fetched rows plus the gocql page state needed to
// resume mid-bucket. resumeState is non-empty only when the bucket holds more
// rows than were returned, so the walk can distinguish "bucket drained" from
// "page capped by limit" unambiguously.
type bucketPage[T any] struct {
	rows        []T
	resumeState []byte
}

// bucketFetcher fetches up to limit fully-materialized rows from a single
// bucket. firstBucket selects the caller's first-bucket predicate; pageState
// resumes a partially consumed bucket. It MUST honor limit exactly — returning
// min(limit, rowsInBucket) rows and draining short driver pages internally — so
// that a non-empty resumeState unambiguously means "more rows remain here".
type bucketFetcher[T any] func(ctx context.Context, bucket int64, firstBucket bool, pageState []byte, limit int) (bucketPage[T], error)

// gocqlBucketFetcher adapts a bucketQueryFn + scan into a bucketFetcher backed
// by the live session. It drains short driver pages until limit rows are
// collected or the bucket is exhausted, preserving the original within-bucket
// drain semantics.
//
// scan must consume up to `remaining` rows from iter and return them; a non-nil
// error aborts the fetch (and, upstream, the whole walk). This is how scan
// signals a fatal per-row error (e.g. a decrypt failure).
func gocqlBucketFetcher[T any](
	queryFn bucketQueryFn,
	scan func(iter *gocql.Iter, remaining int) ([]T, error),
) bucketFetcher[T] {
	return func(ctx context.Context, bucket int64, firstBucket bool, pageState []byte, limit int) (bucketPage[T], error) {
		rows := make([]T, 0, limit)
		state := pageState
		for len(rows) < limit {
			q := queryFn(bucket, firstBucket).WithContext(ctx).PageSize(limit - len(rows))
			if state != nil {
				q = q.PageState(state)
			}
			iter := q.Iter()
			got, scanErr := scan(iter, limit-len(rows))
			rows = append(rows, got...)
			state = iter.PageState()
			if closeErr := iter.Close(); closeErr != nil {
				return bucketPage[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, closeErr)
			}
			if scanErr != nil {
				return bucketPage[T]{}, fmt.Errorf("scan bucket %d: %w", bucket, scanErr)
			}
			if len(state) == 0 {
				break // bucket fully drained
			}
		}
		return bucketPage[T]{rows: rows, resumeState: state}, nil
	}
}

// bucketWalk holds the immutable parameters of one paginated walk over a
// bucketed table. Its run method fills a single page by fetching buckets
// concurrently in fan-out waves:
//
//   - Wave 1 probes only the start bucket, so a dense start region fills the
//     page in one query with no speculative reads (the same cost the old serial
//     walk paid for busy rooms).
//   - Once the start bucket underfills, later waves fetch several buckets at once,
//     collapsing a long run of sparse/empty buckets from many serial round-trips
//     into a few concurrent ones. Each wave's width adapts to the rows-per-bucket
//     density seen so far (see adaptiveWaveWidth): sparse/empty runs widen toward
//     fanout to skip fast, dense runs narrow toward 1 to avoid over-reading fat
//     buckets for a small shortfall.
//
// Rows are always assembled in strict bucket order and the returned cursor is
// identical to what a serial walk would produce — concurrency overlaps the I/O
// only, never the ordering or pagination.
//
// The walk is contiguous by design: it visits every bucket between its start and
// its floor, and never jumps over a run it believes is empty. That is worth
// stating because the belief is tempting and unavailable. The obvious source for
// it, rooms.lastMsgAt, is not a watermark for this table: unread-worker projects
// it from MESSAGES-CANONICAL on a consumer separate from the message-worker that
// writes these rows, with MaxDeliver=-1 holding batches un-acked through a Mongo
// outage. The pointer therefore lags Cassandra by an unbounded amount by design
// (docs/load-testing/failure/mongodb.md), so no timestamp taken alongside it can
// bound how far it lags, and any bucket it authorizes skipping may hold a row.
// Only a watermark advanced by Cassandra persistence itself could, and none
// exists. Widen the floor or the fan-out instead.
type bucketWalk[T any] struct {
	sizer        msgbucket.Sizer
	direction    walkDirection
	startBucket  int64
	floorBucket  int64
	maxBuckets   int
	pageSize     int
	initialState []byte
	fanout       int
	fetch        bucketFetcher[T]
}

// walkBuckets fills a page starting at startBucket. See bucketWalk for the
// traversal strategy. A fetch error aborts the walk and is returned to the
// caller (accumulated rows are discarded).
//
// floorBucket bounds the walk: DESC stops when bucket < floorBucket; ASC stops
// when bucket > floorBucket. To disable floor-based termination, callers pass
// math.MinInt64 (DESC) or math.MaxInt64 (ASC). fanout caps how many buckets are
// fetched concurrently once the walk fans out.
func walkBuckets[T any](
	ctx context.Context,
	sizer msgbucket.Sizer,
	direction walkDirection,
	startBucket int64,
	floorBucket int64,
	maxBuckets int,
	pageSize int,
	initialPageState []byte,
	fanout int,
	fetch bucketFetcher[T],
) (pageResult[T], error) {
	if fanout < 1 {
		fanout = 1
	}
	w := &bucketWalk[T]{
		sizer:        sizer,
		direction:    direction,
		startBucket:  startBucket,
		floorBucket:  floorBucket,
		maxBuckets:   maxBuckets,
		pageSize:     pageSize,
		initialState: initialPageState,
		fanout:       fanout,
		fetch:        fetch,
	}
	return w.run(ctx)
}

// run drives the wave loop: check for termination, plan a wave of buckets, fetch
// them concurrently, then assemble their rows in order until the page fills.
func (w *bucketWalk[T]) run(ctx context.Context) (pageResult[T], error) {
	out := make([]T, 0, w.pageSize)
	curBucket := w.startBucket
	walked := 0
	waveWidth := 1 // wave 1 probes only the start bucket; widen adaptively after it underfills

	for {
		if w.crossedFloor(curBucket) {
			return terminalPage(out), nil
		}
		if walked >= w.maxBuckets {
			return resumeAtBucket(out, curBucket, nil)
		}

		buckets := w.planWave(curBucket, waveWidth, walked)
		if len(buckets) == 0 {
			// The floor or the maxBuckets budget left no room to fetch.
			return resumeAtBucket(out, curBucket, nil)
		}
		pages, err := w.fetchWave(ctx, buckets)
		if err != nil {
			return pageResult[T]{}, err
		}

		// Assemble in strict bucket order, stopping as soon as the page fills.
		for i, bk := range buckets {
			walked++
			page := pages[i]
			taken := min(w.pageSize-len(out), len(page.rows))
			out = append(out, page.rows[:taken]...)
			if len(out) == w.pageSize {
				return w.resumeAfterFill(ctx, out, bk, page, taken)
			}
			// Contract: fewer than pageSize rows ⇒ bucket drained, safe to advance.
		}

		// Whole wave consumed, page still not full: size the next wave to the
		// density seen so far (len(out) rows over walked buckets), then continue.
		curBucket = w.advance(buckets[len(buckets)-1])
		waveWidth = adaptiveWaveWidth(len(out), walked, w.pageSize-len(out), w.fanout)
	}
}

// adaptiveWaveWidth sizes the next fan-out wave to how many buckets the walk
// still expects to need: the rows still needed divided by the rows-per-bucket
// density observed so far (empties count as zero rows), clamped to [1, fanout].
//
// This makes the walk self-tune to the data it's crossing: an empty/sparse run
// (density → 0) widens toward fanout to skip fast, where over-read is cheap;
// a dense region (buckets near a full page) narrows toward 1, so a small
// shortfall doesn't speculatively fetch a whole fanout of fat buckets to use
// one. Width only ever affects performance — the assembled page and cursor are
// identical for any width (see the fan-out parity test).
func adaptiveWaveWidth(rowsSeen, bucketsSeen, needed, fanout int) int {
	density := rowsSeen
	if density < 1 {
		density = 1 // all-empty so far: treat as <1 row/bucket to widen fully
	}
	// ceil(needed * bucketsSeen / rowsSeen) = ceil(needed / avg-rows-per-bucket).
	width := (needed*bucketsSeen + density - 1) / density
	if width < 1 {
		width = 1 // nothing observed yet (bucketsSeen==0) or none needed: at least one bucket
	}
	if width > fanout {
		width = fanout
	}
	return width
}

// planWave lists the buckets to fetch next, starting at from and advancing,
// bounded by width, the floor, and the remaining maxBuckets budget (walked so far).
func (w *bucketWalk[T]) planWave(from int64, width, walked int) []int64 {
	buckets := make([]int64, 0, width)
	for b := from; len(buckets) < width && !w.crossedFloor(b) && walked+len(buckets) < w.maxBuckets; b = w.advance(b) {
		buckets = append(buckets, b)
	}
	return buckets
}

// fetchWave fetches every bucket in the wave concurrently (bounded by fanout) and
// returns their pages in bucket order. Any fetch error aborts the wave.
func (w *bucketWalk[T]) fetchWave(ctx context.Context, buckets []int64) ([]bucketPage[T], error) {
	pages := make([]bucketPage[T], len(buckets))
	// Fast path for a single-bucket wave (wave 1, and every dense/narrow wave):
	// there is no concurrency to orchestrate, so fetch inline and skip the
	// errgroup/goroutine allocation and join. This is the dominant traffic.
	if len(buckets) == 1 {
		p, err := w.fetch(ctx, buckets[0], buckets[0] == w.startBucket, w.baseState(buckets[0]), w.pageSize)
		if err != nil {
			return nil, err
		}
		pages[0] = p
		return pages, nil
	}
	// planWave/adaptiveWaveWidth already bound len(buckets) <= fanout, so the
	// group needs no SetLimit.
	g, gctx := errgroup.WithContext(ctx)
	for i, bk := range buckets {
		g.Go(func() error {
			p, err := w.fetch(gctx, bk, bk == w.startBucket, w.baseState(bk), w.pageSize)
			if err != nil {
				return err
			}
			pages[i] = p
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return pages, nil
}

// resumeAfterFill builds the cursor once the page has just filled at bucket,
// having consumed taken of that bucket's prefetched page.rows. Three cases:
//
//   - taken < len(page.rows): the page filled before all prefetched rows were
//     consumed, so re-fetch exactly taken rows to get a page state aligned to the
//     consumed count (rather than the speculatively-prefetched count).
//   - the bucket holds more rows than were prefetched: resume within the bucket.
//   - the bucket drained exactly at the page boundary: resume at the next bucket,
//     or terminate if that crosses the floor.
func (w *bucketWalk[T]) resumeAfterFill(ctx context.Context, rows []T, bucket int64, page bucketPage[T], taken int) (pageResult[T], error) {
	if taken < len(page.rows) {
		aligned, err := w.fetch(ctx, bucket, bucket == w.startBucket, w.baseState(bucket), taken)
		if err != nil {
			return pageResult[T]{}, err
		}
		return resumeAtBucket(rows, bucket, aligned.resumeState)
	}
	if len(page.resumeState) > 0 {
		return resumeAtBucket(rows, bucket, page.resumeState)
	}
	next := w.advance(bucket)
	if w.crossedFloor(next) {
		return terminalPage(rows), nil
	}
	return resumeAtBucket(rows, next, nil)
}

// advance steps one bucket in the walk direction: older for DESC, newer for ASC.
func (w *bucketWalk[T]) advance(b int64) int64 {
	if w.direction == walkDesc {
		return w.sizer.Prev(b)
	}
	return w.sizer.Next(b)
}

// crossedFloor reports whether b has passed the walk's boundary: below floorBucket
// for DESC, above it for ASC.
func (w *bucketWalk[T]) crossedFloor(b int64) bool {
	if w.direction == walkDesc {
		return b < w.floorBucket
	}
	return b > w.floorBucket
}

// baseState is the page state a bucket is fetched with: only the start bucket
// carries the caller's initial (resume) state; every other bucket starts fresh.
func (w *bucketWalk[T]) baseState(bucket int64) []byte {
	if bucket == w.startBucket {
		return w.initialState
	}
	return nil
}

// resumeAtBucket returns a non-terminal page whose cursor resumes at bucket/state.
func resumeAtBucket[T any](rows []T, bucket int64, state []byte) (pageResult[T], error) {
	cursor, err := encodeBucketCursor(bucket, state)
	if err != nil {
		return pageResult[T]{}, fmt.Errorf("encode resume cursor at bucket %d: %w", bucket, err)
	}
	return pageResult[T]{Rows: rows, NextCursor: cursor, HasNext: true}, nil
}

// terminalPage returns a final page: no cursor, no more pages.
func terminalPage[T any](rows []T) pageResult[T] {
	return pageResult[T]{Rows: rows, NextCursor: "", HasNext: false}
}

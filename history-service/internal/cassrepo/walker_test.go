package cassrepo

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestBucketCursor_RoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		bucket    int64
		pageState []byte
	}{
		{name: "empty page state", bucket: 86_400_000, pageState: nil},
		{name: "small page state", bucket: 0, pageState: []byte{0x01, 0x02, 0x03}},
		{name: "negative bucket allowed (pre-epoch test data)", bucket: -86_400_000, pageState: []byte{0xff}},
		{name: "long page state", bucket: 1_700_000_000_000, pageState: make([]byte, 200)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeBucketCursor(tc.bucket, tc.pageState)
			require.NoError(t, err)
			require.NotEmpty(t, encoded, "encoded cursor must not be empty")

			gotBucket, gotPageState, err := decodeBucketCursor(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.bucket, gotBucket)
			assert.Equal(t, tc.pageState, gotPageState)
		})
	}
}

func TestBucketCursor_EmptyEncoded_IsFreshWalk(t *testing.T) {
	bucket, pageState, err := decodeBucketCursor("")
	require.NoError(t, err)
	assert.Equal(t, int64(0), bucket)
	assert.Nil(t, pageState)
}

func TestBucketCursor_EncodeRejectsOversize(t *testing.T) {
	big := make([]byte, maxEncodedPageState+1)
	_, err := encodeBucketCursor(0, big)
	require.Error(t, err, "encode must refuse pageState that won't fit in maxCursorBytes")
}

func TestBucketCursor_RejectsCorruptBase64(t *testing.T) {
	_, _, err := decodeBucketCursor("not-valid-base64!@#")
	require.Error(t, err)
}

func TestBucketCursor_RejectsTruncatedFraming(t *testing.T) {
	// Valid base64 but only a few bytes (< 8-byte bucket header).
	encoded, err := encodeBucketCursor(0, nil)
	require.NoError(t, err)
	_, _, err = decodeBucketCursor(encoded[:6])
	require.Error(t, err)
}

// The tests below exercise walkBuckets — the direction-agnostic concurrent
// bucket walk — through a fake bucketFetcher that honors the fetcher contract
// (returns min(limit, rowsInBucket) rows; resumeState non-empty iff more rows
// remain). The gocql-backed fetcher's short-page drain loop is covered by the
// messages_by_room integration tests against a real cluster.

func testSizer() msgbucket.Sizer { return msgbucket.New(24 * time.Hour) }

// testStartBucket is an arbitrary but stable bucket to anchor a walk on.
func testStartBucket() int64 { return testSizer().Of(time.Unix(1_700_000_000, 0).UTC()) }

func encodeTestOffset(n int) []byte {
	b := make([]byte, 4)
	// #nosec G115 -- test offsets are small non-negative ints
	binary.BigEndian.PutUint32(b, uint32(n))
	return b
}

func decodeTestOffset(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	return int(binary.BigEndian.Uint32(b))
}

func seq(from, to int) []int {
	out := make([]int, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

type fetchCall struct {
	bucket    int64
	first     bool
	pageState []byte
}

// fakeCassandra is an in-memory bucket store implementing the fetcher contract.
type fakeCassandra struct {
	mu      sync.Mutex
	buckets map[int64][]int
	calls   []fetchCall

	failOn *int64 // bucket to fail on, nil for never

	// concurrency instrumentation / barrier
	inFlight    int
	maxInFlight int
	barrierAt   int           // once this many fetches are concurrently in-flight, release the gate
	gate        chan struct{} // closed when barrierAt reached; nil disables the barrier
}

func (f *fakeCassandra) fetch(ctx context.Context, bucket int64, first bool, pageState []byte, limit int) (bucketPage[int], error) {
	f.mu.Lock()
	f.calls = append(f.calls, fetchCall{bucket: bucket, first: first, pageState: append([]byte(nil), pageState...)})
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	// Only fan-out (non-first) fetches participate in the barrier; the lone
	// wave-1 start-bucket fetch (first=true) must never block on it.
	participate := f.gate != nil && !first
	reachedBarrier := participate && f.inFlight >= f.barrierAt
	gate := f.gate
	fail := f.failOn != nil && *f.failOn == bucket
	f.mu.Unlock()

	if participate {
		if reachedBarrier {
			// Release everyone waiting once enough goroutines are concurrently here.
			select {
			case <-gate:
			default:
				close(gate)
			}
		}
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}

	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if fail {
		return bucketPage[int]{}, errors.New("boom")
	}

	rows := f.buckets[bucket]
	offset := decodeTestOffset(pageState)
	if offset > len(rows) {
		offset = len(rows)
	}
	avail := rows[offset:]
	take := limit
	if take > len(avail) {
		take = len(avail)
	}
	got := append([]int(nil), avail[:take]...)
	var resume []byte
	if offset+take < len(rows) {
		resume = encodeTestOffset(offset + take)
	}
	return bucketPage[int]{rows: got, resumeState: resume}, nil
}

// fetchedBuckets returns how many times each bucket was fetched.
func (f *fakeCassandra) fetchedBuckets() map[int64]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[int64]int{}
	for _, c := range f.calls {
		out[c.bucket]++
	}
	return out
}

const noFloorDesc = math.MinInt64

func runDesc(t *testing.T, store *fakeCassandra, start, floor int64, maxBuckets, pageSize, fanout int, initial []byte) pageResult[int] {
	t.Helper()
	res, err := walkBuckets[int](context.Background(), testSizer(), walkDesc, start, floor, maxBuckets, pageSize, initial, fanout, store.fetch)
	require.NoError(t, err)
	return res
}

func TestWalkBuckets_SingleDenseBucketFillsPage_NoFanout(t *testing.T) {
	start := testStartBucket()
	store := &fakeCassandra{buckets: map[int64][]int{start: seq(1, 30)}}

	res := runDesc(t, store, start, noFloorDesc, 365, 20, 8, nil)

	assert.Equal(t, seq(1, 20), res.Rows)
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, start, b, "resume stays in the same bucket")
	assert.Equal(t, 20, decodeTestOffset(ps))

	// A dense start region must NOT trigger speculative fan-out.
	assert.Equal(t, map[int64]int{start: 1}, store.fetchedBuckets())
	assert.Equal(t, 1, store.maxInFlight)
}

func TestWalkBuckets_SingleBucketExactDrain_CursorAtNextBucket(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	store := &fakeCassandra{buckets: map[int64][]int{start: seq(1, 20)}}

	res := runDesc(t, store, start, noFloorDesc, 365, 20, 8, nil)

	assert.Equal(t, seq(1, 20), res.Rows)
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, s.Prev(start), b, "fully drained bucket resumes at the next bucket")
	assert.Empty(t, ps)
}

func TestWalkBuckets_SparseFillAcrossBuckets_Ordered(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	b1, b2 := s.Prev(start), s.Prev(s.Prev(start))
	b3, b4 := s.Prev(b2), s.Prev(s.Prev(b2))
	store := &fakeCassandra{buckets: map[int64][]int{
		start: {1}, b1: {2}, b2: {3}, b3: {4}, b4: {5},
	}}

	res := runDesc(t, store, start, noFloorDesc, 365, 5, 8, nil)

	assert.Equal(t, []int{1, 2, 3, 4, 5}, res.Rows, "rows must be in descending bucket order")
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, s.Prev(b4), b)
	assert.Empty(t, ps)

	// No bucket other than the start bucket may receive a page state or the
	// firstBucket flag on a fresh walk.
	for _, c := range store.calls {
		if c.bucket != start {
			assert.Empty(t, c.pageState, "non-start bucket %d unexpectedly got a page state", c.bucket)
			assert.False(t, c.first, "non-start bucket %d flagged firstBucket", c.bucket)
		}
	}
}

func TestWalkBuckets_MidBucketBoundary_RequeryAlignsCursor(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	prev := s.Prev(start)
	store := &fakeCassandra{buckets: map[int64][]int{
		start: {10, 11, 12},
		prev:  {20, 21, 22, 23, 24, 25, 26, 27, 28, 29},
	}}

	res := runDesc(t, store, start, noFloorDesc, 365, 5, 8, nil)

	assert.Equal(t, []int{10, 11, 12, 20, 21}, res.Rows)
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, prev, b, "page filled mid-bucket resumes in that bucket")
	assert.Equal(t, 2, decodeTestOffset(ps), "cursor must align to the consumed rows, not the prefetched rows")

	// The boundary bucket is fetched twice: once speculatively (limit=pageSize),
	// once re-queried with the exact consumed count to obtain the aligned state.
	assert.Equal(t, 2, store.fetchedBuckets()[prev])
}

func TestWalkBuckets_FloorTerminationReturnsPartialPage(t *testing.T) {
	start := testStartBucket()
	// floor == start ⇒ DESC crosses the floor at Prev(start), so the walk stops
	// after the start bucket.
	store := &fakeCassandra{buckets: map[int64][]int{start: {1, 2}}}

	res := runDesc(t, store, start, start, 365, 5, 8, nil)

	assert.Equal(t, []int{1, 2}, res.Rows)
	assert.False(t, res.HasNext)
	assert.Empty(t, res.NextCursor)
	assert.Equal(t, map[int64]int{start: 1}, store.fetchedBuckets(), "must not fetch below the floor")
}

func TestWalkBuckets_MaxBucketsCapReturnsResumeCursor(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	store := &fakeCassandra{buckets: map[int64][]int{}} // all empty

	res := runDesc(t, store, start, noFloorDesc, 2, 5, 8, nil)

	assert.Empty(t, res.Rows)
	assert.True(t, res.HasNext, "cap reached with an unfilled page is non-terminal")
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, s.Prev(s.Prev(start)), b, "resume points just past the last walked bucket")
	assert.Empty(t, ps)
	assert.Len(t, store.calls, 2, "must fetch no more than maxBuckets buckets")
}

func TestWalkBuckets_ResumeFromInitialPageState(t *testing.T) {
	start := testStartBucket()
	store := &fakeCassandra{buckets: map[int64][]int{start: seq(1, 10)}}

	res := runDesc(t, store, start, noFloorDesc, 365, 3, 8, encodeTestOffset(5))

	assert.Equal(t, []int{6, 7, 8}, res.Rows, "walk must resume after the initial page state")
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, start, b)
	assert.Equal(t, 8, decodeTestOffset(ps))

	require.NotEmpty(t, store.calls)
	assert.Equal(t, 5, decodeTestOffset(store.calls[0].pageState), "start bucket must receive the initial page state")
	assert.True(t, store.calls[0].first)
}

func TestWalkBuckets_EmptyWalkBelowFloorIsTerminal(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	store := &fakeCassandra{buckets: map[int64][]int{}}

	// floor == Prev(start): crosses at Prev(Prev(start)).
	res := runDesc(t, store, start, s.Prev(start), 365, 5, 8, nil)

	assert.Empty(t, res.Rows)
	assert.False(t, res.HasNext)
	assert.Empty(t, res.NextCursor)
}

func TestWalkBuckets_FetcherErrorPropagates(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	failBucket := s.Prev(start)
	store := &fakeCassandra{
		buckets: map[int64][]int{start: {1}}, // start underfills, forcing a fan-out into the failing bucket
		failOn:  &failBucket,
	}

	_, err := walkBuckets[int](context.Background(), s, walkDesc, start, noFloorDesc, 365, 5, nil, 8, store.fetch)
	require.Error(t, err)
}

func TestWalkBuckets_AscendingDirectionOrdersAndResumes(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	n1, n2 := s.Next(start), s.Next(s.Next(start))
	store := &fakeCassandra{buckets: map[int64][]int{
		start: {1}, n1: {2}, n2: {3, 4, 5},
	}}
	ceiling := s.Next(s.Next(s.Next(start))) // well above n2

	res, err := walkBuckets[int](context.Background(), s, walkAsc, start, ceiling, 365, 4, nil, 8, store.fetch)
	require.NoError(t, err)

	assert.Equal(t, []int{1, 2, 3, 4}, res.Rows)
	assert.True(t, res.HasNext)
	b, ps, err := decodeBucketCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, n2, b, "ASC walk resumes mid-bucket in n2")
	assert.Equal(t, 2, decodeTestOffset(ps), "two rows (3,4) consumed from n2")
}

// TestWalkBuckets_FanoutParityWithSerial is the core safety guarantee:
// concurrency must not change the result. The same store walked at fanout 1 and
// fanout 8 must return byte-identical rows and cursor.
func TestWalkBuckets_FanoutParityWithSerial(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	build := func() *fakeCassandra {
		b1 := s.Prev(start)
		b2 := s.Prev(b1)
		b3 := s.Prev(b2)
		return &fakeCassandra{buckets: map[int64][]int{
			start: {1, 2},
			b1:    {}, // empty gap
			b2:    {3, 4, 5, 6, 7},
			b3:    {8, 9},
		}}
	}
	serial := build()
	concurrent := build()

	resSerial, err := walkBuckets[int](context.Background(), s, walkDesc, start, noFloorDesc, 365, 6, nil, 1, serial.fetch)
	require.NoError(t, err)
	resConc, err := walkBuckets[int](context.Background(), s, walkDesc, start, noFloorDesc, 365, 6, nil, 8, concurrent.fetch)
	require.NoError(t, err)

	assert.Equal(t, resSerial.Rows, resConc.Rows)
	assert.Equal(t, resSerial.NextCursor, resConc.NextCursor)
	assert.Equal(t, resSerial.HasNext, resConc.HasNext)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, resConc.Rows)
}

// TestWalkBuckets_ActuallyFansOutConcurrently proves the walk issues bucket
// queries in parallel: the fake blocks each fetch until barrierAt goroutines are
// concurrently in-flight, so the test can only complete if the walk fetches
// buckets concurrently. Run under -race.
func TestWalkBuckets_ActuallyFansOutConcurrently(t *testing.T) {
	s := testSizer()
	start := testStartBucket()
	buckets := map[int64][]int{start: {}} // empty start forces a fan-out wave
	b := start
	for i := 0; i < 8; i++ {
		b = s.Prev(b)
		buckets[b] = []int{i}
	}
	store := &fakeCassandra{buckets: buckets, barrierAt: 2, gate: make(chan struct{})}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := walkBuckets[int](ctx, s, walkDesc, start, noFloorDesc, 20, 100, nil, 8, store.fetch)
	require.NoError(t, err)
	require.NoError(t, ctx.Err(), "walk deadlocked: buckets were not fetched concurrently")

	assert.GreaterOrEqual(t, store.maxInFlight, 2, "expected concurrent bucket fetches")
	assert.Len(t, res.Rows, 8)
}

package cassrepo

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

// fakeFetcher serves rows from an in-memory map[bucket][]int, honoring `limit`
// and an opaque 4-byte big-endian offset pageState — mirroring gocql's
// PageSize/PageState contract so the walk orchestration can be exercised
// without Cassandra. nextPageState is non-nil iff the bucket was truncated at
// `limit` with rows still remaining.
func fakeFetcher(data map[int64][]int, calls *[]int64, mu *sync.Mutex) bucketFetcher[int] {
	return func(_ context.Context, bucket int64, _ bool, pageState []byte, limit int) (bucketResult[int], error) {
		if calls != nil {
			mu.Lock()
			*calls = append(*calls, bucket)
			mu.Unlock()
		}
		rows := data[bucket]
		offset := 0
		if len(pageState) == 4 {
			offset = int(binary.BigEndian.Uint32(pageState))
		}
		if offset > len(rows) {
			offset = len(rows)
		}
		end := offset + limit
		if end > len(rows) {
			end = len(rows)
		}
		out := append([]int(nil), rows[offset:end]...)
		var next []byte
		if end < len(rows) {
			next = make([]byte, 4)
			binary.BigEndian.PutUint32(next, uint32(end)) // #nosec G115 -- bounded by len(rows)
		}
		return bucketResult[int]{rows: out, nextPageState: next}, nil
	}
}

// paginateAll drives walkBuckets to completion, threading each page's cursor
// into the next call, and returns the per-page row slices.
func paginateAll(t *testing.T, sizer msgbucket.Sizer, dir walkDirection, start, floor int64, cfg walkConfig, pageSize int, fetch bucketFetcher[int]) [][]int {
	t.Helper()
	var pages [][]int
	bucket := start
	var ps []byte
	for i := 0; ; i++ {
		require.Less(t, i, 10000, "pagination did not terminate")
		res, err := walkBuckets[int](context.Background(), sizer, dir, bucket, floor, cfg, pageSize, ps, fetch)
		require.NoError(t, err)
		pages = append(pages, res.Rows)
		if !res.HasNext {
			break
		}
		b, nps, err := decodeBucketCursor(res.NextCursor)
		require.NoError(t, err)
		bucket, ps = b, nps
	}
	return pages
}

// TestWalkBuckets_ParallelMatchesSerial is the differential test: for the same
// data, a parallel (concurrency>1) walk must produce byte-identical pages to a
// serial (concurrency=1) walk, across both directions, varied gap densities,
// and varied page sizes. This is the core correctness guarantee.
func TestWalkBuckets_ParallelMatchesSerial(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	win := sizer.WindowMs()

	directions := []struct {
		name string
		dir  walkDirection
	}{
		{"desc", walkDesc},
		{"asc", walkAsc},
	}

	for _, seed := range []int64{1, 7, 42, 1234, 99999} {
		for _, gapPct := range []int{0, 50, 85, 97} {
			for _, pageSize := range []int{1, 5, 20, 50} {
				// #nosec G404 -- deterministic test fixtures, not security-sensitive
				rng := rand.New(rand.NewSource(seed + int64(gapPct) + int64(pageSize)))
				const spanBuckets = 250
				data := map[int64][]int{}
				id := 0
				for idx := 0; idx <= spanBuckets; idx++ {
					if rng.Intn(100) < gapPct {
						continue // empty bucket
					}
					n := 1 + rng.Intn(6)
					rows := make([]int, 0, n)
					for k := 0; k < n; k++ {
						id++
						rows = append(rows, id)
					}
					data[int64(idx)*win] = rows
				}

				for _, d := range directions {
					var start, floor int64
					if d.dir == walkDesc {
						start, floor = int64(spanBuckets)*win, 0
					} else {
						start, floor = 0, int64(spanBuckets)*win
					}
					serialCfg := walkConfig{maxBuckets: 100000, concurrency: 1, escalateAfter: 0}
					parCfg := walkConfig{maxBuckets: 100000, concurrency: 4, escalateAfter: 3}

					serial := paginateAll(t, sizer, d.dir, start, floor, serialCfg, pageSize, fakeFetcher(data, nil, nil))
					par := paginateAll(t, sizer, d.dir, start, floor, parCfg, pageSize, fakeFetcher(data, nil, nil))

					assert.Equalf(t, serial, par,
						"seed=%d gap=%d page=%d dir=%s: parallel pages diverged from serial", seed, gapPct, pageSize, d.name)
				}
			}
		}
	}
}

// TestWalkBuckets_EscalatesAndHandsBackToSerial proves the parallel path
// actually runs: after escalateAfter consecutive empties it batch-probes ahead,
// then re-reads the first data bucket on the serial path (so the data bucket is
// fetched twice — once speculatively, once for the exact-limit page).
func TestWalkBuckets_EscalatesAndHandsBackToSerial(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	win := sizer.WindowMs()
	dataBucket := int64(5) * win
	data := map[int64][]int{dataBucket: {101, 102, 103}}

	var calls []int64
	var mu sync.Mutex
	cfg := walkConfig{maxBuckets: 100, concurrency: 4, escalateAfter: 2}
	res, err := walkBuckets[int](context.Background(), sizer, walkDesc, int64(20)*win, 0, cfg, 10, nil, fakeFetcher(data, &calls, &mu))
	require.NoError(t, err)

	assert.Equal(t, []int{101, 102, 103}, res.Rows)
	assert.False(t, res.HasNext, "walk reaching the floor must terminate without a cursor")

	count := 0
	for _, b := range calls {
		if b == dataBucket {
			count++
		}
	}
	assert.GreaterOrEqualf(t, count, 2, "data bucket should be probed in a batch then re-read serially; calls=%v", calls)
}

// TestWalkBuckets_RespectsMaxBuckets confirms a walk over empties stops after
// exactly maxBuckets, with identical stop position for serial and parallel.
func TestWalkBuckets_RespectsMaxBuckets(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	win := sizer.WindowMs()
	start := int64(100) * win
	data := map[int64][]int{} // entirely empty

	for _, concurrency := range []int{1, 4} {
		cfg := walkConfig{maxBuckets: 5, concurrency: concurrency, escalateAfter: 2}
		res, err := walkBuckets[int](context.Background(), sizer, walkDesc, start, 0, cfg, 10, nil, fakeFetcher(data, nil, nil))
		require.NoError(t, err)
		assert.Empty(t, res.Rows, "concurrency=%d", concurrency)
		require.True(t, res.HasNext, "concurrency=%d: capped walk must signal more", concurrency)
		b, _, err := decodeBucketCursor(res.NextCursor)
		require.NoError(t, err)
		assert.Equal(t, start-5*win, b, "concurrency=%d: must stop exactly maxBuckets in", concurrency)
	}
}

// TestWalkBuckets_StopsAtFloor confirms floor termination yields no cursor.
func TestWalkBuckets_StopsAtFloor(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	win := sizer.WindowMs()
	data := map[int64][]int{} // empty

	for _, concurrency := range []int{1, 4} {
		cfg := walkConfig{maxBuckets: 1000, concurrency: concurrency, escalateAfter: 2}
		res, err := walkBuckets[int](context.Background(), sizer, walkDesc, int64(8)*win, 0, cfg, 10, nil, fakeFetcher(data, nil, nil))
		require.NoError(t, err)
		assert.Empty(t, res.Rows)
		assert.False(t, res.HasNext, "concurrency=%d: reaching floor must terminate", concurrency)
		assert.Empty(t, res.NextCursor, "concurrency=%d", concurrency)
	}
}

// TestWalkBuckets_BatchRunsConcurrently proves the escalated batch is fetched
// in parallel, not sequentially: the batch fetchers rendezvous at a barrier
// that only releases once `concurrency` of them are simultaneously in flight.
// The channel is the synchronization primitive; time.After is only a
// deadlock failsafe so a serial regression fails (slowly) instead of hanging.
func TestWalkBuckets_BatchRunsConcurrently(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	win := sizer.WindowMs()
	const concurrency = 4

	var inFlight, maxInFlight int32
	var skipFirst sync.Once
	var releaseOnce sync.Once
	release := make(chan struct{})

	fetch := func(_ context.Context, _ int64, _ bool, _ []byte, _ int) (bucketResult[int], error) {
		// The single pre-escalation serial empty must not join the barrier.
		skipped := false
		skipFirst.Do(func() { skipped = true })
		if !skipped {
			n := atomic.AddInt32(&inFlight, 1)
			for {
				m := atomic.LoadInt32(&maxInFlight)
				if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
					break
				}
			}
			if n >= concurrency {
				releaseOnce.Do(func() { close(release) })
			}
			select {
			case <-release:
			case <-time.After(500 * time.Millisecond):
			}
			atomic.AddInt32(&inFlight, -1)
		}
		return bucketResult[int]{}, nil // every bucket empty
	}

	cfg := walkConfig{maxBuckets: 12, concurrency: concurrency, escalateAfter: 1}
	_, err := walkBuckets[int](context.Background(), sizer, walkDesc, int64(50)*win, 0, cfg, 10, nil, fetch)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&maxInFlight)), 2, "escalated batch must fetch buckets concurrently")
}

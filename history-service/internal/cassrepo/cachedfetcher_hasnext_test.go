package cassrepo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/msgbucket"
)

// liveLikeFetcher is a bucketFetcher honoring the contract gocqlBucketFetcher
// implements: return min(limit, len(rows)) rows, and report a non-empty
// resumeState exactly when rows remain in the bucket. The cached fetcher must
// agree with this one on HasNext for every walk shape.
func liveLikeFetcher(rows []models.Message) bucketFetcher[models.Message] {
	return func(_ context.Context, _ int64, _ bool, _ []byte, limit int) (bucketPage[models.Message], error) {
		if limit >= len(rows) {
			return bucketPage[models.Message]{rows: rows}, nil
		}
		return bucketPage[models.Message]{rows: rows[:limit], resumeState: []byte("more-rows-here")}, nil
	}
}

// hasNextRows builds count messages in created_at DESC order, one minute apart.
func hasNextRows(base time.Time, count int) []models.Message {
	rows := make([]models.Message, count)
	for i := range rows {
		rows[i] = models.Message{
			MessageID: fmt.Sprintf("m%02d", i),
			RoomID:    "r1",
			CreatedAt: base.Add(-time.Duration(i) * time.Minute),
			Msg:       fmt.Sprintf("body-%d", i),
		}
	}
	return rows
}

// TestCachedDescFetcher_HasNextMatchesLive pins the cached DESC walk to the live
// walk's HasNext.
//
// cachedDescFetcher always returns a nil resumeState, even when it truncates a
// cached bucket to `limit`. The PR treats that as a cursor-only concern and
// guards resume cursors at the method, but resumeState also feeds HasNext:
// walker.resumeAfterFill reads an empty resumeState as "bucket drained" and
// advances. When the page fills at the floor bucket, that advance crosses the
// floor and the walk reports terminalPage — HasNext=false — while rows remain.
//
// HasNext is client-visible: service/messages.go builds LoadHistoryResponse.HasNext
// from it, and that RPC pages by before = oldest returned createdAt, so a client
// honoring hasNext=false stops paginating and never sees the older messages.
// service/rooms.go and LoadSurroundingMessages.MoreBefore read it too.
func TestCachedDescFetcher_HasNextMatchesLive(t *testing.T) {
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))
	base := fetcherNow.Add(-73 * time.Hour) // inside the sealed bucket

	tests := []struct {
		name string
		// rowCount is how many rows the sealed bucket holds.
		rowCount int
		pageSize int
		// floorIsSealedBucket puts the walk floor at the same bucket the rows are
		// in, so filling the page there crosses the floor on the next step.
		floorIsSealedBucket bool
	}{
		{
			// LoadHistory shape: a dormant room whose whole history window sits in
			// one sealed bucket holding more rows than one page.
			name:                "page fills at floor bucket with rows remaining",
			rowCount:            30,
			pageSize:            20,
			floorIsSealedBucket: true,
		},
		{
			// rooms.go preview walk: lastMsgWalkFirstPage is 1, so a single
			// ineligible newest message (deleted or a system message) makes the
			// walk consult HasNext on a one-row page.
			name:                "single-row preview page at floor bucket",
			rowCount:            2,
			pageSize:            1,
			floorIsSealedBucket: true,
		},
		{
			// Control: bucket drains exactly at the page boundary, so both walks
			// legitimately terminate.
			name:                "bucket drains exactly at page boundary",
			rowCount:            20,
			pageSize:            20,
			floorIsSealedBucket: true,
		},
		{
			// Control: rows remain but the floor is an older bucket, so the walk
			// advances without crossing it.
			name:                "rows remain with floor below the bucket",
			rowCount:            30,
			pageSize:            20,
			floorIsSealedBucket: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mv := newMemValkey()
			r := newCachedRepo(t, mv)
			rows := hasNextRows(base, tt.rowCount)
			seedCache(t, mv, sealed, rows...)

			floorBucket := sealed
			if !tt.floorIsSealedBucket {
				floorBucket = sizer.Prev(sealed)
			}

			live := liveLikeFetcher(rows)
			cached := r.cachedDescFetcher("r1", fetcherNow, nil, floorBucket, live)

			walk := func(f bucketFetcher[models.Message]) pageResult[models.Message] {
				res, err := walkBuckets(context.Background(), sizer, walkDesc,
					sealed, floorBucket, 122, tt.pageSize, nil, 4, f)
				require.NoError(t, err)
				return res
			}

			liveRes := walk(live)
			cachedRes := walk(cached)

			require.Equal(t, len(liveRes.Rows), len(cachedRes.Rows),
				"cached and live walks must return the same number of rows")
			assert.Equal(t, liveRes.HasNext, cachedRes.HasNext,
				"cached walk reported HasNext=%v where live reports %v; a client honoring "+
					"hasNext stops paginating and never sees the remaining %d rows",
				cachedRes.HasNext, liveRes.HasNext, tt.rowCount-len(cachedRes.Rows))
		})
	}
}

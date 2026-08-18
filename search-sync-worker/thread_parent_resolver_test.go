package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prevMonth is the trap in this file: replyAt.AddDate(0, -1, 0) normalises
// "31 February" back into March, so the boundary probe would silently repeat the
// month it just tried.
func TestPrevMonth(t *testing.T) {
	tests := []struct {
		name      string
		at        time.Time
		wantYear  int
		wantMonth time.Month
	}{
		{"mid-month", time.Date(2026, 6, 14, 9, 0, 0, 0, time.UTC), 2026, time.May},
		{"31st of a month the previous one is shorter than", time.Date(2026, 3, 31, 23, 59, 0, 0, time.UTC), 2026, time.February},
		{"1st of the month", time.Date(2026, 3, 1, 0, 5, 0, 0, time.UTC), 2026, time.February},
		{"january rolls back a year", time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC), 2025, time.December},
		{"leap february", time.Date(2024, 3, 30, 12, 0, 0, 0, time.UTC), 2024, time.February},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := prevMonth(tt.at)
			assert.Equal(t, tt.wantYear, got.Year())
			assert.Equal(t, tt.wantMonth, got.Month())
		})
	}
}

// recordingReader captures which indices were probed, and answers from a fixture.
type recordingReader struct {
	found   map[string]time.Time // index -> createdAt present in that index
	probed  []string
	searchN int
	searchT time.Time
	searchK bool
}

func (r *recordingReader) get(_ context.Context, index, _ string) (time.Time, bool) {
	r.probed = append(r.probed, index)
	at, ok := r.found[index]
	return at, ok
}

func (r *recordingReader) search(context.Context, string) (time.Time, bool) {
	r.searchN++
	return r.searchT, r.searchK
}

func newTestResolver(rr *recordingReader) *esParentResolver {
	return &esParentResolver{
		esGet:       rr.get,
		esSearch:    rr.search,
		indexPrefix: "msgs-v1",
		timeout:     time.Second,
	}
}

func TestESParentResolver_ResolveParentCreatedAt(t *testing.T) {
	parentAt := time.Date(2026, 6, 1, 9, 30, 0, 0, time.UTC)
	replyAt := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)

	t.Run("a same-month parent costs one point read and no search", func(t *testing.T) {
		rr := &recordingReader{found: map[string]time.Time{"msgs-v1-2026-06": parentAt}}

		got, ok := newTestResolver(rr).ResolveParentCreatedAt(context.Background(), "parent-1", replyAt)

		require.True(t, ok)
		assert.True(t, got.Equal(parentAt))
		assert.Equal(t, []string{"msgs-v1-2026-06"}, rr.probed)
		assert.Zero(t, rr.searchN, "the cross-month search must stay off the common path")
	})

	t.Run("falls back to the previous month before searching", func(t *testing.T) {
		rr := &recordingReader{found: map[string]time.Time{"msgs-v1-2026-05": parentAt}}

		got, ok := newTestResolver(rr).ResolveParentCreatedAt(context.Background(), "parent-1", replyAt)

		require.True(t, ok)
		assert.True(t, got.Equal(parentAt))
		assert.Equal(t, []string{"msgs-v1-2026-06", "msgs-v1-2026-05"}, rr.probed)
		assert.Zero(t, rr.searchN)
	})

	t.Run("an older parent still resolves via the cross-month search", func(t *testing.T) {
		old := time.Date(2025, 1, 2, 3, 4, 0, 0, time.UTC)
		rr := &recordingReader{found: map[string]time.Time{}, searchT: old, searchK: true}

		got, ok := newTestResolver(rr).ResolveParentCreatedAt(context.Background(), "parent-1", replyAt)

		require.True(t, ok)
		assert.True(t, got.Equal(old))
		assert.Equal(t, []string{"msgs-v1-2026-06", "msgs-v1-2026-05"}, rr.probed)
		assert.Equal(t, 1, rr.searchN)
	})

	t.Run("ok=false when nothing has the parent", func(t *testing.T) {
		rr := &recordingReader{found: map[string]time.Time{}}

		_, ok := newTestResolver(rr).ResolveParentCreatedAt(context.Background(), "missing", replyAt)

		assert.False(t, ok)
		assert.Equal(t, 1, rr.searchN, "the search is the last resort, tried once")
	})
}

package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPagedLimit(t *testing.T) {
	ptr := func(i int) *int { return &i }

	cases := []struct {
		name   string
		limit  *int
		want   int64
		wantOK bool
	}{
		{"nil limit reads unbounded", nil, 0, false},
		{"zero limit reads unbounded", ptr(0), 0, false},
		{"negative limit reads unbounded", ptr(-3), 0, false},
		{"positive limit over-fetches one row", ptr(1), 2, true},
		{"larger limit over-fetches one row", ptr(50), 51, true},
		{"max int would overflow, so reads unbounded", ptr(math.MaxInt), 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := pagedLimit(tc.limit)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.want, got)
			if ok {
				assert.Positive(t, got, "a $limit/SetLimit value must stay positive")
			}
		})
	}
}

func TestTrimOverFetch(t *testing.T) {
	ptr := func(i int) *int { return &i }

	cases := []struct {
		name        string
		rows        []string
		limit       *int
		want        []string
		wantHasMore bool
	}{
		{"nil limit keeps every row", []string{"a", "b"}, nil, []string{"a", "b"}, false},
		{"non-positive limit keeps every row", []string{"a", "b"}, ptr(0), []string{"a", "b"}, false},
		{"short page has no further rows", []string{"a"}, ptr(2), []string{"a"}, false},
		{"exactly full page has no further rows", []string{"a", "b"}, ptr(2), []string{"a", "b"}, false},
		{"probe row is trimmed and reported", []string{"a", "b", "c"}, ptr(2), []string{"a", "b"}, true},
		{"empty page", nil, ptr(2), nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, hasMore := trimOverFetch(tc.rows, tc.limit)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantHasMore, hasMore)
		})
	}
}

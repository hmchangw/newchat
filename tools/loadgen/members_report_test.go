package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrintMembersSummary_IncludesAllSections(t *testing.T) {
	s := MembersSummary{
		Preset: "members-medium", Site: "site-A", Inject: "frontdoor", Shape: "users",
		Seed: 42, TargetRate: 100, ActualRate: 98.7,
		Duration: 60 * time.Second, Warmup: 10 * time.Second,
		UsersPerAdd: 10, Sent: 6000, SentMeasured: 5000,
		PublishErrors: 0, RoomServiceErrors: 2,
		MissingReplies: 1, MissingEvents: 0,
		E1:      Percentiles{P50: 4 * time.Millisecond, P95: 12 * time.Millisecond, P99: 28 * time.Millisecond, Max: 50 * time.Millisecond},
		E2:      Percentiles{P50: 10 * time.Millisecond, P95: 31 * time.Millisecond, P99: 78 * time.Millisecond, Max: 90 * time.Millisecond},
		E1Count: 5000, E2Count: 4999,
		Consumers: []ConsumerStat{{Stream: "ROOMS_site-A", Durable: "room-worker", FinalPending: 0}},
	}
	var buf bytes.Buffer
	require.NoError(t, PrintMembersSummary(&buf, &s))
	out := buf.String()
	for _, want := range []string{
		"members-medium", "frontdoor", "users",
		"target rate: 100",
		"users per add:    10",
		"publish errors:    0",
		"room-service errors:", "2",
		"E1 reply", "E2 member-event",
		"ROOMS_site-A", "room-worker",
	} {
		assert.True(t, strings.Contains(out, want), "summary missing %q\n--- output ---\n%s", want, out)
	}
}

func TestPrintCapacitySummary_BucketTable(t *testing.T) {
	s := CapacitySummary{
		Preset: "members-capacity", Site: "site-A", Inject: "frontdoor", Shape: "users",
		Seed: 1, UsersPerAdd: 10, TargetSize: 500,
		Buckets: []SizeBucket{
			{Lower: 0, Upper: 100, Count: 10,
				E1: Percentiles{P50: 5 * time.Millisecond}, E2: Percentiles{P50: 12 * time.Millisecond}},
			{Lower: 100, Upper: 500, Count: 40,
				E1: Percentiles{P50: 8 * time.Millisecond}, E2: Percentiles{P50: 30 * time.Millisecond}},
		},
		FinalSizes: map[string]int{"r1": 500, "r2": 500},
	}
	var buf bytes.Buffer
	require.NoError(t, PrintCapacitySummary(&buf, &s))
	out := buf.String()
	for _, want := range []string{"members-capacity", "size_bucket", "0-100", "100-500", "r1", "500"} {
		assert.True(t, strings.Contains(out, want), "capacity summary missing %q\n%s", want, out)
	}
}

func TestBucketize_BoundaryEdges(t *testing.T) {
	edges := []int{0, 10, 100, 1000}
	cases := []struct {
		size int
		want int
	}{
		{0, 0}, {9, 0}, {10, 1}, {99, 1}, {100, 2}, {999, 2}, {1000, -1}, {-1, -1},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, BucketIndex(tc.size, edges), "size=%d", tc.size)
	}
}

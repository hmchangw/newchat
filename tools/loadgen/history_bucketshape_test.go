package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prodBucketWidth is history-service's MESSAGE_BUCKET_HOURS default (360h =
// 15 days). Seed-time and read-time bucket math must agree, so a benchmark run
// configures loadgen with whatever the service under test uses; these tests pin
// the shape at the production default.
const prodBucketWidth = 360 * time.Hour

// TestHistoryPresets_BucketShape records which presets can measure the
// per-bucket read cache and which cannot.
//
// Two independent thresholds decide it, both from history-service's defaults:
// a bucket denser than historyCacheMaxRows is declined as oversized and never
// cached at all, and a bucket holding at least a full page leaves no
// multi-bucket walk for the cache to collapse.
//
// The small/medium/large presets fail both by one to two orders of magnitude —
// they model busy rooms, and this cache targets sparse ones. Running a
// cache-effectiveness benchmark on them measures nothing and reads as "the
// cache does not help", which is why history-sparse exists.
func TestHistoryPresets_BucketShape(t *testing.T) {
	tests := []struct {
		preset string
		// cacheable is whether a bucket is under the oversized cap, i.e. whether
		// the cache can hold it at all.
		cacheable bool
		// multiBucketPages is whether a default page spans more than one bucket,
		// i.e. whether there is a walk worth collapsing.
		multiBucketPages bool
	}{
		{preset: "history-small", cacheable: false, multiBucketPages: false},
		{preset: "history-medium", cacheable: false, multiBucketPages: false},
		{preset: "history-large", cacheable: false, multiBucketPages: false},
		{preset: "history-sparse", cacheable: true, multiBucketPages: true},
	}

	for _, tt := range tests {
		t.Run(tt.preset, func(t *testing.T) {
			p, ok := BuiltinHistoryPreset(tt.preset)
			require.True(t, ok)

			rows := HistoryRowsPerBucket(&p, prodBucketWidth)
			t.Logf("%s: %.0f rows/bucket, %d sealed buckets at a %v window",
				tt.preset, rows, HistorySealedBuckets(&p, prodBucketWidth), prodBucketWidth)

			assert.Equal(t, tt.cacheable, rows <= historyCacheMaxRows,
				"rows/bucket %.0f vs oversized cap %d: above the cap every bucket is "+
					"declined and the cache serves nothing", rows, historyCacheMaxRows)
			assert.Equal(t, tt.multiBucketPages, rows < historyDefaultPageLimit,
				"rows/bucket %.0f vs page size %d: at or above a full page, a page "+
					"fills from one bucket and there is no walk to collapse",
				rows, historyDefaultPageLimit)
		})
	}
}

// The cache only serves SEALED buckets, so a preset whose whole span fits in the
// current bucket can never produce a hit however sparse it is. history-sparse
// spans many buckets at the production width; history-medium spans less than
// one.
func TestHistoryPresets_SealedBucketCoverage(t *testing.T) {
	sparse, ok := BuiltinHistoryPreset("history-sparse")
	require.True(t, ok)
	assert.GreaterOrEqual(t, HistorySealedBuckets(&sparse, prodBucketWidth), 20,
		"history-sparse must span enough sealed buckets for a scrollback chain to cross several")

	medium, ok := BuiltinHistoryPreset("history-medium")
	require.True(t, ok)
	assert.Zero(t, HistorySealedBuckets(&medium, prodBucketWidth),
		"history-medium's 7-day span fits inside one 15-day bucket, which is the "+
			"current one — nothing it seeds is ever cacheable")
}

func TestHistoryRowsPerBucket_Edges(t *testing.T) {
	p := HistoryPreset{MessagesPerRoom: 100, MessageSpanDays: 10}

	assert.InDelta(t, 10.0, HistoryRowsPerBucket(&p, 24*time.Hour), 0.001,
		"a bucket covering a tenth of the span holds a tenth of the rows")
	assert.InDelta(t, 100.0, HistoryRowsPerBucket(&p, 30*24*time.Hour), 0.001,
		"a bucket wider than the span holds the whole room")
	assert.Zero(t, HistoryRowsPerBucket(&p, 0))
	assert.Zero(t, HistoryRowsPerBucket(&HistoryPreset{MessagesPerRoom: 10}, time.Hour),
		"a zero span has no defined density")
}

func TestHistorySealedBuckets_Edges(t *testing.T) {
	p := HistoryPreset{MessageSpanDays: 10}

	assert.Equal(t, 9, HistorySealedBuckets(&p, 24*time.Hour), "10 buckets, newest is current")
	assert.Equal(t, 0, HistorySealedBuckets(&p, 30*24*time.Hour), "one bucket, and it is current")
	assert.Equal(t, 0, HistorySealedBuckets(&p, 0))
}

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestNewCassandraMessageSource(t *testing.T) {
	sizer := msgbucket.New(24 * time.Hour)

	source := newCassandraMessageSource(nil, sizer)

	require.NotNil(t, source)
	assert.Nil(t, source.session)
	assert.Equal(t, sizer, source.sizer)
}

func TestBucketRange_SingleBucketWindow(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	from := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)

	buckets := bucketRange(sizer, from, to)

	assert.Equal(t, []int64{sizer.Of(from)}, buckets)
}

func TestBucketRange_MultiBucketWindowIncludesEveryBucketTouchingTheRange(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	// from is aligned to a bucket boundary so the 200h window below spans
	// exactly 3 windows of 72h — an unaligned `from` (e.g. the raw
	// 2026-07-01 date, which lands 24h into its epoch-aligned bucket) would
	// spill into a 4th bucket and make this test's "spans 3 windows" premise
	// false regardless of bucketRange's correctness.
	from := time.UnixMilli(sizer.Of(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)))
	to := from.Add(200 * time.Hour) // spans 3 windows of 72h

	buckets := bucketRange(sizer, from, to)

	assert.Len(t, buckets, 3)
	assert.Equal(t, sizer.Of(from), buckets[0])
	assert.Equal(t, sizer.Of(to.Add(-time.Millisecond)), buckets[len(buckets)-1])
}

func TestBucketRange_ToExactlyOnABucketBoundaryExcludesThatBucket(t *testing.T) {
	sizer := msgbucket.New(72 * time.Hour)
	from := time.UnixMilli(sizer.Of(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)))
	to := time.UnixMilli(sizer.Next(sizer.Of(from))) // exactly the next bucket boundary

	buckets := bucketRange(sizer, from, to)

	// [from, to) — the bucket starting exactly at `to` holds no row < to, so it must not be walked.
	assert.Equal(t, []int64{sizer.Of(from)}, buckets)
}

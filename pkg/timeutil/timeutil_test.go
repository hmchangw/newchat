package timeutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMillisToTime(t *testing.T) {
	assert.Nil(t, MillisToTime(nil), "nil in ⇒ nil out")

	ms := int64(1735689600123) // 2025-01-01T00:00:00.123Z
	got := MillisToTime(&ms)
	require.NotNil(t, got)
	assert.Equal(t, time.UnixMilli(ms).UTC(), *got)
	assert.Equal(t, time.UTC, got.Location(), "result is normalized to UTC")
}

func TestTimeToMillis(t *testing.T) {
	assert.Nil(t, TimeToMillis(nil), "nil in ⇒ nil out")

	ts := time.Date(2025, 1, 1, 0, 0, 0, 123_000_000, time.UTC)
	got := TimeToMillis(&ts)
	require.NotNil(t, got)
	assert.Equal(t, ts.UnixMilli(), *got)
}

func TestRoundTrip(t *testing.T) {
	ms := int64(1735689600123)
	require.Equal(t, ms, *TimeToMillis(MillisToTime(&ms)), "millis → time → millis is lossless")

	// A non-UTC timestamp converts by absolute instant, not wall clock.
	loc := time.FixedZone("UTC+5", 5*3600)
	ts := time.Date(2025, 1, 1, 5, 0, 0, 0, loc)
	require.Equal(t, ts.UnixMilli(), *TimeToMillis(&ts))
}

func TestCoalesce(t *testing.T) {
	a := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	b := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	assert.Same(t, &a, Coalesce(&a, &b), "first non-nil wins")
	assert.Same(t, &b, Coalesce(nil, &b), "nil first ⇒ second")
	assert.Nil(t, Coalesce[time.Time](nil, nil), "both nil ⇒ nil")

	ms := int64(42)
	assert.Same(t, &ms, Coalesce(&ms, nil), "works for the millis shape too")
}

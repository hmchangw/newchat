package msgbucket_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/msgbucket"
)

func TestSizer_Of(t *testing.T) {
	tests := []struct {
		name   string
		window time.Duration
		t      time.Time
		want   int64
	}{
		{
			name:   "epoch in 24h window",
			window: 24 * time.Hour,
			t:      time.UnixMilli(0).UTC(),
			want:   0,
		},
		{
			name:   "1ms after start of bucket lands in same bucket",
			window: 24 * time.Hour,
			t:      time.UnixMilli(1).UTC(),
			want:   0,
		},
		{
			name:   "1ms before window edge lands in same bucket",
			window: 24 * time.Hour,
			t:      time.UnixMilli(86_400_000 - 1).UTC(),
			want:   0,
		},
		{
			name:   "exactly on window edge advances",
			window: 24 * time.Hour,
			t:      time.UnixMilli(86_400_000).UTC(),
			want:   86_400_000,
		},
		{
			name:   "1h window",
			window: time.Hour,
			t:      time.UnixMilli(3_600_000 + 123).UTC(),
			want:   3_600_000,
		},
		{
			name:   "12h window",
			window: 12 * time.Hour,
			t:      time.UnixMilli(13 * 3_600_000).UTC(),
			want:   12 * 3_600_000,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := msgbucket.New(tc.window)
			assert.Equal(t, tc.want, s.Of(tc.t))
		})
	}
}

func TestSizer_PrevNextRoundTrip(t *testing.T) {
	s := msgbucket.New(24 * time.Hour)
	now := time.Date(2026, 5, 1, 12, 34, 56, 0, time.UTC)
	b := s.Of(now)
	assert.Equal(t, b, s.Next(s.Prev(b)))
	assert.Equal(t, b, s.Prev(s.Next(b)))
}

func TestSizer_PrevAdvancesBy_Window(t *testing.T) {
	s := msgbucket.New(6 * time.Hour)
	b := s.Of(time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC))
	assert.Equal(t, b-int64(6*time.Hour/time.Millisecond), s.Prev(b))
	assert.Equal(t, b+int64(6*time.Hour/time.Millisecond), s.Next(b))
}

func TestSizer_WindowMs(t *testing.T) {
	s := msgbucket.New(24 * time.Hour)
	assert.Equal(t, int64(86_400_000), s.WindowMs())
}

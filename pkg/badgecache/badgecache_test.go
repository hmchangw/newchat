package badgecache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKey_HashTagShape(t *testing.T) {
	tests := []struct {
		name    string
		account string
		want    string
	}{
		{"simple account", "alice", "badge:{alice}"},
		{"empty account", "", "badge:{}"},
		{"account with special chars", "alice@example.com", "badge:{alice@example.com}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Key(tt.account))
		})
	}
}

func TestKey_DifferentAccountsDifferentKeys(t *testing.T) {
	assert.NotEqual(t, Key("alice"), Key("bob"))
}

func TestMarkerKey_SameSlotAsKey(t *testing.T) {
	assert.Equal(t, "badge:fresh:{alice}", MarkerKey("alice"))
	// Same {…} hash tag content as Key → same cluster slot (scripts and DELs
	// can address both keys atomically).
	assert.Contains(t, Key("alice"), "{alice}")
	assert.Contains(t, MarkerKey("alice"), "{alice}")
}

func TestNew_MarkerTTLDefaultsToSetTTL(t *testing.T) {
	c := New(nil, time.Hour, 10)
	assert.Equal(t, time.Hour, c.markerTTL, "absent option ⇒ marker shares the set TTL (today's behavior)")
}

func TestWithMarkerTTL(t *testing.T) {
	tests := []struct {
		name      string
		setTTL    time.Duration
		markerTTL time.Duration
		want      time.Duration
	}{
		{"shorter than set ttl is honored", 24 * time.Hour, 10 * time.Minute, 10 * time.Minute},
		{"equal to set ttl is honored", time.Hour, time.Hour, time.Hour},
		{"longer than set ttl is clamped", time.Hour, 24 * time.Hour, time.Hour},
		{"zero falls back to set ttl", time.Hour, 0, time.Hour},
		{"negative falls back to set ttl", time.Hour, -time.Second, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(nil, tt.setTTL, 10, WithMarkerTTL(tt.markerTTL))
			assert.Equal(t, tt.want, c.markerTTL)
		})
	}
}

func TestMarkerTTLSeconds_NeverExceedsUnjittered(t *testing.T) {
	c := New(nil, time.Hour, 10, WithMarkerTTL(10*time.Minute))
	seen := make(map[int64]struct{})
	for i := 0; i < 200; i++ {
		account := fmt.Sprintf("user%d", i)
		got := c.markerTTLSeconds(account)
		assert.GreaterOrEqual(t, got, int64(480), "must not shave more than 20%% off 600s")
		assert.LessOrEqual(t, got, int64(600), "must never exceed the unjittered marker TTL")
		seen[got] = struct{}{}
	}
	assert.Greater(t, len(seen), 1, "per-account offsets must produce more than one distinct value across many accounts")
}

func TestMarkerTTLSeconds_Deterministic(t *testing.T) {
	c := New(nil, time.Hour, 10, WithMarkerTTL(10*time.Minute))
	first := c.markerTTLSeconds("alice")
	for i := 0; i < 10; i++ {
		assert.Equal(t, first, c.markerTTLSeconds("alice"), "same account must return the same offset every call")
	}
}

func TestMarkerTTLSeconds_NeverZero(t *testing.T) {
	tests := []struct {
		name      string
		markerTTL time.Duration
	}{
		{"sub-second truncates to 0s and must still floor to 1", 500 * time.Millisecond},
		{"1s floors via early return", time.Second},
		{"2s must be floored, not truncated to 0", 2 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(nil, time.Hour, 10, WithMarkerTTL(tt.markerTTL))
			for i := 0; i < 200; i++ {
				account := fmt.Sprintf("user%d", i)
				got := c.markerTTLSeconds(account)
				assert.GreaterOrEqual(t, got, int64(1), "marker TTL seconds must never be 0")
			}
		})
	}
}

func TestNew_MaxCount(t *testing.T) {
	tests := []struct {
		name    string
		cap     int
		n       int64
		want    int
		wantCap int
	}{
		{"below cap passes through", 10, 3, 3, 10},
		{"above cap is capped", 10, 50, 10, 10},
		{"custom cap honored", 25, 50, 25, 25},
		{"zero cap falls back to default", 0, 50, DefaultMaxCount, DefaultMaxCount},
		{"negative cap falls back to default", -1, 50, DefaultMaxCount, DefaultMaxCount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(nil, time.Hour, tt.cap)
			assert.Equal(t, tt.wantCap, c.maxCount)
			assert.Equal(t, tt.want, c.capCount(tt.n))
		})
	}
}

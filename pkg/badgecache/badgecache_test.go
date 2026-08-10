package badgecache

import (
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

package valkeyutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClusterOptionsFor(t *testing.T) {
	tests := []struct {
		name        string
		profile     Profile
		wantRead    time.Duration
		wantRetries int
	}{
		{"cache profile", CacheProfile, 150 * time.Millisecond, 1},
		{"store profile", StoreProfile, 500 * time.Millisecond, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := ClusterOptionsFor([]string{"host:6379"}, "pw", tt.profile)

			assert.Equal(t, []string{"host:6379"}, opts.Addrs)
			assert.Equal(t, "pw", opts.Password)
			assert.Equal(t, tt.wantRead, opts.ReadTimeout)
			assert.Equal(t, tt.wantRead, opts.WriteTimeout)
			assert.Equal(t, tt.wantRetries, opts.MaxRetries)
			assert.Equal(t, time.Second, opts.DialTimeout)
			// The critical one: without this, a caller's context deadline does
			// not bound the socket read and the profile buys nothing.
			assert.True(t, opts.ContextTimeoutEnabled, "ContextTimeoutEnabled must be set")
		})
	}
}

func TestProfiles_CacheIsTighterThanStore(t *testing.T) {
	// Presence uses Valkey as its store of record, so it gets more headroom
	// than the cache consumers, which all have a Mongo/ES fallback.
	assert.Less(t, CacheProfile.ReadTimeout, StoreProfile.ReadTimeout)
	assert.Less(t, CacheProfile.MaxRetries, StoreProfile.MaxRetries)
}

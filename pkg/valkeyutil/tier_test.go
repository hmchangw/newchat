package valkeyutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshAfter(t *testing.T) {
	tests := []struct {
		name string
		ttl  time.Duration
		want time.Duration
	}{
		{"90m tier", 90 * time.Minute, 67*time.Minute + 30*time.Second},
		{"one hour", time.Hour, 45 * time.Minute},
		{"zero", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, RefreshAfter(tt.ttl))
		})
	}
}

// The window must leave room for a re-validation to fail and still serve: an
// entry is refreshed with a quarter of its life left, not at the deadline.
func TestRefreshAfter_LeavesHeadroomBeforeExpiry(t *testing.T) {
	ttl := 90 * time.Minute
	assert.Less(t, RefreshAfter(ttl), ttl, "refresh must happen before expiry, or the slide can never run")
	assert.Equal(t, ttl/4, ttl-RefreshAfter(ttl), "a quarter of the TTL is the outage headroom")
}

func TestFresh(t *testing.T) {
	now := time.Now()
	ttl := time.Hour // refresh window: 45m
	tests := []struct {
		name     string
		cachedAt time.Time
		want     bool
	}{
		{"just written", now, true},
		{"inside the window", now.Add(-44 * time.Minute), true},
		{"on the boundary", now.Add(-45 * time.Minute), false},
		{"past the window", now.Add(-46 * time.Minute), false},
		{"never stamped", time.UnixMilli(0), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Fresh(tt.cachedAt.UnixMilli(), now, ttl))
		})
	}
}

type slideClient struct {
	Client
	expired []string
	ttls    map[string]time.Duration
	err     error
	found   bool
}

func (s *slideClient) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	s.expired = append(s.expired, key)
	if s.err != nil {
		return false, s.err
	}
	if s.ttls == nil {
		s.ttls = map[string]time.Duration{}
	}
	s.ttls[key] = ttl
	return s.found, nil
}

func TestSlideTTL_ReArmsTheDeadline(t *testing.T) {
	c := &slideClient{found: true}
	SlideTTL(context.Background(), c, "k1", time.Hour, "test")
	assert.Equal(t, []string{"k1"}, c.expired)
	assert.Equal(t, time.Hour, c.ttls["k1"])
}

// The slide is best-effort: a failure leaves the entry on its current deadline
// rather than propagating, because the caller is already serving stale by design.
func TestSlideTTL_SwallowsFailures(t *testing.T) {
	c := &slideClient{err: errors.New("valkey down")}
	require.NotPanics(t, func() { SlideTTL(context.Background(), c, "k1", time.Hour, "test") })
}

func TestSlideTTL_NilClientIsNoOp(t *testing.T) {
	require.NotPanics(t, func() { SlideTTL(context.Background(), nil, "k1", time.Hour, "test") })
}

type bustClient struct {
	Client
	deleted  []string
	err      error
	sawDelay bool
}

func (b *bustClient) Del(ctx context.Context, keys ...string) error {
	if _, ok := ctx.Deadline(); ok {
		b.sawDelay = true
	}
	b.deleted = append(b.deleted, keys...)
	return b.err
}

func TestBustKeys_DeletesEveryKey(t *testing.T) {
	c := &bustClient{}
	BustKeys(context.Background(), c, "test", "k1", "k2")
	assert.Equal(t, []string{"k1", "k2"}, c.deleted)
}

// A hung Valkey must never stall the write path that triggered the bust — the
// authoritative write already succeeded and the TTL reconciles a missed bust.
func TestBustKeys_BoundsTheCall(t *testing.T) {
	c := &bustClient{}
	BustKeys(context.Background(), c, "test", "k1")
	assert.True(t, c.sawDelay, "the delete must carry a deadline")
}

func TestBustKeys_SwallowsFailures(t *testing.T) {
	c := &bustClient{err: errors.New("valkey down")}
	require.NotPanics(t, func() { BustKeys(context.Background(), c, "test", "k1") })
}

func TestBustKeys_NilClientAndEmptyKeysAreNoOps(t *testing.T) {
	require.NotPanics(t, func() { BustKeys(context.Background(), nil, "test", "k1") })
	c := &bustClient{}
	BustKeys(context.Background(), c, "test")
	assert.Empty(t, c.deleted, "an empty key set must not issue a DEL")
}

func TestNoopRecorder_SatisfiesCacheRecorderWithoutPanicking(t *testing.T) {
	var rec CacheRecorder = NoopRecorder{}
	require.NotPanics(t, func() {
		rec.Hit(context.Background())
		rec.Miss(context.Background())
		rec.Error(context.Background())
	})
}

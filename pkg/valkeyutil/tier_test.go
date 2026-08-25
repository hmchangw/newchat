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
	deleted []string
	// calls records the key set of each individual DEL, which is what makes
	// cluster-slot grouping assertable — `deleted` alone flattens it away.
	calls    [][]string
	err      error
	sawDelay bool
}

// Del models a real client: a cancelled context fails before any command is
// issued. Without this the double silently accepts calls that production would
// refuse, and no test here could detect an inherited cancellation.
func (b *bustClient) Del(ctx context.Context, keys ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); ok {
		b.sawDelay = true
	}
	b.deleted = append(b.deleted, keys...)
	b.calls = append(b.calls, append([]string(nil), keys...))
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

// A bust runs AFTER the authoritative write has committed, so it must not
// inherit the caller's cancellation: a request that finishes (or a client that
// disconnects) the instant the write lands would otherwise skip the DEL
// entirely, leaving the cache serving data the write just invalidated for a
// full TTL. The deadline still applies — cancellation is dropped, not the bound.
func TestBustKeys_RunsAfterCallerCancellation(t *testing.T) {
	c := &bustClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	BustKeys(ctx, c, "test", "k1")

	assert.Equal(t, []string{"k1"}, c.deleted, "a cancelled caller must not skip the invalidation")
	assert.True(t, c.sawDelay, "the delete must still carry a deadline")
}

// Valkey rejects a cross-slot multi-key DEL outright, clearing NONE of the
// keys rather than some. Leaving that to each caller has already produced one
// production defect and three different hand-rolled obediences, so BustKeys
// groups the keys itself: same hash tag batches, everything else gets its own
// DEL. A bust is off the hot path, so the extra round trips cost nothing.
func TestBustKeys_GroupsKeysByHashTag(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want [][]string
	}{
		{
			name: "one shared hash tag batches into a single DEL",
			keys: []string{"sub:{r1}:alice", "sub:{r1}:bob"},
			want: [][]string{{"sub:{r1}:alice", "sub:{r1}:bob"}},
		},
		{
			name: "untagged keys never share a DEL",
			keys: []string{"user:id:u1", "user:acct:alice"},
			want: [][]string{{"user:id:u1"}, {"user:acct:alice"}},
		},
		{
			name: "different tags are separate DELs, each batched",
			keys: []string{"sub:{r1}:alice", "sub:{r2}:bob", "sub:{r1}:carol"},
			want: [][]string{{"sub:{r1}:alice", "sub:{r1}:carol"}, {"sub:{r2}:bob"}},
		},
		{
			name: "a tagged and an untagged key do not batch",
			keys: []string{"room:{r1}:meta:v2", "room:v3:r1:subs"},
			want: [][]string{{"room:{r1}:meta:v2"}, {"room:v3:r1:subs"}},
		},
		{
			name: "an empty tag is not a tag",
			keys: []string{"a:{}:1", "b:{}:2"},
			want: [][]string{{"a:{}:1"}, {"b:{}:2"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &bustClient{}
			BustKeys(context.Background(), c, "test", tt.keys...)
			assert.Equal(t, tt.want, c.calls)
		})
	}
}

// Grouping must not let one failing slot swallow the others: each group is an
// independent DEL and a failure in one cannot skip the rest.
func TestBustKeys_OneFailingGroupDoesNotSkipTheOthers(t *testing.T) {
	c := &bustClient{err: errors.New("valkey down")}
	BustKeys(context.Background(), c, "test", "user:id:u1", "user:acct:alice")
	assert.Len(t, c.calls, 2, "every group must be attempted")
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

// cancelAwareSlideClient reports whether the context reaching EXPIRE was still
// live, which is the whole question for a slide issued after a failed load.
type cancelAwareSlideClient struct {
	Client
	sawCtxErr error
	calls     int
}

func (s *cancelAwareSlideClient) Expire(ctx context.Context, _ string, _ time.Duration) (bool, error) {
	s.calls++
	s.sawCtxErr = ctx.Err()
	return true, nil
}

// A slide runs precisely when the source read just failed, and one common
// reason for that failure is the caller's own context expiring. Inheriting it
// means the EXPIRE dies with it, the entry keeps its original deadline, and it
// lapses during the outage this tier exists to survive. BustKeys already drops
// cancellation for the same reason; the slide is the other half of that policy.
func TestSlideTTL_SurvivesACancelledCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := &cancelAwareSlideClient{}
	SlideTTL(ctx, c, "k1", time.Hour, "test")

	require.Equal(t, 1, c.calls, "the slide must still be attempted")
	assert.NoError(t, c.sawCtxErr,
		"the re-arm must not inherit the caller's cancellation — the entry would lapse mid-outage")
}

// A non-positive TTL must never reach EXPIRE: Valkey treats EXPIRE key 0 as an
// immediate delete, so a misconfigured tier would evict on every slide — the
// exact inverse of re-arming.
func TestSlideTTL_NonPositiveTTLNeverExpiresTheKey(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		c := &slideClient{found: true}
		SlideTTL(context.Background(), c, "k1", ttl, "test")
		assert.Empty(t, c.expired, "a non-positive TTL must not issue EXPIRE (it would delete the key)")
	}
}

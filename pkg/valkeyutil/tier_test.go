package valkeyutil

import (
	"context"
	"errors"
	"strconv"
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

// Callers hand over any key set, however its keys hash. Spreading them across
// cluster slots belongs to the client (clusterClient.Del pipelines one DEL per
// key), so BustKeys must not re-derive it — a caller that splits the keys
// itself pays a round trip per group for nothing, which is what the three
// hand-rolled obediences in this repo did.
func TestBustKeys_PassesMixedSlotKeysStraightThrough(t *testing.T) {
	tests := []struct {
		name string
		keys []string
	}{
		{"one shared hash tag", []string{"sub:{r1}:alice", "sub:{r1}:bob"}},
		{"untagged keys", []string{"user:id:u1", "user:acct:alice"}},
		{"several tags interleaved", []string{"sub:{r1}:alice", "sub:{r2}:bob", "sub:{r1}:carol"}},
		{"tagged alongside untagged", []string{"room:{r1}:meta:v2", "room:v3:r1:subs"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &bustClient{}
			BustKeys(context.Background(), c, "test", tt.keys...)
			assert.Equal(t, [][]string{tt.keys}, c.calls, "one call, keys unreordered")
		})
	}
}

// A failing batch must not skip the ones after it.
func TestBustKeys_OneFailingBatchDoesNotSkipTheOthers(t *testing.T) {
	c := &bustClient{err: errors.New("valkey down")}
	keys := make([]string, bustBatchSize+1)
	for i := range keys {
		keys[i] = "user:acct:" + strconv.Itoa(i)
	}

	BustKeys(context.Background(), c, "test", keys...)

	assert.Len(t, c.calls, 2, "every batch must be attempted")
}

func TestBustKeys_SwallowsFailures(t *testing.T) {
	c := &bustClient{err: errors.New("valkey down")}
	require.NotPanics(t, func() { BustKeys(context.Background(), c, "test", "k1") })
}

// A room-wide bust can name one key per member per generation. Without a cap it
// would build one pipeline proportional to room size.
func TestBustKeys_SplitsALargeKeySetIntoBoundedBatches(t *testing.T) {
	c := &bustClient{}
	keys := make([]string, 0, bustBatchSize*2+5)
	for i := range cap(keys) {
		keys = append(keys, "sub:{room1}:acct"+strconv.Itoa(i))
	}

	BustKeys(context.Background(), c, "test", keys...)

	require.Len(t, c.calls, 3, "two full batches and the remainder")
	assert.Len(t, c.calls[0], bustBatchSize)
	assert.Len(t, c.calls[1], bustBatchSize)
	assert.Len(t, c.calls[2], 5)

	var got []string
	for _, b := range c.calls {
		got = append(got, b...)
	}
	assert.Equal(t, keys, got, "every key is deleted, in order, exactly once")
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

// setManyClient records the bulk write.
type setManyClient struct {
	Client
	calls [][]KV
	err   error
}

func (c *setManyClient) MSet(_ context.Context, entries []KV, _ time.Duration) error {
	c.calls = append(c.calls, entries)
	return c.err
}

// The point of the bulk write: a caller writing N entries pays one round trip,
// not N. Mention lists are attacker-influenced in size and sit on the message
// hot path, so the difference is not academic.
func TestSetMany_UsesOneRoundTripWhenTheClientCan(t *testing.T) {
	c := &setManyClient{}
	entries := []KV{{"user:id:u1", "a"}, {"user:acct:alice", "a"}, {"user:id:u2", "b"}}

	SetMany(context.Background(), c, entries, time.Minute, "user")

	require.Len(t, c.calls, 1, "one call regardless of key count or slot spread")
	assert.Equal(t, entries, c.calls[0])
}

// A failure is swallowed: a cache write is best-effort and the caller already
// has the value it was trying to store.
func TestSetMany_SwallowsFailures(t *testing.T) {
	require.NotPanics(t, func() {
		SetMany(context.Background(), &setManyClient{err: errors.New("valkey down")},
			[]KV{{"k1", "a"}}, time.Minute, "user")
	})
}

func TestSetMany_NilClientAndEmptyEntriesAreNoOps(t *testing.T) {
	require.NotPanics(t, func() {
		SetMany(context.Background(), nil, []KV{{"k1", "a"}}, time.Minute, "user")
	})
	c := &setManyClient{}
	SetMany(context.Background(), c, nil, time.Minute, "user")
	assert.Empty(t, c.calls, "an empty entry set must not round-trip")
}

// A non-positive TTL would store without expiry on the Set path and is almost
// certainly a misconfiguration, so it is refused rather than written.
func TestSetMany_RefusesANonPositiveTTL(t *testing.T) {
	c := &setManyClient{}
	SetMany(context.Background(), c, []KV{{"k1", "a"}}, 0, "user")
	assert.Empty(t, c.calls)
}

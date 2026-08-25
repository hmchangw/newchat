package roomtimescache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeValkey is an in-memory stand-in for the cluster client, with hooks for
// the failure modes the tier must swallow.
type fakeValkey struct {
	mu       sync.Mutex
	data     map[string]string
	expires  map[string]time.Duration
	getErr   error
	setErr   error
	setCalls int
	expCalls int
}

func newFakeValkey() *fakeValkey {
	return &fakeValkey{data: map[string]string{}, expires: map[string]time.Duration{}}
}

func (f *fakeValkey) Get(_ context.Context, key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.data[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeValkey) MGet(context.Context, []string) (map[string]string, error) { return nil, nil }

func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setCalls++
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
}

func (f *fakeValkey) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}
func (f *fakeValkey) IncrEx(context.Context, string, time.Duration) (int64, error) { return 0, nil }

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}

func (f *fakeValkey) Expire(_ context.Context, key string, ttl time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expCalls++
	if _, ok := f.data[key]; !ok {
		return false, nil
	}
	f.expires[key] = ttl
	return true, nil
}

func (f *fakeValkey) Close() error { return nil }

func (f *fakeValkey) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[key]
	return ok
}

const ttl = 90 * time.Minute

var created = time.UnixMilli(1_600_000_000_000).UTC()

func TestKey_IsHashTaggedOnTheRoom(t *testing.T) {
	// The room hash-tag colocates this entry with the room's other cached
	// state, so a multi-key delete over one room can never go CROSSSLOT.
	assert.Equal(t, "roomtimes:{r1}", Key("r1"))
}

func TestTier_StoreThenFallback_RoundTrips(t *testing.T) {
	fv := newFakeValkey()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", created)

	got, found := tier.Fallback(context.Background(), "r1")
	require.True(t, found)
	assert.Equal(t, created, got)
}

// The tier caches createdAt and nothing else. rooms.lastMsgAt is projected by
// unread-worker on a consumer separate from the message-worker that writes to
// Cassandra, with MaxDeliver=-1 holding batches un-acked through a Mongo outage,
// so it lags Cassandra by an unbounded amount and can bound no read of it.
// Storing it here would only put it within reach of a caller who assumes it can.
func TestTier_Store_CachesOnlyTheImmutableTime(t *testing.T) {
	fv := newFakeValkey()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", created)

	assert.NotContains(t, fv.data[Key("r1")], "lastMsgAt")
}

// A room with no createdAt recorded bounds nothing, so it must not read as a
// hit — that would hand the caller a zero indistinguishable from no entry.
func TestTier_StoreThenFallback_ZeroCreatedAtIsNotAHit(t *testing.T) {
	fv := newFakeValkey()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", time.Time{})

	_, found := tier.Fallback(context.Background(), "r1")
	assert.False(t, found)
}

func TestTier_Fallback_MissReportsNotFound(t *testing.T) {
	tier := NewTier(newFakeValkey(), ttl, nil)

	_, found := tier.Fallback(context.Background(), "never-seen")

	assert.False(t, found)
}

// Serving a fallback is exactly the moment the source of truth is down, so the
// entry must outlive the outage. EXPIRE, never SET: a bust landing between the
// GET and this call must stay busted.
func TestTier_Fallback_ReArmsTheDeadlineWithoutRewriting(t *testing.T) {
	fv := newFakeValkey()
	tier := NewTier(fv, ttl, nil)
	tier.Store(context.Background(), "r1", created)
	writesAfterStore := fv.setCalls

	_, found := tier.Fallback(context.Background(), "r1")

	require.True(t, found)
	assert.Equal(t, writesAfterStore, fv.setCalls, "a fallback must not rewrite the entry")
	assert.Equal(t, 1, fv.expCalls, "it re-arms the deadline instead")
	assert.Equal(t, ttl, fv.expires[Key("r1")])
}

// Every Valkey failure mode degrades to "no floor", never to an error: the
// caller is already on its fallback path and must not be given a second one.
func TestTier_ToleratesEveryValkeyFailure(t *testing.T) {
	tests := []struct {
		name string
		tier func() *Tier
	}{
		{"nil client", func() *Tier { return NewTier(nil, ttl, nil) }},
		{"non-positive ttl disables the tier", func() *Tier { return NewTier(newFakeValkey(), 0, nil) }},
		{"get fails", func() *Tier {
			fv := newFakeValkey()
			fv.getErr = errors.New("valkey down")
			return NewTier(fv, ttl, nil)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tier := tt.tier()
			assert.NotPanics(t, func() { tier.Store(context.Background(), "r1", created) })
			_, found := tier.Fallback(context.Background(), "r1")
			assert.False(t, found)
		})
	}
}

// A malformed entry — a foreign value under the same key, or one written by an
// older build — must read as "no entry", not as a zero masquerading as real.
func TestTier_Fallback_TreatsACorruptEntryAsAbsent(t *testing.T) {
	fv := newFakeValkey()
	fv.data[Key("r1")] = "{not json"
	tier := NewTier(fv, ttl, nil)

	_, found := tier.Fallback(context.Background(), "r1")

	assert.False(t, found)
}

// An entry left by the build that also cached lastMsgAt still reads: the field
// it no longer carries is simply ignored, and its createdAt is as good as ever.
func TestTier_Fallback_ReadsAnEntryFromTheOlderWireForm(t *testing.T) {
	fv := newFakeValkey()
	fv.data[Key("r1")] = `{"lastMsgAt":1700000000000,"createdAt":1600000000000}`
	tier := NewTier(fv, ttl, nil)

	got, found := tier.Fallback(context.Background(), "r1")

	require.True(t, found)
	assert.Equal(t, created, got)
}

func TestTier_Store_SwallowsWriteFailure(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	tier := NewTier(fv, ttl, nil)

	// The caller already has the authoritative answer; a failed populate costs
	// only the next outage's floor.
	assert.NotPanics(t, func() { tier.Store(context.Background(), "r1", created) })
	assert.False(t, fv.has(Key("r1")))
}

// The tier is written only from a confirmed source-of-truth read and is never
// consulted while that source is healthy, so a stale entry cannot influence a
// healthy answer — which is why it needs no invalidation and does not join the
// cache-fill-after-invalidation race (#336).
func TestTier_DisabledTierNeverWrites(t *testing.T) {
	fv := newFakeValkey()
	tier := NewTier(fv, 0, nil)

	tier.Store(context.Background(), "r1", created)

	assert.Zero(t, fv.setCalls)
}

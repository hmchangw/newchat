package roomtimescache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyfake"
)

const ttl = 90 * time.Minute

var created = time.UnixMilli(1_600_000_000_000).UTC()

func TestKey_IsHashTaggedOnTheRoom(t *testing.T) {
	// The room hash-tag colocates this entry with the room's other cached
	// state, so a multi-key delete over one room can never go CROSSSLOT.
	assert.Equal(t, "roomtimes:{r1}", Key("r1"))
}

func TestTier_StoreThenFallback_RoundTrips(t *testing.T) {
	fv := valkeyfake.New()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", created)

	got, found := tier.Fallback(context.Background(), "r1")
	require.True(t, found)
	assert.Equal(t, created, got)
}

// The tier caches createdAt and nothing else. rooms.lastMsgAt is projected by
// roomlist-worker on a consumer separate from the message-worker that writes to
// Cassandra, with MaxDeliver=-1 holding batches un-acked through a Mongo outage,
// so it lags Cassandra by an unbounded amount and can bound no read of it.
// Storing it here would only put it within reach of a caller who assumes it can.
func TestTier_Store_CachesOnlyTheImmutableTime(t *testing.T) {
	fv := valkeyfake.New()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", created)

	assert.NotContains(t, fv.Value(Key("r1")), "lastMsgAt")
}

// A room with no createdAt recorded bounds nothing, so it must not read as a
// hit — that would hand the caller a zero indistinguishable from no entry.
func TestTier_StoreThenFallback_ZeroCreatedAtIsNotAHit(t *testing.T) {
	fv := valkeyfake.New()
	tier := NewTier(fv, ttl, nil)

	tier.Store(context.Background(), "r1", time.Time{})

	_, found := tier.Fallback(context.Background(), "r1")
	assert.False(t, found)
}

func TestTier_Fallback_MissReportsNotFound(t *testing.T) {
	tier := NewTier(valkeyfake.New(), ttl, nil)

	_, found := tier.Fallback(context.Background(), "never-seen")

	assert.False(t, found)
}

// Serving a fallback is exactly the moment the source of truth is down, so the
// entry must outlive the outage. EXPIRE, never SET: a bust landing between the
// GET and this call must stay busted.
func TestTier_Fallback_ReArmsTheDeadlineWithoutRewriting(t *testing.T) {
	fv := valkeyfake.New()
	tier := NewTier(fv, ttl, nil)
	tier.Store(context.Background(), "r1", created)
	writesAfterStore := fv.Calls().Set

	_, found := tier.Fallback(context.Background(), "r1")

	require.True(t, found)
	assert.Equal(t, writesAfterStore, fv.Calls().Set, "a fallback must not rewrite the entry")
	assert.Equal(t, 1, fv.Calls().Expire, "it re-arms the deadline instead")
	assert.Equal(t, ttl, mustTTL(t, fv, Key("r1")))
}

// Every Valkey failure mode degrades to "no floor", never to an error: the
// caller is already on its fallback path and must not be given a second one.
func TestTier_ToleratesEveryValkeyFailure(t *testing.T) {
	tests := []struct {
		name string
		tier func() *Tier
	}{
		{"nil client", func() *Tier { return NewTier(nil, ttl, nil) }},
		{"non-positive ttl disables the tier", func() *Tier { return NewTier(valkeyfake.New(), 0, nil) }},
		{"get fails", func() *Tier {
			fv := valkeyfake.New()
			fv.FailGet(errors.New("valkey down"))
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
	fv := valkeyfake.New()
	fv.Seed(Key("r1"), "{not json", ttl)
	tier := NewTier(fv, ttl, nil)

	_, found := tier.Fallback(context.Background(), "r1")

	assert.False(t, found)
}

// An entry left by the build that also cached lastMsgAt still reads: the field
// it no longer carries is simply ignored, and its createdAt is as good as ever.
func TestTier_Fallback_ReadsAnEntryFromTheOlderWireForm(t *testing.T) {
	fv := valkeyfake.New()
	fv.Seed(Key("r1"), `{"lastMsgAt":1700000000000,"createdAt":1600000000000}`, ttl)
	tier := NewTier(fv, ttl, nil)

	got, found := tier.Fallback(context.Background(), "r1")

	require.True(t, found)
	assert.Equal(t, created, got)
}

func TestTier_Store_SwallowsWriteFailure(t *testing.T) {
	fv := valkeyfake.New()
	fv.FailSet(errors.New("valkey down"))
	tier := NewTier(fv, ttl, nil)

	// The caller already has the authoritative answer; a failed populate costs
	// only the next outage's floor.
	assert.NotPanics(t, func() { tier.Store(context.Background(), "r1", created) })
	assert.False(t, fv.Has(Key("r1")))
}

// The tier is written only from a confirmed source-of-truth read and is never
// consulted while that source is healthy, so a stale entry cannot influence a
// healthy answer — which is why it needs no invalidation and does not join the
// cache-fill-after-invalidation race (#336).
func TestTier_DisabledTierNeverWrites(t *testing.T) {
	fv := valkeyfake.New()
	tier := NewTier(fv, 0, nil)

	tier.Store(context.Background(), "r1", created)

	assert.Zero(t, fv.Calls().Set)
}

// mustTTL reads a key's TTL, failing the test when the key is absent.
func mustTTL(t *testing.T, c *valkeyfake.Client, key string) time.Duration {
	t.Helper()
	d, ok := c.TTL(key)
	require.True(t, ok, "expected %s to be present", key)
	return d
}

package valkeyfake

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

// The fake must satisfy the interface it stands in for, or every migrated test
// package would find out separately.
var _ valkeyutil.Client = (*Client)(nil)

func TestClient_GetMiss(t *testing.T) {
	c := New()

	_, err := c.Get(context.Background(), "absent")

	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss, "an absent key is a miss, not a transport error")
}

func TestClient_SetThenGet(t *testing.T) {
	ctx := context.Background()
	c := New()

	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))

	got, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)
	ttl, ok := c.TTL("k")
	require.True(t, ok)
	assert.Equal(t, time.Minute, ttl)
}

func TestClient_MGet_ReturnsOnlyPresentKeys(t *testing.T) {
	ctx := context.Background()
	c := New()
	c.Seed("a", "1", time.Minute)
	c.Seed("c", "3", time.Minute)

	got, err := c.MGet(ctx, []string{"a", "b", "c"})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "c": "3"}, got, "an absent key is simply missing")
}

func TestClient_MGet_EmptyInput(t *testing.T) {
	got, err := New().MGet(context.Background(), nil)

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestClient_Del_RemovesKeysAndRecordsTheBatch(t *testing.T) {
	ctx := context.Background()
	c := New()
	c.Seed("a", "1", time.Minute)
	c.Seed("b", "2", time.Minute)

	require.NoError(t, c.Del(ctx, "a", "b", "absent"))

	assert.False(t, c.Has("a"))
	assert.False(t, c.Has("b"))
	assert.Equal(t, [][]string{{"a", "b", "absent"}}, c.DelBatches(),
		"batches are recorded per call, so a test can assert round-trip count")
	assert.Equal(t, 1, c.Calls().Del)
}

func TestClient_Del_EmptyIsANoOp(t *testing.T) {
	c := New()

	require.NoError(t, c.Del(context.Background()))

	assert.Empty(t, c.DelBatches())
	assert.Equal(t, 0, c.Calls().Del)
}

// Expire mirrors Valkey: it re-arms an existing key's deadline and reports
// whether the key was there, but never creates one. A fake that created the key
// would hide exactly the bug SlideTTL's EXPIRE-not-SET choice exists to avoid.
func TestClient_Expire(t *testing.T) {
	ctx := context.Background()
	c := New()
	c.Seed("present", "v", time.Minute)

	existed, err := c.Expire(ctx, "present", time.Hour)
	require.NoError(t, err)
	assert.True(t, existed)
	ttl, ok := c.TTL("present")
	require.True(t, ok)
	assert.Equal(t, time.Hour, ttl)

	existed, err = c.Expire(ctx, "absent", time.Hour)
	require.NoError(t, err)
	assert.False(t, existed)
	assert.False(t, c.Has("absent"), "EXPIRE must not create the key")
	assert.Equal(t, []string{"present", "absent"}, c.ExpiredKeys())
}

func TestClient_SetNX(t *testing.T) {
	ctx := context.Background()
	c := New()

	acquired, err := c.SetNX(ctx, "lock", "a", time.Minute)
	require.NoError(t, err)
	assert.True(t, acquired)

	acquired, err = c.SetNX(ctx, "lock", "b", time.Minute)
	require.NoError(t, err)
	assert.False(t, acquired, "a held key refuses")
	v := c.Value("lock")
	assert.Equal(t, "a", v, "a refused SetNX leaves the value alone")
}

func TestClient_IncrEx(t *testing.T) {
	ctx := context.Background()
	c := New()

	n, err := c.IncrEx(ctx, "count", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	ttl, ok := c.TTL("count")
	require.True(t, ok)
	assert.Equal(t, time.Minute, ttl, "the TTL applies on the 0->1 transition")

	n, err = c.IncrEx(ctx, "count", time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	ttl, _ = c.TTL("count")
	assert.Equal(t, time.Minute, ttl, "a later increment must not re-arm the window")
}

// Set keys are recorded even when Set is failing: a test asserting "nothing was
// cached" needs to see the attempt, and one asserting "Set never ran" needs the
// absence to be meaningful.
func TestClient_SetKeys_RecordedEvenWhenSetFails(t *testing.T) {
	ctx := context.Background()
	c := New()
	require.NoError(t, c.Set(ctx, "ok", "v", time.Minute))
	c.FailSet(errors.New("boom"))
	_ = c.Set(ctx, "failed", "v", time.Minute)

	assert.Equal(t, []string{"ok", "failed"}, c.SetKeys())
	assert.False(t, c.Has("failed"), "a failed Set still stores nothing")
}

// The bulk path exists to replace a per-key Get loop, so a test needs to see
// both how many batches were issued and how many keys each carried.
func TestClient_MGetBatches(t *testing.T) {
	ctx := context.Background()
	c := New()

	_, err := c.MGet(ctx, []string{"a", "b"})
	require.NoError(t, err)
	_, err = c.MGet(ctx, []string{"c"})
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b", "c"}, c.MGetKeys())
	assert.Equal(t, 2, c.Calls().MGet)
}

// MSet makes the fake satisfy valkeyutil's optional multiSetter, so a test
// exercises the one-round-trip bulk path production takes rather than silently
// falling back to SetMany's per-key loop.
func TestClient_MSet(t *testing.T) {
	ctx := context.Background()
	c := New()

	require.NoError(t, c.MSet(ctx, []valkeyutil.KV{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}, time.Minute))

	assert.Equal(t, "1", c.Value("a"))
	assert.Equal(t, "2", c.Value("b"))
	ttl, ok := c.TTL("a")
	require.True(t, ok)
	assert.Equal(t, time.Minute, ttl)
	assert.Equal(t, [][]valkeyutil.KV{{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}}, c.MSetBatches(),
		"one batch per call, so a test can tell one bulk write from a loop")
	assert.Zero(t, c.Calls().Set, "the bulk path must not count as per-key Sets")
}

func TestClient_MSet_FailureLeavesTheStoreUntouched(t *testing.T) {
	boom := errors.New("boom")
	c := New()
	c.FailSet(boom)

	assert.ErrorIs(t, c.MSet(context.Background(), []valkeyutil.KV{{Key: "a", Value: "1"}}, time.Minute), boom)

	assert.False(t, c.Has("a"))
	assert.Len(t, c.MSetBatches(), 1, "a failed bulk write is still an attempt")
}

func TestClient_FailureInjection(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	t.Run("get", func(t *testing.T) {
		c := New()
		c.Seed("k", "v", time.Minute)
		c.FailGet(boom)
		_, err := c.Get(ctx, "k")
		assert.ErrorIs(t, err, boom)
	})
	t.Run("mget", func(t *testing.T) {
		c := New()
		c.FailMGet(boom)
		_, err := c.MGet(ctx, []string{"k"})
		assert.ErrorIs(t, err, boom)
	})
	t.Run("set leaves the store untouched", func(t *testing.T) {
		c := New()
		c.FailSet(boom)
		assert.ErrorIs(t, c.Set(ctx, "k", "v", time.Minute), boom)
		assert.False(t, c.Has("k"))
	})
	t.Run("del leaves the store untouched", func(t *testing.T) {
		c := New()
		c.Seed("k", "v", time.Minute)
		c.FailDel(boom)
		assert.ErrorIs(t, c.Del(ctx, "k"), boom)
		assert.True(t, c.Has("k"), "a failed DEL must not have deleted anything")
	})
	t.Run("expire", func(t *testing.T) {
		c := New()
		c.Seed("k", "v", time.Minute)
		c.FailExpire(boom)
		_, err := c.Expire(ctx, "k", time.Hour)
		assert.ErrorIs(t, err, boom)
	})
}

// A tier slides or busts on a context derived with WithoutCancel, and the only
// way to prove that is to observe the context the call actually runs under.
func TestClient_OnDel_ObservesTheContext(t *testing.T) {
	c := New()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	var sawErr error
	c.OnDel(func(ctx context.Context) { sawErr = ctx.Err() })
	require.NoError(t, c.Del(context.WithoutCancel(cancelled), "k"))

	assert.NoError(t, sawErr, "WithoutCancel must strip the caller's cancellation")
}

// AfterGet lets a test interleave a write between a read and whatever the
// caller does with the result — the fill-racing-an-invalidation shape.
func TestClient_AfterGet_RunsBetweenReadAndReturn(t *testing.T) {
	c := New()
	c.Seed("k", "v", time.Minute)
	c.AfterGet(func(string) { c.Seed("k", "raced", time.Minute) })

	got, err := c.Get(context.Background(), "k")

	require.NoError(t, err)
	assert.Equal(t, "v", got, "the read returns what it read, not the interleaved write")
	v := c.Value("k")
	assert.Equal(t, "raced", v)
}

// With a clock injected, an entry past its deadline reads as a miss, so a test
// can exercise the outage path where an entry lapses.
func TestClient_WithClock_ExpiresEntries(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := New()
	c.SetClock(func() time.Time { return now })
	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))

	got, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	now = now.Add(2 * time.Minute)
	_, err = c.Get(ctx, "k")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

// SetClock installs the clock after construction, for a test whose helper is
// handed an already-built client and a clock together.
func TestClient_SetClock_AppliesToLaterWrites(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := New()
	c.SetClock(func() time.Time { return now })

	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))
	deadline, ok := c.Deadline("k")
	require.True(t, ok)
	assert.Equal(t, now.Add(time.Minute), deadline)

	now = now.Add(2 * time.Minute)
	_, err := c.Get(ctx, "k")
	assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
}

// A slide must move the absolute deadline, not just the recorded TTL — that is
// the whole point of re-arming an entry mid-outage.
func TestClient_Deadline_MovesOnExpire(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := New()
	c.SetClock(func() time.Time { return now })
	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))
	first, _ := c.Deadline("k")

	now = now.Add(30 * time.Second)
	_, err := c.Expire(ctx, "k", time.Hour)
	require.NoError(t, err)

	moved, ok := c.Deadline("k")
	require.True(t, ok)
	assert.True(t, moved.After(first), "the deadline must move past the original expiry")
}

// Without a clock there is no deadline to report.
func TestClient_Deadline_AbsentWithoutClock(t *testing.T) {
	c := New()
	c.Seed("k", "v", time.Minute)

	_, ok := c.Deadline("k")

	assert.False(t, ok)
}

// Without a clock the fake never expires anything: most tiers drive staleness
// from the stamp inside the cached value and inject their own clock there.
func TestClient_WithoutClock_NeverExpires(t *testing.T) {
	ctx := context.Background()
	c := New()
	require.NoError(t, c.Set(ctx, "k", "v", time.Nanosecond))

	got, err := c.Get(ctx, "k")

	require.NoError(t, err)
	assert.Equal(t, "v", got)
}

func TestClient_Keys_AreSorted(t *testing.T) {
	c := New()
	c.Seed("c", "3", time.Minute)
	c.Seed("a", "1", time.Minute)
	c.Seed("b", "2", time.Minute)

	assert.Equal(t, []string{"a", "b", "c"}, c.Keys(), "sorted, so assertions are deterministic")
}

// Every migrated package runs under -race, and several drive the fake from
// concurrent goroutines.
func TestClient_ConcurrentUse(t *testing.T) {
	ctx := context.Background()
	c := New()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := string(rune('a' + i%26))
			_ = c.Set(ctx, key, "v", time.Minute)
			_, _ = c.Get(ctx, key)
			_, _ = c.MGet(ctx, []string{key})
			_, _ = c.Expire(ctx, key, time.Hour)
			_ = c.Del(ctx, key)
			c.Keys()
			c.Calls()
			c.DelBatches()
		}()
	}
	wg.Wait()
}

func TestClient_Close(t *testing.T) {
	c := New()

	require.NoError(t, c.Close())
}

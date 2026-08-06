package bucketcache

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeValkey is an in-memory valkeyutil.Client with per-op error hooks.
type fakeValkey struct {
	mu     sync.Mutex
	data   map[string]string
	dels   []string
	getErr error
	setErr error
	delErr error
}

func newFakeValkey() *fakeValkey { return &fakeValkey{data: map[string]string{}} }

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

func (f *fakeValkey) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
}

func (f *fakeValkey) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	return true, nil
}

func (f *fakeValkey) IncrEx(_ context.Context, _ string, _ time.Duration) (int64, error) {
	return 0, nil
}

func (f *fakeValkey) Del(_ context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dels = append(f.dels, keys...)
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.data, k)
	}
	return nil
}

func (f *fakeValkey) Close() error { return nil }

func (f *fakeValkey) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[key]
	return ok
}

func bucketOf(msg string) []models.Message {
	return []models.Message{{
		RoomID:    "r1",
		MessageID: "m1",
		Msg:       msg,
		CreatedAt: time.Unix(0, 0).UTC(),
	}}
}

func newCache(t *testing.T, fv valkeyutil.Client) *Cache {
	t.Helper()
	c, err := NewCache(fv, 1<<20, time.Minute)
	require.NoError(t, err)
	return c
}

func TestKey_Format(t *testing.T) {
	assert.Equal(t, "hist:{r1}:bkt:864000000", Key("r1", 864000000))
}

func TestNewCache_InvalidArgs(t *testing.T) {
	_, err := NewCache(newFakeValkey(), 0, time.Minute)
	require.Error(t, err)
	_, err = NewCache(newFakeValkey(), 10, 0)
	require.Error(t, err)
}

func TestCache_MissThenPutThenGet_CopySafe(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	_, ok := c.Get(ctx, "r1", 100)
	assert.False(t, ok, "cold get is a miss")

	c.Put(ctx, "r1", 100, bucketOf("orig"))

	got1, ok := c.Get(ctx, "r1", 100)
	require.True(t, ok)
	require.Len(t, got1, 1)
	assert.Equal(t, "orig", got1[0].Msg)

	// Mutate the returned slice in place (as the service layer's redaction would).
	got1[0].Msg = "MUTATED"

	got2, ok := c.Get(ctx, "r1", 100)
	require.True(t, ok)
	require.Len(t, got2, 1)
	assert.Equal(t, "orig", got2[0].Msg, "each Get must return a fresh copy")
}

func TestCache_L2Hit_PopulatesL1(t *testing.T) {
	fv := newFakeValkey() // shared L2
	ctx := context.Background()

	c1 := newCache(t, fv)
	c1.Put(ctx, "r1", 100, bucketOf("orig"))

	// Second instance: cold L1, shares L2 → hits.
	c2 := newCache(t, fv)
	got, ok := c2.Get(ctx, "r1", 100)
	require.True(t, ok, "served from shared L2")
	assert.Equal(t, "orig", got[0].Msg)

	// Prove c2's L1 was populated: wipe L2, c2 still serves from L1.
	fv.mu.Lock()
	fv.data = map[string]string{}
	fv.mu.Unlock()
	got2, ok := c2.Get(ctx, "r1", 100)
	require.True(t, ok, "served from L1 after L2 wiped")
	assert.Equal(t, "orig", got2[0].Msg)
}

func TestCache_Bust_RemovesEntry(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	require.True(t, fv.has(Key("r1", 100)))

	c.Bust(ctx, "r1", 100)
	assert.False(t, fv.has(Key("r1", 100)), "L2 entry deleted")
	assert.Equal(t, []string{Key("r1", 100)}, fv.dels)

	_, ok := c.Get(ctx, "r1", 100)
	assert.False(t, ok, "L1 entry removed too")
}

func TestCache_FailOpen_GetError(t *testing.T) {
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey down")
	c := newCache(t, fv)

	_, ok := c.Get(context.Background(), "r1", 100)
	assert.False(t, ok, "a Valkey GET error degrades to a miss, no panic")
}

func TestCache_FailOpen_SetError(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	c := newCache(t, fv)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.Put(ctx, "r1", 100, bucketOf("orig")) })

	// L1 is independent, so it still serves despite the L2 write failing.
	got, ok := c.Get(ctx, "r1", 100)
	require.True(t, ok)
	assert.Equal(t, "orig", got[0].Msg)
}

func TestCache_FailOpen_DelError(t *testing.T) {
	fv := newFakeValkey()
	fv.delErr = errors.New("valkey down")
	c := newCache(t, fv)
	ctx := context.Background()

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	assert.NotPanics(t, func() { c.Bust(ctx, "r1", 100) })

	// The DEL failed, so the L2 entry survives (fail-open: the TTL reconciles a
	// missed bust). The bust is swallowed, not fatal, and the read still works.
	assert.True(t, fv.has(Key("r1", 100)), "a failed DEL leaves the L2 entry for the TTL to reap")
	got, ok := c.Get(ctx, "r1", 100)
	require.True(t, ok, "still served from L2 after a failed bust")
	assert.Equal(t, "orig", got[0].Msg)
}

func TestCache_NilClient_Noops(t *testing.T) {
	c, err := NewCache(nil, 1<<20, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.Put(ctx, "r1", 100, bucketOf("orig")) })
	assert.NotPanics(t, func() { c.Bust(ctx, "r1", 100) })
	_, ok := c.Get(ctx, "r1", 100)
	assert.False(t, ok, "nil client is a permanent miss")
}

// TestCache_ByteBoundedEviction verifies L1 evicts least-recently-used buckets
// to stay under the byte budget (not an entry count). Uses a nil Valkey so Get
// reflects L1 alone.
func TestCache_ByteBoundedEviction(t *testing.T) {
	ctx := context.Background()
	blob, err := encode(bucketOf("payload"))
	require.NoError(t, err)
	s := int64(len(blob))

	c, err := NewCache(nil, 2*s, time.Minute) // budget holds exactly two buckets
	require.NoError(t, err)

	c.Put(ctx, "r", 1, bucketOf("payload"))
	c.Put(ctx, "r", 2, bucketOf("payload"))
	require.Equal(t, 2*s, c.curBytes.Load(), "two buckets fill the budget exactly")

	c.Put(ctx, "r", 3, bucketOf("payload")) // over budget → evict the oldest (bucket 1)
	assert.Equal(t, 2*s, c.curBytes.Load(), "curBytes stays at the budget after eviction")
	assert.LessOrEqual(t, c.curBytes.Load(), c.maxBytes)

	_, ok := c.Get(ctx, "r", 1)
	assert.False(t, ok, "least-recently-used bucket was evicted")
	_, ok = c.Get(ctx, "r", 2)
	assert.True(t, ok)
	_, ok = c.Get(ctx, "r", 3)
	assert.True(t, ok)
}

// TestCache_ByteAccounting verifies the counter stays exact across a re-Put of
// the same key (no double-count) and a Bust (returns to zero).
func TestCache_ByteAccounting(t *testing.T) {
	ctx := context.Background()
	small, err := encode(bucketOf("s"))
	require.NoError(t, err)
	big, err := encode(bucketOf("a-much-longer-payload-string"))
	require.NoError(t, err)

	c, err := NewCache(nil, 1<<20, time.Minute)
	require.NoError(t, err)

	c.Put(ctx, "r", 1, bucketOf("s"))
	require.Equal(t, int64(len(small)), c.curBytes.Load())

	// Re-Put the same key with larger content: the counter reflects the new size
	// only (the old entry is removed first, not replaced in place).
	c.Put(ctx, "r", 1, bucketOf("a-much-longer-payload-string"))
	require.Equal(t, int64(len(big)), c.curBytes.Load(), "re-Put must not double-count")

	c.Bust(ctx, "r", 1)
	require.Equal(t, int64(0), c.curBytes.Load(), "bust returns the counter to zero")
}

// TestCache_ByteAccounting_ConsistentUnderConcurrency runs concurrent Puts/Gets
// (with -race) and confirms the atomic byte counter never drifts from the actual
// bytes held in L1.
func TestCache_ByteAccounting_ConsistentUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	c, err := NewCache(nil, 1<<20, time.Minute)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := int64(i % 8)
			c.Put(ctx, "r", b, bucketOf("payload"))
			c.Get(ctx, "r", b)
		}(i)
	}
	wg.Wait()

	var actual int64
	for _, v := range c.l1.Values() {
		actual += int64(len(v))
	}
	assert.Equal(t, actual, c.curBytes.Load(), "byte counter must match the bytes actually held in L1")
}

// guards against an accidental key-format change.
func TestKey_HashTagStable(t *testing.T) {
	assert.Equal(t, "hist:{room-xyz}:bkt:-5", Key("room-xyz", -5))
	assert.Equal(t, "hist:{r}:bkt:"+strconv.FormatInt(1<<40, 10), Key("r", 1<<40))
}

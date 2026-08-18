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
	"github.com/hmchangw/chat/pkg/model/cassandra"
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

// Buckets are cached in Cassandra's sealed form, so the encoding must carry
// enc_payload and the enc_meta pointer through intact — a silent drop here would
// surface as undecryptable rows rather than as a cache miss.
func TestCache_RoundTripsSealedRows(t *testing.T) {
	c := newCache(t, newFakeValkey())
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	c.Put(ctx, "r1", 100, []models.Message{{
		MessageID:  "m1",
		RoomID:     "r1",
		CreatedAt:  at,
		EncPayload: []byte{0xDE, 0xAD, 0xBE, 0xEF},
		EncMeta:    &cassandra.EncMeta{Nonce: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	}})

	got, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res)
	require.Len(t, got, 1)
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, got[0].EncPayload)
	require.NotNil(t, got[0].EncMeta)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, got[0].EncMeta.Nonce)
}

func TestKey_Format(t *testing.T) {
	assert.Equal(t, "hist:{r1}:bkt:864000000", Key("r1", 864000000))
}

func TestNewCache_InvalidArgs(t *testing.T) {
	// A TTL in (0, minTTL) is the sharp edge: golang-lru remaps ttl <= 0 to its
	// no-eviction sentinel, but keeps a sub-100ns ttl and starts the reaper with
	// time.NewTicker(ttl / 100), which truncates to 0 and panics.
	tests := []struct {
		name     string
		maxBytes int64
		ttl      time.Duration
		wantErr  bool
	}{
		{name: "zero max bytes", maxBytes: 0, ttl: time.Minute, wantErr: true},
		{name: "negative max bytes", maxBytes: -1, ttl: time.Minute, wantErr: true},
		{name: "zero ttl", maxBytes: 10, ttl: 0, wantErr: true},
		{name: "negative ttl", maxBytes: 10, ttl: -time.Second, wantErr: true},
		{name: "ttl below reaper tick floor", maxBytes: 10, ttl: time.Nanosecond, wantErr: true},
		{name: "ttl one tick below floor", maxBytes: 10, ttl: 99 * time.Nanosecond, wantErr: true},
		{name: "ttl at reaper tick floor", maxBytes: 10, ttl: 100 * time.Nanosecond},
		{name: "ordinary ttl", maxBytes: 1 << 20, ttl: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCache(newFakeValkey(), tt.maxBytes, tt.ttl)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, c)
		})
	}
}

func TestCache_MissThenPutThenGet_CopySafe(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "cold get is a miss")

	c.Put(ctx, "r1", 100, bucketOf("orig"))

	got1, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res)
	require.Len(t, got1, 1)
	assert.Equal(t, "orig", got1[0].Msg)

	// Mutate the returned slice in place (as the service layer's redaction would).
	got1[0].Msg = "MUTATED"

	got2, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res)
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
	got, res := c2.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res, "served from shared L2")
	assert.Equal(t, "orig", got[0].Msg)

	// Prove c2's L1 was populated: wipe L2, c2 still serves from L1.
	fv.mu.Lock()
	fv.data = map[string]string{}
	fv.mu.Unlock()
	got2, res := c2.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res, "served from L1 after L2 wiped")
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

	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "L1 entry removed too")
}

func TestCache_FailOpen_GetError(t *testing.T) {
	fv := newFakeValkey()
	fv.getErr = errors.New("valkey down")
	c := newCache(t, fv)

	_, res := c.Get(context.Background(), "r1", 100)
	assert.Equal(t, Miss, res, "a Valkey GET error degrades to a miss, no panic")
}

func TestCache_FailOpen_SetError(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	c := newCache(t, fv)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.Put(ctx, "r1", 100, bucketOf("orig")) })

	// L1 is independent, so it still serves despite the L2 write failing.
	got, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res)
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
	got, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res, "still served from L2 after a failed bust")
	assert.Equal(t, "orig", got[0].Msg)
}

func TestCache_NilClient_Noops(t *testing.T) {
	c, err := NewCache(nil, 1<<20, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.Put(ctx, "r1", 100, bucketOf("orig")) })
	assert.NotPanics(t, func() { c.Bust(ctx, "r1", 100) })
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "nil client is a permanent miss")
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

	_, res := c.Get(ctx, "r", 1)
	assert.Equal(t, Miss, res, "least-recently-used bucket was evicted")
	_, res = c.Get(ctx, "r", 2)
	assert.Equal(t, Hit, res)
	_, res = c.Get(ctx, "r", 3)
	assert.Equal(t, Hit, res)
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

// An oversized bucket is never cached, so without a marker every read repeats
// the full-bucket probe that discovered it. The marker records that verdict so
// later reads go straight to the bounded live query.
func TestCache_PutOversized_ReportsOversized(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	_, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Miss, res, "cold get is a miss, not oversized")

	c.PutOversized(ctx, "r1", 100)

	msgs, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Oversized, res)
	assert.Nil(t, msgs, "an oversized marker carries no rows")
	assert.True(t, fv.has(Key("r1", 100)), "marker written to L2 under the bucket's own key")
}

// The marker must cross replicas, or every process pays the probe once per TTL.
func TestCache_PutOversized_SharedViaL2(t *testing.T) {
	fv := newFakeValkey()
	ctx := context.Background()

	c1 := newCache(t, fv)
	c1.PutOversized(ctx, "r1", 100)

	c2 := newCache(t, fv) // cold L1, shared L2
	_, res := c2.Get(ctx, "r1", 100)
	assert.Equal(t, Oversized, res, "marker served from shared L2")

	// L1 was populated too: wipe L2 and c2 still answers without a round trip.
	fv.mu.Lock()
	fv.data = map[string]string{}
	fv.mu.Unlock()
	_, res = c2.Get(ctx, "r1", 100)
	assert.Equal(t, Oversized, res, "marker served from L1 after L2 wiped")
}

func TestCache_FailOpen_PutOversizedSetError(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	c := newCache(t, fv)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.PutOversized(ctx, "r1", 100) })

	// L1 is independent of the failed L2 write, so this replica still skips the
	// probe; other replicas re-probe until one of them lands the marker.
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Oversized, res)
}

// A delete can drop a bucket back under the cap, so a bust must clear the
// verdict as well as any cached rows — otherwise the bucket stays uncacheable
// until the TTL expires.
func TestCache_Bust_ClearsOversizedMarker(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	c.PutOversized(ctx, "r1", 100)
	require.True(t, fv.has(Key("r1", 100)))

	c.Bust(ctx, "r1", 100)

	assert.False(t, fv.has(Key("r1", 100)), "L2 marker deleted")
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "bucket is re-probed after a bust")
}

// A marker replaces cached rows and vice versa: both live under one key, so the
// byte accounting must not double-count or strand the previous value.
func TestCache_OversizedMarker_ReplacesCachedRows(t *testing.T) {
	ctx := context.Background()
	c, err := NewCache(nil, 1<<20, time.Minute)
	require.NoError(t, err)

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	c.PutOversized(ctx, "r1", 100)

	_, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Oversized, res, "marker supersedes the cached rows")
	assert.Equal(t, int64(1), c.curBytes.Load(), "L1 holds the marker alone")

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	got, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res, "a later fill supersedes the marker")
	require.Len(t, got, 1)
	assert.Equal(t, "orig", got[0].Msg)
}

// Stored values are tagged so rows and markers are distinguishable. Anything
// else — a value written by an older build, or a truncated one — must degrade to
// a miss (fail-open), never an error or a bogus decode.
func TestCache_UnrecognizedStoredValue_IsMiss(t *testing.T) {
	ctx := context.Background()
	untagged, err := encode(bucketOf("orig"))
	require.NoError(t, err)

	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "unknown tag", value: "\xff garbage"},
		{name: "known tag, corrupt body", value: string(untagged[:1]) + "not-gob"},
		{name: "pre-tag raw gob", value: string(untagged[1:])},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fv := newFakeValkey()
			fv.data[Key("r1", 100)] = tt.value
			c := newCache(t, fv)

			msgs, res := c.Get(ctx, "r1", 100)
			assert.Equal(t, Miss, res)
			assert.Nil(t, msgs)
		})
	}
}

// guards against an accidental key-format change.
func TestKey_HashTagStable(t *testing.T) {
	assert.Equal(t, "hist:{room-xyz}:bkt:-5", Key("room-xyz", -5))
	assert.Equal(t, "hist:{r}:bkt:"+strconv.FormatInt(1<<40, 10), Key("r", 1<<40))
}

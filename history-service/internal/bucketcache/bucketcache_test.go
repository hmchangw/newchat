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
	c, err := NewCache(fv, time.Minute)
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
	tests := []struct {
		name    string
		ttl     time.Duration
		wantErr bool
	}{
		{name: "zero ttl", ttl: 0, wantErr: true},
		{name: "negative ttl", ttl: -time.Second, wantErr: true},
		{name: "ordinary ttl", ttl: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewCache(newFakeValkey(), tt.ttl)
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

// The cache has one shared tier and no per-replica copy, so a Bust by any
// instance is immediately visible to every other. This is the property the
// earlier L1 could not provide: a sibling replica went on serving pre-mutation
// rows from its own memory until that entry's TTL, whatever happened in Valkey.
func TestCache_BustIsVisibleToOtherInstances(t *testing.T) {
	fv := newFakeValkey() // the shared tier, standing in for one Valkey cluster
	ctx := context.Background()

	writer := newCache(t, fv) // the replica handling the mutation
	reader := newCache(t, fv) // a sibling replica serving reads

	writer.Put(ctx, "r1", 100, bucketOf("orig"))

	got, res := reader.Get(ctx, "r1", 100)
	require.Equal(t, Hit, res, "sibling serves the shared entry")
	require.Len(t, got, 1)
	assert.Equal(t, "orig", got[0].Msg)

	writer.Bust(ctx, "r1", 100)

	_, res = reader.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "the sibling must not keep serving the busted bucket")
}

func TestCache_Bust_RemovesEntry(t *testing.T) {
	fv := newFakeValkey()
	c := newCache(t, fv)
	ctx := context.Background()

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	require.True(t, fv.has(Key("r1", 100)))

	c.Bust(ctx, "r1", 100)
	assert.False(t, fv.has(Key("r1", 100)), "entry deleted")
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

	// With one shared tier there is no per-process copy to fall back on: a failed
	// write simply leaves the bucket uncached, and the next read goes live. The
	// cache degrades to absent, never to a divergent per-pod view.
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "a failed write must leave nothing cached")
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
	c, err := NewCache(nil, time.Minute)
	require.NoError(t, err)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.Put(ctx, "r1", 100, bucketOf("orig")) })
	assert.NotPanics(t, func() { c.Bust(ctx, "r1", 100) })
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "nil client is a permanent miss")
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
func TestCache_PutOversized_SharedAcrossReplicas(t *testing.T) {
	fv := newFakeValkey()
	ctx := context.Background()

	c1 := newCache(t, fv)
	c1.PutOversized(ctx, "r1", 100)

	c2 := newCache(t, fv) // a sibling replica
	_, res := c2.Get(ctx, "r1", 100)
	assert.Equal(t, Oversized, res, "the marker is shared, so one probe serves every replica")

	// Nothing survives outside the shared tier: wipe it and the verdict is gone.
	fv.mu.Lock()
	fv.data = map[string]string{}
	fv.mu.Unlock()
	_, res = c2.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "no per-process copy outlives the shared entry")
}

func TestCache_FailOpen_PutOversizedSetError(t *testing.T) {
	fv := newFakeValkey()
	fv.setErr = errors.New("valkey down")
	c := newCache(t, fv)
	ctx := context.Background()

	assert.NotPanics(t, func() { c.PutOversized(ctx, "r1", 100) })

	// The marker did not land, so the verdict is simply not recorded and the next
	// read re-probes. Costly, but never wrong.
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "an unrecorded verdict re-probes rather than being assumed")
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

	assert.False(t, fv.has(Key("r1", 100)), "marker deleted")
	_, res := c.Get(ctx, "r1", 100)
	assert.Equal(t, Miss, res, "bucket is re-probed after a bust")
}

// A marker replaces cached rows and vice versa: both live under one key.
func TestCache_OversizedMarker_ReplacesCachedRows(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, newFakeValkey())

	c.Put(ctx, "r1", 100, bucketOf("orig"))
	c.PutOversized(ctx, "r1", 100)

	_, res := c.Get(ctx, "r1", 100)
	require.Equal(t, Oversized, res, "marker supersedes the cached rows")

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

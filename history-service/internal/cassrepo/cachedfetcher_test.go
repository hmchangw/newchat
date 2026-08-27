package cassrepo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/models"
	"github.com/hmchangw/chat/pkg/atrest"
	"github.com/hmchangw/chat/pkg/bucketcache"
	"github.com/hmchangw/chat/pkg/model/cassandra"
	"github.com/hmchangw/chat/pkg/msgbucket"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// memValkey is a minimal in-memory valkeyutil.Client for wiring a real
// bucketcache.Cache in a unit test (no Cassandra, no container).
type memValkey struct{ data map[string]string }

func newMemValkey() *memValkey { return &memValkey{data: map[string]string{}} }

func (m *memValkey) Get(_ context.Context, k string) (string, error) {
	if v, ok := m.data[k]; ok {
		return v, nil
	}
	return "", valkeyutil.ErrCacheMiss
}
func (m *memValkey) Set(_ context.Context, k, v string, _ time.Duration) error {
	m.data[k] = v
	return nil
}
func (m *memValkey) SetNX(_ context.Context, k, v string, _ time.Duration) (bool, error) {
	m.data[k] = v
	return true, nil
}
func (m *memValkey) IncrEx(_ context.Context, _ string, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *memValkey) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		delete(m.data, k)
	}
	return nil
}
func (m *memValkey) Close() error { return nil }

const fetcherWindow = 72 * time.Hour

var fetcherNow = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)

// recordingLive is a fake live bucketFetcher that records the buckets it served
// and returns one message stamped with the bucket, so tests can tell live from
// cache and assert the hot/sealed routing.
type recordingLive struct{ served []int64 }

func (l *recordingLive) fetch() bucketFetcher[models.Message] {
	return func(_ context.Context, bucket int64, _ bool, _ []byte, _ int) (bucketPage[models.Message], error) {
		l.served = append(l.served, bucket)
		return bucketPage[models.Message]{rows: []models.Message{{MessageID: "live", RoomID: "r1", CreatedAt: fetcherNow}}}, nil
	}
}

func newCachedRepo(t *testing.T, mv valkeyutil.Client) *Repository {
	t.Helper()
	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	return NewRepository(nil, msgbucket.New(fetcherWindow), 122, nil,
		WithBucketCache(bc, 2000),
		withClock(func() time.Time { return fetcherNow }),
	)
}

// fakeCipher maps an enc_payload to a recognizable plaintext body so tests can
// tell a decrypted row from one still carrying ciphertext, and counts Decrypt
// calls so they can assert how many rows actually paid for decryption.
type fakeCipher struct {
	err   error
	calls int
}

func (c *fakeCipher) Encrypt(context.Context, string, atrest.EncryptedFields) ([]byte, atrest.EncMeta, error) {
	return nil, atrest.EncMeta{}, nil
}

func (c *fakeCipher) Decrypt(_ context.Context, _ string, encPayload []byte, _ atrest.EncMeta) (atrest.EncryptedFields, error) {
	c.calls++
	if c.err != nil {
		return atrest.EncryptedFields{}, c.err
	}
	return atrest.EncryptedFields{Msg: "plain(" + string(encPayload) + ")"}, nil
}

func (c *fakeCipher) EnsureDEK(context.Context, string) error { return nil }

func newCachedRepoWithCipher(t *testing.T, mv valkeyutil.Client, cipher atrest.Cipher) *Repository {
	t.Helper()
	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	return NewRepository(nil, msgbucket.New(fetcherWindow), 122, cipher,
		WithBucketCache(bc, 2000),
		withClock(func() time.Time { return fetcherNow }),
	)
}

// encRow builds a row in the shape Cassandra stores it: user-authored fields
// still sealed in enc_payload, clustering columns plaintext.
func encRow(id string, at time.Time, payload string) models.Message {
	return models.Message{
		MessageID:  id,
		RoomID:     "r1",
		CreatedAt:  at,
		EncPayload: []byte(payload),
		EncMeta:    &cassandra.EncMeta{Nonce: []byte("nonce")},
	}
}

// seedCache writes rows into the same (roomID, bucket) key the repository's
// cache reads, standing in for a previous fill.
func seedCache(t *testing.T, mv valkeyutil.Client, bucket int64, rows ...models.Message) {
	t.Helper()
	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	bc.Put(context.Background(), "r1", bucket, rows)
}

func TestCachedDescFetcher_DecryptsCiphertextFromCache(t *testing.T) {
	mv := newMemValkey()
	cipher := &fakeCipher{}
	r := newCachedRepoWithCipher(t, mv, cipher)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	seedCache(t, mv, sealed, encRow("a", t0, "A"), encRow("b", t0.Add(-time.Minute), "B"))

	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	page, err := f(context.Background(), sealed, true, nil, 20)
	require.NoError(t, err)
	require.Len(t, page.rows, 2)
	assert.Equal(t, "plain(A)", page.rows[0].Msg)
	assert.Equal(t, "plain(B)", page.rows[1].Msg)
	assert.Nil(t, page.rows[0].EncPayload, "ciphertext must not escape the cassrepo layer")
	assert.Nil(t, page.rows[0].EncMeta, "enc meta must not escape the cassrepo layer")
}

func TestCachedDescFetcher_DecryptsOnlyTheRowsItReturns(t *testing.T) {
	mv := newMemValkey()
	cipher := &fakeCipher{}
	r := newCachedRepoWithCipher(t, mv, cipher)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	seedCache(t, mv, sealed,
		encRow("a", t0, "A"),
		encRow("b", t0.Add(-time.Minute), "B"),
		encRow("c", t0.Add(-2*time.Minute), "C"),
	)

	// Bounds and limit are applied on the clustering column, which stays
	// plaintext — so only the page actually served should pay for decryption.
	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	page, err := f(context.Background(), sealed, true, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.rows, 1)
	assert.Equal(t, 1, cipher.calls, "a whole cached bucket must not be decrypted to serve one row")
}

func TestCachedDescFetcher_UnencryptedRowsPassThrough(t *testing.T) {
	mv := newMemValkey()
	cipher := &fakeCipher{}
	r := newCachedRepoWithCipher(t, mv, cipher)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	seedCache(t, mv, sealed, models.Message{MessageID: "a", RoomID: "r1", CreatedAt: t0, Msg: "clear"})

	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	page, err := f(context.Background(), sealed, true, nil, 20)
	require.NoError(t, err)
	require.Len(t, page.rows, 1)
	assert.Equal(t, "clear", page.rows[0].Msg)
	assert.Zero(t, cipher.calls, "rows with no enc_payload must not reach the cipher")
}

func TestCachedDescFetcher_DecryptError_Propagates(t *testing.T) {
	mv := newMemValkey()
	cipher := &fakeCipher{err: errors.New("dek unavailable")}
	r := newCachedRepoWithCipher(t, mv, cipher)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	seedCache(t, mv, sealed, encRow("a", t0, "A"))

	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	_, err := f(context.Background(), sealed, true, nil, 20)
	require.Error(t, err, "a failed decrypt must surface, not serve an empty body")
}

func TestCachedDescFetcher_HotBucket_ServesLive(t *testing.T) {
	r := newCachedRepo(t, newMemValkey())
	live := &recordingLive{}
	sizer := msgbucket.New(fetcherWindow)
	current := sizer.Of(fetcherNow)

	f := r.cachedDescFetcher("r1", fetcherNow, nil, current, live.fetch())
	page, err := f(context.Background(), current, true, nil, 20)
	require.NoError(t, err)
	require.Len(t, page.rows, 1)
	assert.Equal(t, "live", page.rows[0].MessageID)
	assert.Equal(t, []int64{current}, live.served, "current bucket must be served live")
}

func TestCachedDescFetcher_SealedBucket_ServesFromCacheAndFilters(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow)) // one window back → sealed

	// Pre-populate the bucket with three DESC-ordered messages.
	t0 := fetcherNow.Add(-73 * time.Hour)
	t1 := t0.Add(-time.Minute)
	t2 := t1.Add(-time.Minute)
	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	bc.Put(context.Background(), "r1", sealed, []models.Message{
		{MessageID: "a", RoomID: "r1", CreatedAt: t0},
		{MessageID: "b", RoomID: "r1", CreatedAt: t1},
		{MessageID: "c", RoomID: "r1", CreatedAt: t2},
	})

	live := &recordingLive{}
	// before excludes t0 (created_at < t0 keeps b, c); firstBucket applies it.
	f := r.cachedDescFetcher("r1", t0, nil, sealed, live.fetch())
	page, err := f(context.Background(), sealed, true, nil, 20)
	require.NoError(t, err)
	assert.Nil(t, page.resumeState, "cached bucket returns nil resume state (LoadHistory re-anchors by timestamp)")
	assert.Empty(t, live.served, "a sealed cache hit must not call the live fetcher")
	require.Len(t, page.rows, 2)
	assert.Equal(t, "b", page.rows[0].MessageID)
	assert.Equal(t, "c", page.rows[1].MessageID)
}

func TestCachedDescFetcher_SealedBucket_RespectsRemaining(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	bc.Put(context.Background(), "r1", sealed, []models.Message{
		{MessageID: "a", RoomID: "r1", CreatedAt: t0},
		{MessageID: "b", RoomID: "r1", CreatedAt: t0.Add(-time.Minute)},
		{MessageID: "c", RoomID: "r1", CreatedAt: t0.Add(-2 * time.Minute)},
	})

	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	page, err := f(context.Background(), sealed, true, nil, 2) // limit=2
	require.NoError(t, err)
	require.Len(t, page.rows, 2, "must not return more than `limit` rows")
	assert.Equal(t, "a", page.rows[0].MessageID)
	assert.Equal(t, "b", page.rows[1].MessageID)
}

// A bucket already known to be too dense to cache must be served straight from
// the live fetcher: no full-bucket probe, no second Cassandra round trip. The
// repository is built with a nil session, so any probe would panic — reaching
// the live fetcher at all is the assertion.
func TestCachedDescFetcher_KnownOversized_SkipsProbe(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	bc, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	bc.PutOversized(context.Background(), "r1", sealed)

	live := &recordingLive{}
	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, live.fetch())
	page, err := f(context.Background(), sealed, true, nil, 20)

	require.NoError(t, err)
	require.Len(t, page.rows, 1)
	assert.Equal(t, "live", page.rows[0].MessageID)
	assert.Equal(t, []int64{sealed}, live.served, "a known-oversized bucket goes straight to the live query")
}

func TestBustBucket_EvictsMessageBucket(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	ctx := context.Background()
	createdAt := fetcherNow.Add(-100 * time.Hour)
	bucket := msgbucket.New(fetcherWindow).Of(createdAt)

	seed, err := bucketcache.NewCache(mv, time.Minute)
	require.NoError(t, err)
	seed.Put(ctx, "r1", bucket, []models.Message{{MessageID: "x", RoomID: "r1", CreatedAt: createdAt}})
	require.Contains(t, mv.data, bucketcache.Key("r1", bucket))

	r.bustBucket(ctx, "r1", createdAt)
	assert.NotContains(t, mv.data, bucketcache.Key("r1", bucket), "bustBucket must DEL the message's bucket")
}

func TestBustBucket_NilCache_NoPanic(t *testing.T) {
	r := NewRepository(nil, msgbucket.New(fetcherWindow), 122, nil)
	assert.NotPanics(t, func() { r.bustBucket(context.Background(), "r1", fetcherNow) })
}

func TestHasResumeCursor(t *testing.T) {
	tests := []struct {
		name    string
		pageReq PageRequest
		want    bool
	}{
		{"nil cursor", PageRequest{Cursor: nil}, false},
		{"empty cursor", PageRequest{Cursor: &Cursor{}}, false},
		{"cursor with state", PageRequest{Cursor: &Cursor{state: []byte{0x01, 0x02}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, hasResumeCursor(tt.pageReq))
		})
	}
}

func TestCachedDescFetcher_NilCache_AlwaysLive(t *testing.T) {
	r := NewRepository(nil, msgbucket.New(fetcherWindow), 122, nil,
		withClock(func() time.Time { return fetcherNow }))
	live := &recordingLive{}
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	// With no cache, cachedDescFetcher returns the live fetcher unchanged.
	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, live.fetch())
	_, err := f(context.Background(), sealed, true, nil, 20)
	require.NoError(t, err)
	assert.Equal(t, []int64{sealed}, live.served, "no cache → sealed bucket served live")
}

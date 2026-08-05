package cassrepo

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/history-service/internal/bucketcache"
	"github.com/hmchangw/chat/history-service/internal/models"
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
	return func(_ context.Context, bucket int64, _ bool, _ int, _ []byte) ([]models.Message, []byte, error) {
		l.served = append(l.served, bucket)
		return []models.Message{{MessageID: "live", RoomID: "r1", CreatedAt: fetcherNow}}, nil, nil
	}
}

func newCachedRepo(t *testing.T, mv valkeyutil.Client) *Repository {
	t.Helper()
	bc, err := bucketcache.NewCache(mv, 1000, time.Minute)
	require.NoError(t, err)
	return NewRepository(nil, msgbucket.New(fetcherWindow), 122, nil,
		WithBucketCache(bc, 2000),
		withClock(func() time.Time { return fetcherNow }),
	)
}

func TestCachedDescFetcher_HotBucket_ServesLive(t *testing.T) {
	r := newCachedRepo(t, newMemValkey())
	live := &recordingLive{}
	sizer := msgbucket.New(fetcherWindow)
	current := sizer.Of(fetcherNow)

	f := r.cachedDescFetcher("r1", fetcherNow, nil, current, live.fetch())
	rows, _, err := f(context.Background(), current, true, 20, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "live", rows[0].MessageID)
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
	bc, err := bucketcache.NewCache(mv, 1000, time.Minute)
	require.NoError(t, err)
	bc.Put(context.Background(), "r1", sealed, []models.Message{
		{MessageID: "a", RoomID: "r1", CreatedAt: t0},
		{MessageID: "b", RoomID: "r1", CreatedAt: t1},
		{MessageID: "c", RoomID: "r1", CreatedAt: t2},
	})

	live := &recordingLive{}
	// before excludes t0 (created_at < t0 keeps b, c); firstBucket applies it.
	f := r.cachedDescFetcher("r1", t0, nil, sealed, live.fetch())
	rows, next, err := f(context.Background(), sealed, true, 20, nil)
	require.NoError(t, err)
	assert.Nil(t, next, "cached bucket returns nil resume state (LoadHistory re-anchors by timestamp)")
	assert.Empty(t, live.served, "a sealed cache hit must not call the live fetcher")
	require.Len(t, rows, 2)
	assert.Equal(t, "b", rows[0].MessageID)
	assert.Equal(t, "c", rows[1].MessageID)
}

func TestCachedDescFetcher_SealedBucket_RespectsRemaining(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	t0 := fetcherNow.Add(-73 * time.Hour)
	bc, err := bucketcache.NewCache(mv, 1000, time.Minute)
	require.NoError(t, err)
	bc.Put(context.Background(), "r1", sealed, []models.Message{
		{MessageID: "a", RoomID: "r1", CreatedAt: t0},
		{MessageID: "b", RoomID: "r1", CreatedAt: t0.Add(-time.Minute)},
		{MessageID: "c", RoomID: "r1", CreatedAt: t0.Add(-2 * time.Minute)},
	})

	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, (&recordingLive{}).fetch())
	rows, _, err := f(context.Background(), sealed, true, 2, nil) // remaining=2
	require.NoError(t, err)
	require.Len(t, rows, 2, "must not return more than `remaining` rows")
	assert.Equal(t, "a", rows[0].MessageID)
	assert.Equal(t, "b", rows[1].MessageID)
}

func TestBustBucket_EvictsMessageBucket(t *testing.T) {
	mv := newMemValkey()
	r := newCachedRepo(t, mv)
	ctx := context.Background()
	createdAt := fetcherNow.Add(-100 * time.Hour)
	bucket := msgbucket.New(fetcherWindow).Of(createdAt)

	seed, err := bucketcache.NewCache(mv, 1000, time.Minute)
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

func TestCachedDescFetcher_NilCache_AlwaysLive(t *testing.T) {
	r := NewRepository(nil, msgbucket.New(fetcherWindow), 122, nil,
		withClock(func() time.Time { return fetcherNow }))
	live := &recordingLive{}
	sizer := msgbucket.New(fetcherWindow)
	sealed := sizer.Prev(sizer.Of(fetcherNow))

	// With no cache, cachedDescFetcher returns the live fetcher unchanged.
	f := r.cachedDescFetcher("r1", fetcherNow, nil, sealed, live.fetch())
	_, _, err := f(context.Background(), sealed, true, 20, nil)
	require.NoError(t, err)
	assert.Equal(t, []int64{sealed}, live.served, "no cache → sealed bucket served live")
}

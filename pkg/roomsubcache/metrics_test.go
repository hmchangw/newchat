package roomsubcache_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/roomsubcache"
)

// fakeRecorder counts cache outcomes for assertions.
type fakeRecorder struct{ hits, misses, errs int }

func (r *fakeRecorder) Hit(context.Context)   { r.hits++ }
func (r *fakeRecorder) Miss(context.Context)  { r.misses++ }
func (r *fakeRecorder) Error(context.Context) { r.errs++ }

func TestValkeyCache_Get_RecordsHit(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	rec := &fakeRecorder{}
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMetrics(rec))

	require.NoError(t, cache.Set(ctx, "room1", []roomsubcache.Member{{ID: "u1", Account: "a"}}, time.Minute))
	_, err := cache.Get(ctx, "room1")
	require.NoError(t, err)

	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 0, rec.errs)
}

func TestValkeyCache_Get_RecordsMiss(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	rec := &fakeRecorder{}
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMetrics(rec))

	_, err := cache.Get(ctx, "absent")
	require.Error(t, err)

	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 0, rec.errs)
}

func TestValkeyCache_Get_RecordsErrorOnTransportFailure(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.getErr = errors.New("valkey down")
	rec := &fakeRecorder{}
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMetrics(rec))

	_, err := cache.Get(ctx, "room1")
	require.Error(t, err)

	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 1, rec.errs)
}

func TestValkeyCache_Get_RecordsErrorOnOversizeBlob(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.store["room:room1:subs"] = strings.Repeat("x", 200)
	rec := &fakeRecorder{}
	cache := roomsubcache.NewValkeyCache(client,
		roomsubcache.WithMaxValueBytes(100), roomsubcache.WithMetrics(rec))

	_, err := cache.Get(ctx, "room1")
	require.Error(t, err)

	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 1, rec.errs)
}

func TestValkeyCache_Get_RecordsErrorOnMalformedJSON(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	client.store["room:room1:subs"] = "{not json"
	rec := &fakeRecorder{}
	cache := roomsubcache.NewValkeyCache(client, roomsubcache.WithMetrics(rec))

	_, err := cache.Get(ctx, "room1")
	require.Error(t, err)

	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 1, rec.errs)
}

// A cache built without WithMetrics must still work and never panic when
// recording against the package-default recorder.
func TestValkeyCache_Get_NoMetricsOption_NoPanic(t *testing.T) {
	ctx := context.Background()
	client := newFakeClient()
	cache := roomsubcache.NewValkeyCache(client)

	assert.NotPanics(t, func() {
		_, _ = cache.Get(ctx, "absent")
	})
}

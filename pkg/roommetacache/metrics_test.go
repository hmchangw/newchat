package roommetacache

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

// fakeRecorder counts L2 cache outcomes for assertions.
type fakeRecorder struct{ hits, misses, errs int }

func (r *fakeRecorder) Hit(context.Context)   { r.hits++ }
func (r *fakeRecorder) Miss(context.Context)  { r.misses++ }
func (r *fakeRecorder) Error(context.Context) { r.errs++ }

func TestReadL2_Hit(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 4}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)
	rec := &fakeRecorder{}

	got, found := readL2(context.Background(), fake, "r1", rec)

	require.True(t, found)
	assert.Equal(t, want, got)
	assert.Equal(t, 1, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 0, rec.errs)
}

func TestReadL2_Miss(t *testing.T) {
	fake := newFakeValkey() // empty store → ErrCacheMiss
	rec := &fakeRecorder{}

	_, found := readL2(context.Background(), fake, "r1", rec)

	assert.False(t, found)
	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 1, rec.misses)
	assert.Equal(t, 0, rec.errs)
}

func TestReadL2_Error(t *testing.T) {
	fake := newFakeValkey()
	fake.getErr = errors.New("valkey down")
	rec := &fakeRecorder{}

	_, found := readL2(context.Background(), fake, "r1", rec)

	assert.False(t, found)
	assert.Equal(t, 0, rec.hits)
	assert.Equal(t, 0, rec.misses)
	assert.Equal(t, 1, rec.errs)
}

func TestCache_L1Metrics_HitMissError(t *testing.T) {
	ctx := context.Background()
	rec := &fakeRecorder{}
	loadErr := errors.New("mongo down")
	var fail bool
	cache, err := New(10, time.Minute, func(context.Context, string) (Meta, error) {
		if fail {
			return Meta{}, loadErr
		}
		return Meta{ID: "r1"}, nil
	}, WithMetrics(rec))
	require.NoError(t, err)

	// Miss: not cached, loader succeeds.
	_, err = cache.Get(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, [3]int{0, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Hit: served from L1.
	_, err = cache.Get(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, [3]int{1, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Error: loader fails.
	fail = true
	_, err = cache.Get(ctx, "r2")
	require.Error(t, err)
	assert.Equal(t, [3]int{1, 1, 1}, [3]int{rec.hits, rec.misses, rec.errs})
}

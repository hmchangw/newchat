package readcache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRecorder counts cache outcomes for assertions.
type fakeRecorder struct{ hits, misses, errs int }

func (r *fakeRecorder) Hit(context.Context)   { r.hits++ }
func (r *fakeRecorder) Miss(context.Context)  { r.misses++ }
func (r *fakeRecorder) Error(context.Context) { r.errs++ }

func TestTTLCache_Metrics_HitMissError(t *testing.T) {
	ctx := context.Background()
	rec := &fakeRecorder{}
	c, err := newTTLCache[string](10, time.Minute, rec)
	require.NoError(t, err)

	loadOK := func(context.Context) (string, bool, error) { return "v", true, nil }
	loadErr := func(context.Context) (string, bool, error) { return "", false, errors.New("mongo down") }

	// Miss: not cached, load succeeds.
	_, err = c.getOrLoad(ctx, "k1", loadOK)
	require.NoError(t, err)
	assert.Equal(t, [3]int{0, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Hit.
	_, err = c.getOrLoad(ctx, "k1", loadOK)
	require.NoError(t, err)
	assert.Equal(t, [3]int{1, 1, 0}, [3]int{rec.hits, rec.misses, rec.errs})

	// Error: load fails.
	_, err = c.getOrLoad(ctx, "k2", loadErr)
	require.Error(t, err)
	assert.Equal(t, [3]int{1, 1, 1}, [3]int{rec.hits, rec.misses, rec.errs})
}

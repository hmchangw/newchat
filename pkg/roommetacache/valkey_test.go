package roommetacache

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/valkeyutil"
)

// fakeValkey is an in-memory valkeyutil.Client for unit tests.
type fakeValkey struct {
	mu     sync.Mutex
	data   map[string]string
	dels   []string
	sets   []string // keys passed to Set, regardless of setErr
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
	f.sets = append(f.sets, key)
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
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

func TestMetaKey(t *testing.T) {
	assert.Equal(t, "room:{r123}:meta", MetaKey("r123"))
}

func TestReadThrough_L2Hit(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 7}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)

	// nil *mongo.Collection is safe: on an L2 hit, Mongo is never touched.
	got, err := ReadThrough(context.Background(), fake, nil, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// TestReadThrough_L2Hit_DoesNotPopulate confirms that on a cache hit the
// populate (Set) path is never reached — Mongo is not consulted and no Set
// call is issued.
//
// NOTE: the miss→populate and error-fallthrough paths inside ReadThrough
// require a live *mongo.Collection to avoid a nil-dereference; those paths
// are covered by integration_test.go (Task 2).
func TestReadThrough_L2Hit_DoesNotPopulate(t *testing.T) {
	fake := newFakeValkey()
	want := Meta{ID: "r1", Type: model.RoomTypeChannel, Name: "general", SiteID: "site-a", UserCount: 3}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	fake.data[MetaKey("r1")] = string(raw)

	// nil *mongo.Collection is safe on a hit — Mongo must never be touched.
	got, err := ReadThrough(context.Background(), fake, nil, "r1", time.Minute, &fakeRecorder{})
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Empty(t, fake.sets, "Set must not be called on a cache hit")
}

func TestBustMeta_CallsDel(t *testing.T) {
	fake := newFakeValkey()
	fake.data[MetaKey("r1")] = "{}"
	BustMeta(context.Background(), fake, "r1")
	assert.Equal(t, []string{MetaKey("r1")}, fake.dels)
	_, present := fake.data[MetaKey("r1")]
	assert.False(t, present)
}

func TestBustMeta_NilClient_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { BustMeta(context.Background(), nil, "r1") })
}

func TestBustMeta_FailOpen(t *testing.T) {
	fake := newFakeValkey()
	fake.delErr = errors.New("valkey down")
	// Must not panic and must not propagate — best-effort.
	assert.NotPanics(t, func() { BustMeta(context.Background(), fake, "r1") })
	assert.Equal(t, []string{MetaKey("r1")}, fake.dels)
}

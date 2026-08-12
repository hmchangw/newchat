package valkeyutil_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/valkeyutil"
)

type fakeClient struct {
	store       map[string]string
	ttls        map[string]time.Duration
	setErr      error
	getErr      error
	setNXErr    error
	incrErr     error
	delCalls    [][]string
	closeCalled int
	closeErr    error
}

func newFake() *fakeClient {
	return &fakeClient{
		store: make(map[string]string),
		ttls:  make(map[string]time.Duration),
	}
}

var _ valkeyutil.Client = (*fakeClient)(nil)

func (f *fakeClient) Get(_ context.Context, key string) (string, error) {
	if f.getErr != nil {
		return "", f.getErr
	}
	v, ok := f.store[key]
	if !ok {
		return "", valkeyutil.ErrCacheMiss
	}
	return v, nil
}

func (f *fakeClient) Set(_ context.Context, key, value string, ttl time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.store[key] = value
	f.ttls[key] = ttl
	return nil
}

func (f *fakeClient) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	if f.setNXErr != nil {
		return false, f.setNXErr
	}
	if _, exists := f.store[key]; exists {
		return false, nil
	}
	f.store[key] = value
	f.ttls[key] = ttl
	return true, nil
}

func (f *fakeClient) IncrEx(_ context.Context, key string, ttl time.Duration) (int64, error) {
	if f.incrErr != nil {
		return 0, f.incrErr
	}
	var n int64
	if v, ok := f.store[key]; ok {
		var parsed int64
		_, _ = fmt.Sscan(v, &parsed)
		n = parsed + 1
	} else {
		n = 1
	}
	f.store[key] = fmt.Sprintf("%d", n)
	// TTL applied only on 0->1 (fixed-window recipe).
	if n == 1 {
		f.ttls[key] = ttl
	}
	return n, nil
}

func (f *fakeClient) Del(_ context.Context, keys ...string) error {
	f.delCalls = append(f.delCalls, keys)
	for _, k := range keys {
		delete(f.store, k)
		delete(f.ttls, k)
	}
	return nil
}
func (f *fakeClient) Expire(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

func (f *fakeClient) Close() error {
	f.closeCalled++
	return f.closeErr
}

type cached struct {
	Rooms []string `json:"rooms"`
}

func TestGetJSON_HitAndMiss(t *testing.T) {
	ctx := context.Background()
	client := newFake()

	t.Run("miss returns ErrCacheMiss", func(t *testing.T) {
		var out cached
		err := valkeyutil.GetJSON(ctx, client, "missing", &out)
		assert.ErrorIs(t, err, valkeyutil.ErrCacheMiss)
	})

	t.Run("hit decodes JSON", func(t *testing.T) {
		require.NoError(t, valkeyutil.SetJSONWithTTL(ctx, client, "k1", cached{Rooms: []string{"r1", "r2"}}, 5*time.Minute))
		var out cached
		require.NoError(t, valkeyutil.GetJSON(ctx, client, "k1", &out))
		assert.Equal(t, []string{"r1", "r2"}, out.Rooms)
	})

	t.Run("Set persists TTL", func(t *testing.T) {
		require.NoError(t, valkeyutil.SetJSONWithTTL(ctx, client, "k2", cached{}, 30*time.Second))
		assert.Equal(t, 30*time.Second, client.ttls["k2"])
	})

	t.Run("malformed JSON wraps unmarshal error", func(t *testing.T) {
		client.store["bad"] = "{not json"
		var out cached
		err := valkeyutil.GetJSON(ctx, client, "bad", &out)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	})

	t.Run("transport error propagates", func(t *testing.T) {
		broken := newFake()
		broken.getErr = errors.New("boom")
		var out cached
		err := valkeyutil.GetJSON(ctx, broken, "k", &out)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, valkeyutil.ErrCacheMiss)
	})
}

func TestDisconnect(t *testing.T) {
	t.Run("nil is safe", func(t *testing.T) {
		valkeyutil.Disconnect(nil)
	})

	t.Run("happy path calls Close once", func(t *testing.T) {
		f := newFake()
		valkeyutil.Disconnect(f)
		assert.Equal(t, 1, f.closeCalled)
	})

	t.Run("Close error is logged but does not panic", func(t *testing.T) {
		f := newFake()
		f.closeErr = errors.New("close failed")
		// Disconnect swallows the error (logs only). The assertion is
		// that Close WAS called — the error path doesn't skip or retry.
		valkeyutil.Disconnect(f)
		assert.Equal(t, 1, f.closeCalled)
	})
}

// TestSetNXFakeSemantics documents the interface contract SetNX callers depend on.
func TestSetNXFakeSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("first call to unset key acquires and returns true", func(t *testing.T) {
		f := newFake()
		acquired, err := f.SetNX(ctx, "idem:1", "processing", time.Second)
		require.NoError(t, err)
		assert.True(t, acquired)
		assert.Equal(t, "processing", f.store["idem:1"])
		assert.Equal(t, time.Second, f.ttls["idem:1"])
	})

	t.Run("second call to same key does not overwrite and returns false", func(t *testing.T) {
		f := newFake()
		_, _ = f.SetNX(ctx, "idem:1", "first", time.Second)
		acquired, err := f.SetNX(ctx, "idem:1", "second", 2*time.Second)
		require.NoError(t, err)
		assert.False(t, acquired)
		assert.Equal(t, "first", f.store["idem:1"], "value must be immutable while the key exists")
	})

	t.Run("transport error propagates", func(t *testing.T) {
		f := newFake()
		f.setNXErr = errors.New("boom")
		acquired, err := f.SetNX(ctx, "k", "v", time.Second)
		assert.Error(t, err)
		assert.False(t, acquired)
	})
}

// TestIncrExFakeSemantics: TTL set only on 0->1 so subsequent hits don't reset the window.
func TestIncrExFakeSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("first hit sets TTL and returns 1", func(t *testing.T) {
		f := newFake()
		n, err := f.IncrEx(ctx, "rl:alice", time.Minute)
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)
		assert.Equal(t, time.Minute, f.ttls["rl:alice"])
	})

	t.Run("second hit does not reset TTL and returns 2", func(t *testing.T) {
		f := newFake()
		_, _ = f.IncrEx(ctx, "rl:alice", time.Minute)
		f.ttls["rl:alice"] = 30 * time.Second
		n, err := f.IncrEx(ctx, "rl:alice", time.Minute)
		require.NoError(t, err)
		assert.Equal(t, int64(2), n)
		assert.Equal(t, 30*time.Second, f.ttls["rl:alice"], "TTL must not reset on subsequent hits")
	})

	t.Run("transport error propagates", func(t *testing.T) {
		f := newFake()
		f.incrErr = errors.New("boom")
		n, err := f.IncrEx(ctx, "k", time.Second)
		assert.Error(t, err)
		assert.Zero(t, n)
	})
}

func TestConnectCluster_ErrorPath(t *testing.T) {
	_, err := valkeyutil.ConnectCluster(context.Background(), []string{"127.0.0.1:1"}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "valkey cluster connect")
}

// --- ReadCachedJSON ---

type cacheSpy struct{ hit, miss, err int }

func (s *cacheSpy) Hit(context.Context)   { s.hit++ }
func (s *cacheSpy) Miss(context.Context)  { s.miss++ }
func (s *cacheSpy) Error(context.Context) { s.err++ }

type cachedThing struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func nonEmptyID(c *cachedThing) bool { return c.ID != "" }

func TestReadCachedJSON(t *testing.T) {
	const key = "thing:{r1}"
	tests := []struct {
		name      string
		stored    string // "" means absent
		getErr    error
		wantOK    bool
		wantID    string
		wantHit   int
		wantMiss  int
		wantError int
	}{
		{name: "usable hit", stored: `{"id":"u1","name":"alice"}`, wantOK: true, wantID: "u1", wantHit: 1},
		{name: "clean miss", wantMiss: 1},
		// Any well-formed JSON that is not a T decodes to the zero value. Serving
		// that is worse than a refetch, so it must count as a miss, not a hit.
		{name: "json null decodes to zero", stored: "null", wantMiss: 1},
		{name: "empty object decodes to zero", stored: "{}", wantMiss: 1},
		{name: "foreign value decodes to zero", stored: `{"unrelated":"x"}`, wantMiss: 1},
		{name: "partial value failing validation", stored: `{"name":"alice"}`, wantMiss: 1},
		{name: "transport failure", getErr: errors.New("valkey unreachable"), wantError: 1},
		{name: "undecodable payload", stored: "not-json", wantError: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := newFake()
			fake.getErr = tt.getErr
			if tt.stored != "" {
				fake.store[key] = tt.stored
			}
			rec := &cacheSpy{}

			got, ok := valkeyutil.ReadCachedJSON(context.Background(), fake, key, "thing", rec, nonEmptyID, "room_id", "r1")

			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantID, got.ID)
			assert.Equal(t, tt.wantHit, rec.hit, "hit count")
			assert.Equal(t, tt.wantMiss, rec.miss, "miss count")
			assert.Equal(t, tt.wantError, rec.err, "error count")
		})
	}
}

// A nil validity predicate means every decoded value is usable.
func TestReadCachedJSON_NilPredicateAcceptsZeroValue(t *testing.T) {
	fake := newFake()
	fake.store["k"] = "{}"
	rec := &cacheSpy{}

	_, ok := valkeyutil.ReadCachedJSON[cachedThing](context.Background(), fake, "k", "thing", rec, nil)

	assert.True(t, ok)
	assert.Equal(t, 1, rec.hit)
}

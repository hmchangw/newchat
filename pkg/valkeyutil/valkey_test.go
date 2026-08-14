package valkeyutil_test

import (
	"context"
	"errors"
	"fmt"
	"net"
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

// blackholeListener accepts TCP connections and never responds. This is the
// failure mode that matters: a Valkey that refuses connections fails fast on
// its own, but one that silently drops packets stalls every call until a
// timeout fires. Connection-refused testing never catches it.
func blackholeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			// Hold the connection open and never write. Closed by t.Cleanup.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
	})
	return ln.Addr().String()
}

func TestConnectCluster_BoundedLatencyAgainstBlackhole(t *testing.T) {
	addr := blackholeListener(t)

	start := time.Now()
	_, err := valkeyutil.ConnectCluster(context.Background(), []string{addr}, "")
	elapsed := time.Since(start)

	require.Error(t, err, "connect against a blackhole must fail, not hang")
	// 2s is chosen to discriminate, not just to be generous: it is below the old
	// go-redis default ReadTimeout (3s) and below ConnectCluster's own 5s ping
	// ceiling, so this assertion would FAIL against the unfixed defaults instead
	// of being masked by that ceiling. Measured actual is ~0.17s, so 2s still
	// leaves ~10x margin against CI jitter.
	assert.Less(t, elapsed, 2*time.Second, "connect must be bounded by dial/read timeouts")
}

func TestConnectClusterLazy_ReturnsUsableClientWhenUnreachable(t *testing.T) {
	addr := blackholeListener(t)

	client, err := valkeyutil.ConnectClusterLazy(context.Background(), []string{addr}, "")
	require.NoError(t, err, "lazy connect must not fail on an unreachable Valkey")
	require.NotNil(t, client)
	t.Cleanup(func() { _ = client.Close() })

	// The client is usable; the call itself errors rather than panicking.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, getErr := client.Get(ctx, "some-key")
	assert.Error(t, getErr)
}

func TestConnectClusterLazy_BoundedFirstCall(t *testing.T) {
	addr := blackholeListener(t)
	client, err := valkeyutil.ConnectClusterLazy(context.Background(), []string{addr}, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	start := time.Now()
	_, _ = client.Get(context.Background(), "k")
	elapsed := time.Since(start)
	t.Logf("first Get against blackhole took %s", elapsed)
	// The brief's own suggested 5s ceiling matches ConnectClusterLazy's PING
	// bound and would pass even against an unbounded stall, so it proves
	// nothing. CacheProfile bounds a single Get to ReadTimeout(150ms) +
	// MaxRetries(1)*ReadTimeout ~= 300ms plus one-time cluster-slots discovery
	// against the same blackhole; 2s (same threshold as the ConnectCluster
	// blackhole test above, and still below go-redis's old 3s default
	// ReadTimeout) discriminates a genuinely bounded first call from an
	// unbounded one while leaving ample margin over the measured ~0.2-0.3s.
	assert.Less(t, elapsed, 2*time.Second, "CacheProfile must bound the first call")
}

// NewClusterClient exists so raw-client consumers (Lua scripting, sorted sets)
// stop re-implementing construction and silently losing instrumentation. Like
// ConnectClusterLazy, an unreachable cluster is a logged probe, not an error.
func TestNewClusterClient_UnreachableIsNotAnError(t *testing.T) {
	c, err := valkeyutil.NewClusterClient(context.Background(),
		[]string{"127.0.0.1:1"}, "", valkeyutil.WithProfile(valkeyutil.StoreProfile))
	require.NoError(t, err, "unreachability must never fail construction")
	require.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })

	opts := c.Options()
	assert.Equal(t, valkeyutil.StoreProfile.ReadTimeout, opts.ReadTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.WriteTimeout, opts.WriteTimeout)
	assert.Equal(t, valkeyutil.StoreProfile.MaxRetries, opts.MaxRetries)
	assert.True(t, opts.ContextTimeoutEnabled, "caller deadlines must bound socket reads")
}

func TestNewClusterClient_DefaultsToCacheProfile(t *testing.T) {
	c, err := valkeyutil.NewClusterClient(context.Background(), []string{"127.0.0.1:1"}, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	assert.Equal(t, valkeyutil.CacheProfile.ReadTimeout, c.Options().ReadTimeout)
}

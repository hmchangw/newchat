package valkeyutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubClient is a Client whose Get result is controlled per call.
type stubClient struct {
	mu   sync.Mutex
	err  error
	val  string
	gets int
}

func (s *stubClient) Get(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return s.val, s.err
}
func (s *stubClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return s.err
}
func (s *stubClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return true, s.err
}
func (s *stubClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	return 1, s.err
}
func (s *stubClient) Del(ctx context.Context, keys ...string) error { return s.err }
func (s *stubClient) Close() error                                  { return nil }

func (s *stubClient) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}
func (s *stubClient) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// This is the test that matters most. gobreaker counts every returned error as
// a failure, so a cache miss — which is the cache working correctly — would
// trip the breaker and disable Valkey for a perfectly healthy workload.
func TestIsSuccessful(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is success", nil, true},
		{"cache miss is success, not a failure", ErrCacheMiss, true},
		{"wrapped cache miss is success", fmt.Errorf("valkey get json: %w", ErrCacheMiss), true},
		// A cancelled call proves nothing about Valkey, and gobreaker resets
		// ConsecutiveFailures on every success — so scoring it a success would let a
		// steady trickle of cancellations (errgroup cancelling siblings during an
		// outage) keep the breaker from ever tripping. Failure is the safer wrong answer.
		{"context canceled is failure", context.Canceled, false},
		{"wrapped context canceled is failure", fmt.Errorf("valkey get: %w", context.Canceled), false},
		{"deadline exceeded is failure", context.DeadlineExceeded, false},
		// Pool exhaustion is local saturation, not an unreachable Valkey. Scoring it a
		// failure would let a burst of our own concurrency black out a healthy cache
		// and stampede the fallback store — here success is the safer wrong answer.
		{"pool timeout is success", redis.ErrPoolTimeout, true},
		{"wrapped pool timeout is success", fmt.Errorf("valkey get: %w", redis.ErrPoolTimeout), true},
		{"transport error is failure", errors.New("dial tcp: i/o timeout"), false},
		{"plain error is failure", errors.New("x"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSuccessful(tt.err))
		})
	}
}

func TestIsSuccessful_StringSimilarErrorIsStillAFailure(t *testing.T) {
	// Only errors.Is-matching misses count as success. An error that merely
	// reads like a miss must still trip the breaker.
	lookalike := errors.New("valkey get json: " + ErrCacheMiss.Error())
	assert.False(t, isSuccessful(lookalike))
}

func TestBreaker_CacheMissesNeverTrip(t *testing.T) {
	stub := &stubClient{err: ErrCacheMiss}
	c := NewBreakerClient(stub, "test-miss")

	for i := 0; i < 50; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, ErrCacheMiss)
	}
	assert.Equal(t, 50, stub.callCount(), "every call must reach the inner client")
}

func TestBreaker_TripsAfterConsecutiveFailures(t *testing.T) {
	boom := errors.New("i/o timeout")
	stub := &stubClient{err: boom}
	c := NewBreakerClient(stub, "test-trip")

	for i := 0; i < breakerFailureThreshold; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, boom)
	}
	inner := stub.callCount()

	// Breaker is now open: calls short-circuit without touching the inner client.
	_, err := c.Get(context.Background(), "k")
	assert.True(t, IsUnavailable(err), "expected ErrUnavailable, got %v", err)
	assert.Equal(t, inner, stub.callCount(), "open breaker must not call inner")
}

func TestBreaker_RecoversAfterCooldown(t *testing.T) {
	boom := errors.New("i/o timeout")
	stub := &stubClient{err: boom}
	c := NewBreakerClient(stub, "test-recover")

	for i := 0; i < breakerFailureThreshold; i++ {
		_, _ = c.Get(context.Background(), "k")
	}
	require.True(t, IsUnavailable(mustErr(c.Get(context.Background(), "k"))))

	// Valkey comes back; wait out the cooldown so the breaker half-opens.
	stub.setErr(nil)
	stub.val = "v"
	time.Sleep(breakerCooldown + 100*time.Millisecond)

	got, err := c.Get(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)
}

func TestBreaker_ConcurrentCallersAreSafe(t *testing.T) {
	stub := &stubClient{err: errors.New("i/o timeout")}
	c := NewBreakerClient(stub, "test-concurrent")

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.Get(context.Background(), "k")
		}()
	}
	wg.Wait() // -race asserts no data race in the breaker itself
}

func TestBreaker_PassesThroughAllMethods(t *testing.T) {
	stub := &stubClient{val: "v"}
	c := NewBreakerClient(stub, "test-methods")
	ctx := context.Background()

	got, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "v", got)

	require.NoError(t, c.Set(ctx, "k", "v", time.Minute))

	ok, err := c.SetNX(ctx, "k", "v", time.Minute)
	require.NoError(t, err)
	assert.True(t, ok)

	n, err := c.IncrEx(ctx, "k", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.NoError(t, c.Del(ctx, "k"))
	require.NoError(t, c.Close())
}

func mustErr(_ string, err error) error { return err }

func TestLogDegraded_LevelDependsOnCircuitState(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"live failure is news, logs at warn", errors.New("dial tcp: connection refused"), "WARN"},
		{"open circuit is already known, drops to debug", ErrUnavailable, "DEBUG"},
		{"wrapped open circuit drops to debug", fmt.Errorf("l2 get: %w", ErrUnavailable), "DEBUG"},
		{"cache miss is still a live outcome, warns", ErrCacheMiss, "WARN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			old := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(old) })

			LogDegraded(context.Background(), "room meta L2 read failed", tt.err, "room_id", "r1")

			var rec map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
			assert.Equal(t, tt.want, rec["level"])
			assert.Equal(t, "room meta L2 read failed", rec["msg"], "message must survive the level change")
			assert.Equal(t, "r1", rec["room_id"], "caller context must survive the level change")
			assert.NotEmpty(t, rec["error"], "the cause must always be attached")
		})
	}
}

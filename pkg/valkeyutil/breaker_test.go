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
//
// isSuccessful only decides the success/failure axis. Errors that are evidence
// of nothing either way are removed from the accounting entirely by isExcluded,
// which gobreaker consults first; see TestIsExcluded.
func TestIsSuccessful(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is success", nil, true},
		{"cache miss is success, not a failure", ErrCacheMiss, true},
		{"wrapped cache miss is success", fmt.Errorf("valkey get json: %w", ErrCacheMiss), true},
		// Neither of these is a success. Scoring them so would clear
		// ConsecutiveFailures and hold the breaker closed through a real outage.
		// They are excluded rather than scored — isSuccessful is never reached for
		// them — but the verdict is asserted here so a future change to the
		// exclusion set cannot silently promote them to successes.
		{"context canceled is not a success", context.Canceled, false},
		{"pool timeout is not a success", redis.ErrPoolTimeout, false},
		// Deliberately NOT excluded: with ContextTimeoutEnabled a blackholing
		// Valkey surfaces as a deadline, which is the outage this package exists
		// to catch.
		{"deadline exceeded is failure", context.DeadlineExceeded, false},
		{"wrapped deadline exceeded is failure", fmt.Errorf("valkey get: %w", context.DeadlineExceeded), false},
		{"transport error is failure", errors.New("dial tcp: i/o timeout"), false},
		{"plain error is failure", errors.New("x"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isSuccessful(tt.err))
		})
	}
}

// gobreaker v2 has a third outcome: IsExcluded removes a call from the
// accounting entirely, counting it neither a success nor a failure. Errors that
// carry no evidence about Valkey's reachability belong there — scoring them
// either way is a wrong answer with a real cost.
func TestIsExcluded(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is accounted", nil, false},
		// The caller went away. This says nothing about Valkey, so counting it a
		// failure lets a burst of cancellations open the breaker on a healthy
		// cache — and counting it a success lets one hold the breaker closed
		// through a real outage.
		{"context canceled is excluded", context.Canceled, true},
		{"wrapped context canceled is excluded", fmt.Errorf("valkey get: %w", context.Canceled), true},
		// Local saturation: our own concurrency outran the pool, so Valkey was
		// never asked.
		{"pool timeout is excluded", redis.ErrPoolTimeout, true},
		{"wrapped pool timeout is excluded", fmt.Errorf("valkey get: %w", redis.ErrPoolTimeout), true},
		// A deadline IS evidence — a blackholing Valkey looks exactly like this.
		{"deadline exceeded is accounted", context.DeadlineExceeded, false},
		{"wrapped deadline exceeded is accounted", fmt.Errorf("valkey get: %w", context.DeadlineExceeded), false},
		{"cache miss is accounted", ErrCacheMiss, false},
		{"transport error is accounted", errors.New("dial tcp: i/o timeout"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isExcluded(tt.err))
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

// A cancelled caller is no evidence Valkey is down. Counting cancellations as
// failures opens the breaker on a perfectly healthy cache and stampedes the
// fallback store — and they arrive in bursts, since one slow sibling makes an
// errgroup cancel all the others at once.
func TestBreaker_CancellationsAloneNeverTrip(t *testing.T) {
	stub := &stubClient{err: context.Canceled}
	c := NewBreakerClient(stub, "test-cancel-neutral")

	calls := breakerFailureThreshold * 3
	for i := 0; i < calls; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, context.Canceled, "call %d short-circuited: the breaker opened on cancellations", i)
	}
	assert.Equal(t, calls, stub.callCount(), "every call must reach the inner client")
}

// The other half of neutrality: excluded errors must not clear the failure
// count either, or a trickle of them holds the breaker closed through a real
// outage — the failure mode this package exists to prevent.
func TestBreaker_ExcludedErrorsDoNotResetFailureCount(t *testing.T) {
	boom := errors.New("i/o timeout")
	tests := []struct {
		name     string
		excluded error
	}{
		{"cancellation between failures", context.Canceled},
		{"pool timeout between failures", redis.ErrPoolTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &stubClient{}
			c := NewBreakerClient(stub, "test-no-reset-"+tt.name)
			ctx := context.Background()

			// One short of the threshold.
			stub.setErr(boom)
			for i := 0; i < breakerFailureThreshold-1; i++ {
				_, err := c.Get(ctx, "k")
				require.ErrorIs(t, err, boom)
			}

			stub.setErr(tt.excluded)
			_, err := c.Get(ctx, "k")
			require.ErrorIs(t, err, tt.excluded)

			// This is the threshold'th consecutive real failure.
			stub.setErr(boom)
			_, err = c.Get(ctx, "k")
			require.ErrorIs(t, err, boom)

			before := stub.callCount()
			_, err = c.Get(ctx, "k")
			assert.True(t, IsUnavailable(err),
				"breaker must be open: an excluded error must not reset ConsecutiveFailures")
			assert.Equal(t, before, stub.callCount(), "open breaker must not call inner")
		})
	}
}

// Guards the exclusion set from growing too far. A deadline is how a
// blackholing Valkey — the degraded mode this package targets — actually
// surfaces once ContextTimeoutEnabled bounds socket reads, so it must stay in
// the accounting as a failure.
func TestBreaker_DeadlineExceededStillTrips(t *testing.T) {
	stub := &stubClient{err: context.DeadlineExceeded}
	c := NewBreakerClient(stub, "test-deadline-trips")

	for i := 0; i < breakerFailureThreshold; i++ {
		_, err := c.Get(context.Background(), "k")
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}

	before := stub.callCount()
	_, err := c.Get(context.Background(), "k")
	assert.True(t, IsUnavailable(err), "a blackholing Valkey must still open the breaker")
	assert.Equal(t, before, stub.callCount())
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

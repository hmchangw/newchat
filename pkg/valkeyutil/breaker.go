package valkeyutil

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
)

const (
	// breakerFailureThreshold rides out a single blip but trips on a real
	// outage.
	breakerFailureThreshold = 5
	// breakerCooldown is how long the breaker stays open before allowing one
	// half-open probe through.
	breakerCooldown = 5 * time.Second
)

// ErrUnavailable is returned when the circuit breaker is open — Valkey is
// known-down and the call short-circuited without a network round-trip.
//
// Every fail-open consumer already branches errors.Is(err, ErrCacheMiss) for
// the miss case and falls back on everything else, so ErrUnavailable lands in
// the fallback path with no consumer change required.
var ErrUnavailable = errors.New("valkey: circuit open")

// IsUnavailable reports whether err came from an open circuit breaker rather
// than a live failure. Call sites use it to suppress per-call warn logs during
// an outage — the breaker already logs each state transition once.
func IsUnavailable(err error) bool {
	return errors.Is(err, ErrUnavailable)
}

// LogDegraded reports a Valkey call that fell back to its backing store,
// attaching err and the caller's own context args.
//
// It is the single place that decides how loud to be while Valkey is down. A
// live failure is news and logs at Warn. An open circuit is not: the breaker
// already logged the transition once, so a per-call Warn would emit one line
// per request for the whole outage — and because the breaker fails in
// microseconds rather than seconds, nothing throttles that flood any more. Those
// drop to Debug, still available on demand without drowning the log.
//
// Callers keep their own message and fields, so the diagnostic context survives;
// only the level moves.
func LogDegraded(ctx context.Context, msg string, err error, args ...any) {
	level := slog.LevelWarn
	if IsUnavailable(err) {
		level = slog.LevelDebug
	}
	slog.Log(ctx, level, msg, append(args, "error", err)...)
}

// isSuccessful classifies an inner-client error for breaker accounting.
//
// This is the most consequential function in the package. gobreaker treats
// every returned error as a failure, so without this a cold or sparse keyspace
// would trip the breaker on ordinary cache misses and disable Valkey for a
// workload that is behaving perfectly — a self-inflicted outage. Only genuine
// transport failures may count toward the trip threshold.
//
// gobreaker offers no neutral outcome, so each ambiguous error is scored by
// which wrong answer costs less:
//   - Cache miss: a completed round trip. Unambiguously healthy.
//   - Pool timeout: our own concurrency outran the pool; Valkey was never asked.
//     Scored a success, because counting it a failure lets a local burst black out
//     a healthy cache and stampede the fallback store.
//   - Cancellation: scored a FAILURE. It is no evidence Valkey is healthy, and
//     since gobreaker clears ConsecutiveFailures on every success, scoring it a
//     success would let cancellations — which an outage generates in bulk, e.g.
//     an errgroup cancelling siblings — hold the breaker closed through a real
//     outage. That is the failure this package exists to prevent.
func isSuccessful(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, ErrCacheMiss) || errors.Is(err, redis.ErrPoolTimeout)
}

// breakerClient wraps a Client with one shared circuit breaker. Reachability
// is a property of the connection, not of the command, so all methods share a
// single breaker rather than one per operation.
type breakerClient struct {
	inner Client
	cb    *gobreaker.CircuitBreaker[any]
}

// NewBreakerClient wraps inner with a circuit breaker named name (used in log
// and metric labels).
func NewBreakerClient(inner Client, name string) Client {
	cb := gobreaker.NewCircuitBreaker[any](gobreaker.Settings{
		Name:        name,
		MaxRequests: 1, // one half-open probe, so recovery never stampedes
		Interval:    0, // never auto-reset counts while closed
		Timeout:     breakerCooldown,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= breakerFailureThreshold
		},
		IsSuccessful: isSuccessful,
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Warn("valkey circuit breaker state change",
				"breaker", name, "from", from.String(), "to", to.String())
			recordBreakerTransition(name, from, to)
		},
	})
	recordBreakerState(name, gobreaker.StateClosed)
	return &breakerClient{inner: inner, cb: cb}
}

// exec runs fn through the breaker, translating gobreaker's own rejection
// errors into ErrUnavailable so consumers have one sentinel to match on.
func (b *breakerClient) exec(fn func() (any, error)) (any, error) {
	v, err := b.cb.Execute(fn)
	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return nil, ErrUnavailable
	}
	return v, err
}

func (b *breakerClient) Get(ctx context.Context, key string) (string, error) {
	v, err := b.exec(func() (any, error) { return b.inner.Get(ctx, key) })
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (b *breakerClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	_, err := b.exec(func() (any, error) { return nil, b.inner.Set(ctx, key, value, ttl) })
	return err
}

func (b *breakerClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	v, err := b.exec(func() (any, error) { return b.inner.SetNX(ctx, key, value, ttl) })
	if err != nil {
		return false, err
	}
	return v.(bool), nil
}

func (b *breakerClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	v, err := b.exec(func() (any, error) { return b.inner.IncrEx(ctx, key, ttl) })
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

func (b *breakerClient) Del(ctx context.Context, keys ...string) error {
	_, err := b.exec(func() (any, error) { return nil, b.inner.Del(ctx, keys...) })
	return err
}

// Close bypasses the breaker — shutdown must not be blocked by an open circuit.
func (b *breakerClient) Close() error { return b.inner.Close() }

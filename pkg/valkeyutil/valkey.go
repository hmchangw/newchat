// Package valkeyutil provides thin connection + JSON helpers around the Valkey (Redis-compatible)
// client, modeled on pkg/mongoutil. Uses go-redis/v9 — Valkey is wire-compatible so no separate driver is needed.
package valkeyutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	o11yredis "github.com/flywindy/o11y/redis"
	"github.com/redis/go-redis/v9"
)

// Client is the interface exposed by ConnectCluster. Tests can substitute their own implementation without depending on go-redis directly.
type Client interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// SetNX atomically sets key to value with ttl iff key is absent: (true,nil) acquired,
	// (false,nil) refused, (false,err) transport failure. ttl must be > 0 — a zero ttl stores without expiry.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// IncrEx increments key by 1, returning the post-increment count. ttl opens a fixed window
	// via INCR + EXPIRE NX: the first increment sets the expiry, later ones leave a live window
	// alone but repair a missing one, so a counter can never outlive its window.
	IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Del(ctx context.Context, keys ...string) error
	Close() error
}

// ErrCacheMiss is returned by Get and GetJSON when the key does not exist.
var ErrCacheMiss = errors.New("valkey: cache miss")

type clusterClient struct {
	c *redis.ClusterClient
}

// buildCluster constructs and instruments the raw cluster client. Callers own
// closing c on any error path that follows.
func buildCluster(addrs []string, password string, cc *connectConfig) (*redis.ClusterClient, error) {
	c := redis.NewClusterClient(ClusterOptionsFor(addrs, password, cc.profile))
	if err := instrumentCluster(c, cc); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed instrument", "error", closeErr)
		}
		return nil, err
	}
	return c, nil
}

// wrap applies the circuit breaker unless disabled via WithoutCircuitBreaker.
func wrap(c *redis.ClusterClient, cc *connectConfig) Client {
	base := Client(&clusterClient{c: c})
	if !cc.breaker {
		return base
	}
	return NewBreakerClient(base, cc.breakerName)
}

// pingTimeout bounds the startup reachability probe. Note the effective socket
// deadline is min(pingTimeout, Profile.ReadTimeout) — go-redis takes the earlier
// of the two — so in practice the profile governs and this is only a ceiling.
const pingTimeout = 5 * time.Second

// connect builds, instruments and probes a cluster client, returning it with the
// resolved config. failFast decides the sole difference between the exported
// constructors: whether an unreachable cluster is fatal or merely logged.
func connect(ctx context.Context, addrs []string, password string, failFast bool, opts ...Option) (*redis.ClusterClient, *connectConfig, error) {
	cc := newConnectConfig(opts...)
	c, err := buildCluster(addrs, password, &cc)
	if err != nil {
		return nil, nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	switch pingErr := c.Ping(pingCtx).Err(); {
	case pingErr == nil:
		slog.Info("connected to Valkey cluster", "addrs", addrs)
	case failFast:
		// Close the half-constructed client so unreachable addrs don't leak internal go-redis pool state.
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed connect", "error", closeErr)
		}
		return nil, nil, fmt.Errorf("valkey cluster connect: %w", pingErr)
	default:
		slog.Warn("valkey cluster unreachable at startup; continuing with lazy connect",
			"addrs", addrs, "error", pingErr)
	}
	return c, &cc, nil
}

// ConnectCluster dials a Valkey cluster via the provided seed addresses, verifies connectivity with PING, and returns a Client.
// It fails fast when Valkey is unreachable.
//
// Long-running services must use ConnectClusterLazy instead — a fatal startup
// probe crashloops the pod during a Valkey outage. ConnectCluster remains for
// one-shot CLI tools (tools/seed-sample-data), where failing fast is correct.
func ConnectCluster(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	c, cc, err := connect(ctx, addrs, password, true, opts...)
	if err != nil {
		return nil, err
	}
	return wrap(c, cc), nil
}

// ConnectClusterLazy builds an instrumented cluster client without gating on
// reachability. The startup PING becomes a non-fatal probe: an unreachable
// Valkey is logged and a usable Client is still returned.
//
// This is what long-running services must use. go-redis dials lazily and
// self-heals per call, so a Valkey outage no longer prevents a pod from
// starting — which otherwise turns any rollout, autoscale, or node drain
// during the outage into a CrashLoopBackOff on the message path.
//
// The returned error covers construction and instrumentation failures only;
// it is never returned because Valkey happens to be unreachable.
func ConnectClusterLazy(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	c, cc, err := connect(ctx, addrs, password, false, opts...)
	if err != nil {
		return nil, err
	}
	return wrap(c, cc), nil
}

// NewClusterClient returns the raw instrumented cluster client, for consumers
// whose commands fall outside the Client facade — Lua scripting, sorted sets,
// pipelines. It applies the same options (profile, observability) and the same
// non-fatal startup probe as ConnectClusterLazy, so those consumers no longer
// have to re-implement construction and silently lose the o11y hooks.
//
// The circuit breaker is not available here: it decorates the Client facade, so
// a raw-client consumer sees timeouts rather than short-circuits.
//
// The returned error covers construction and instrumentation only; it is never
// returned because Valkey happens to be unreachable.
func NewClusterClient(ctx context.Context, addrs []string, password string, opts ...Option) (*redis.ClusterClient, error) {
	c, _, err := connect(ctx, addrs, password, false, opts...)
	return c, err
}

// instrumentCluster attaches o11y/redis tracing+metrics hooks when observability is configured.
// o11yredis.Wrap mutates the client in place and is idempotent, registering its own teardown — Disconnect needs no extra handling.
func instrumentCluster(c *redis.ClusterClient, cc *connectConfig) error {
	if cc.obs == nil {
		return nil
	}
	if _, err := o11yredis.Wrap(c, cc.obs.TracerProvider(), cc.obs.MeterProvider(), cc.redisOpts...); err != nil {
		return fmt.Errorf("instrument valkey client: %w", err)
	}
	return nil
}

// WrapClusterClient wraps a pre-built *redis.ClusterClient as a Client; intended for tests that
// need a client configured with a ClusterSlots override (testcontainer port-mapping workaround).
func WrapClusterClient(c *redis.ClusterClient) Client {
	return &clusterClient{c: c}
}

// Disconnect closes the client and logs any failure at ERROR.
func Disconnect(client Client) {
	if client == nil {
		return
	}
	if err := client.Close(); err != nil {
		slog.Error("valkey disconnect failed", "error", err)
	}
}

func (r *clusterClient) Get(ctx context.Context, key string) (string, error) {
	val, err := r.c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}
	if err != nil {
		return "", fmt.Errorf("valkey get: %w", err)
	}
	return val, nil
}

func (r *clusterClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := r.c.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}

func (r *clusterClient) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	// SetArgs with Mode:"NX" replaces deprecated SetNX; redis.Nil = refusal, surfaced as (false, nil).
	res, err := r.c.SetArgs(ctx, key, value, redis.SetArgs{Mode: "NX", TTL: ttl}).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("valkey set nx: %w", err)
	}
	return res == "OK", nil
}

func (r *clusterClient) IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	if ttl <= 0 {
		n, err := r.c.Incr(ctx, key).Result()
		if err != nil {
			return 0, fmt.Errorf("valkey incr: %w", err)
		}
		return n, nil
	}
	// EXPIRE NX on every increment, not a bare EXPIRE on the 0->1 transition: if a
	// prior call's INCR landed and its EXPIRE did not, the counter is left with no
	// expiry, never resets, and permanently throttles the caller once it passes the
	// ceiling. NX repairs that on the next increment while leaving a live window
	// alone, so this stays a fixed window rather than becoming a sliding one.
	// Pipelined to keep the pair at one round trip; both commands share a key, so
	// cluster routing keeps them on one node.
	pipe := r.c.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.ExpireNX(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("valkey incr: %w", err)
	}
	return incr.Val(), nil
}

func (r *clusterClient) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := r.c.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("valkey del: %w", err)
	}
	return nil
}

func (r *clusterClient) Close() error {
	return r.c.Close()
}

// GetJSON reads `key` from Valkey and unmarshals the stored JSON into `out`. Returns ErrCacheMiss
// (wrapped, errors.Is-able) if unset; other failures wrap as "valkey get json: …".
func GetJSON(ctx context.Context, client Client, key string, out any) error {
	raw, err := client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("valkey get json: %w", err)
	}
	if err := json.Unmarshal([]byte(raw), out); err != nil {
		return fmt.Errorf("valkey get json: unmarshal: %w", err)
	}
	return nil
}

// SetJSONWithTTL marshals `value` to JSON and stores it under `key` with the given TTL. Zero ttl stores the key without expiry.
func SetJSONWithTTL(ctx context.Context, client Client, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("valkey set json: marshal: %w", err)
	}
	if err := client.Set(ctx, key, string(data), ttl); err != nil {
		return fmt.Errorf("valkey set json: %w", err)
	}
	return nil
}

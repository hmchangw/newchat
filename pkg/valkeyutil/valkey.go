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
	// IncrEx atomically increments key by 1, returning the post-increment count. ttl applies only
	// on the 0->1 transition (standard fixed-window rate-limit recipe), via INCR + conditional EXPIRE.
	IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Del(ctx context.Context, keys ...string) error
	Close() error
}

// ErrCacheMiss is returned by Get and GetJSON when the key does not exist.
var ErrCacheMiss = errors.New("valkey: cache miss")

type clusterClient struct {
	c *redis.ClusterClient
}

// ConnectCluster dials a Valkey cluster via the provided seed addresses, verifies connectivity with PING, and returns a Client.
func ConnectCluster(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: password,
	})
	if err := instrumentCluster(c, newConnectConfig(opts...)); err != nil {
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed instrument", "error", closeErr)
		}
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.Ping(pingCtx).Err(); err != nil {
		// Close the half-constructed client on the ping-failure path so unreachable addrs don't leak internal go-redis pool state.
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed connect", "error", closeErr)
		}
		return nil, fmt.Errorf("valkey cluster connect: %w", err)
	}
	slog.Info("connected to Valkey cluster", "addrs", addrs)
	return &clusterClient{c: c}, nil
}

// instrumentCluster attaches o11y/redis tracing+metrics hooks when observability is configured.
// o11yredis.Wrap mutates the client in place and is idempotent, registering its own teardown — Disconnect needs no extra handling.
func instrumentCluster(c *redis.ClusterClient, cc connectConfig) error {
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
	n, err := r.c.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("valkey incr: %w", err)
	}
	if n == 1 && ttl > 0 {
		// Only the 0->1 caller sets TTL; failure would let the key persist past the window, so surface it.
		if err := r.c.Expire(ctx, key, ttl).Err(); err != nil {
			return n, fmt.Errorf("valkey incr expire: %w", err)
		}
	}
	return n, nil
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

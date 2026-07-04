// Package valkeyutil provides thin connection + JSON helpers around the
// Valkey (Redis-compatible) client. Modeled on pkg/mongoutil so services
// get a one-call Connect + Disconnect pair plus typed get/set helpers for
// the common JSON-over-Valkey pattern.
//
// The underlying client is go-redis/v9 — Valkey is wire-compatible with
// Redis so no Valkey-specific driver is needed.
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

// Client is the interface exposed by ConnectCluster. Tests can substitute
// their own implementation without depending on go-redis directly.
type Client interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
	Close() error
}

// ErrCacheMiss is returned by Get and GetJSON when the key does not exist.
var ErrCacheMiss = errors.New("valkey: cache miss")

type clusterClient struct {
	c *redis.ClusterClient
}

// ConnectCluster dials a Valkey cluster via the provided seed addresses,
// verifies connectivity with PING, and returns a Client.
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
		// Close the half-constructed client on the ping-failure path so
		// unreachable addrs don't leak internal go-redis pool state.
		if closeErr := c.Close(); closeErr != nil {
			slog.Warn("valkey cluster close after failed connect", "error", closeErr)
		}
		return nil, fmt.Errorf("valkey cluster connect: %w", err)
	}
	slog.Info("connected to Valkey cluster", "addrs", addrs)
	return &clusterClient{c: c}, nil
}

// instrumentCluster attaches o11y/redis tracing and metrics hooks to c when
// observability is configured. o11yredis.Wrap mutates the client in place
// (adding hooks) and is idempotent, registering its own metrics teardown via a
// runtime cleanup — so Disconnect needs no extra handling.
func instrumentCluster(c *redis.ClusterClient, cc connectConfig) error {
	if cc.obs == nil {
		return nil
	}
	if _, err := o11yredis.Wrap(c, cc.obs.TracerProvider(), cc.obs.MeterProvider(), cc.redisOpts...); err != nil {
		return fmt.Errorf("instrument valkey client: %w", err)
	}
	return nil
}

// WrapClusterClient wraps a pre-built *redis.ClusterClient as a Client.
// Intended for tests that need to inject a client configured with a
// ClusterSlots override (testcontainer port-mapping workaround).
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

// GetJSON reads `key` from Valkey and unmarshals the stored JSON into
// `out`. Returns ErrCacheMiss (wrapped) if the key is not set so callers
// can `errors.Is` it; all other failures (transport, malformed JSON) wrap
// as "valkey get json: …".
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

// SetJSONWithTTL marshals `value` to JSON and stores it under `key` with
// the given TTL. Zero ttl stores the key without expiry.
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

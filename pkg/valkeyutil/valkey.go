// Package valkeyutil owns the connection and the shared cache policy for every
// Valkey (Redis-compatible) tier in this repo. Uses go-redis/v9 — Valkey is wire
// compatible, so no separate driver is needed.
//
// Two halves, and callers usually want both:
//
//   - Connection (valkey.go, config.go). Config carries VALKEY_ADDRS and
//     VALKEY_PASSWORD; Connect, ConnectRaw and ConnectOptional are the three dial
//     styles. ConnectOptional degrades to a nil Client, which every tier accepts,
//     for a cache that is an optimisation rather than a startup dependency.
//   - Policy (tier.go, readthrough.go). Tier is the read-through: the refresh
//     window (Fresh), the TTL slide (SlideTTL), invalidation (BustKeys), and the
//     Box envelope every tier stores. Tier.serveHit holds the three outcomes a
//     stale hit can have — source down, confirmed gone, confirmed present — and
//     is the single place the fail-open behaviour is decided.
//
// # Who builds on Tier
//
// Four of the repo's L2 tiers are Tier instances, so reading serveHit once
// explains all four:
//
//	pkg/subauthcache    posting permission     SUB_L2_TTL
//	pkg/roommetacache   room metadata (l2.go)  ROOM_META_L2_TTL
//	pkg/sessioncache    bot session validation SESSION_CACHE_TTL
//	pkg/atrest          wrapped DEKs           ATREST_DEK_L2_TTL
//
// Three cache Valkey without using Tier, each for a stated reason:
//
//	pkg/roomtimescache  NOT a read-through at all — write-on-success /
//	                    read-on-failure, so a healthy request never consults it.
//	pkg/userstore       two key spaces per user plus a bulk MGET path; Tier is
//	                    single-key, single-value.
//	pkg/roomsubcache    room member lists. Lookup.serveHit reimplements this
//	                    package's serveHit and adds singleflight, so a change to
//	                    the policy here does NOT reach it.
//
// # Invariants a new tier must keep
//
// Every tier declares its TTL once, in the package owning the key (a TTLConfig
// field), because two services reading one key on different TTLs is the failure
// no per-service default can prevent. Entries are positive-only where a wrong
// answer grants access. A slide re-arms the deadline with EXPIRE and never SET,
// so an entry a write site deleted in between is not resurrected.
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
	// MGet fetches many keys at once, returning only the ones that were present
	// — an absent key is simply missing from the map, since a cache miss is not
	// an error. Prefer it over a Get loop for any lookup whose key count is
	// driven by input (a mention list, a member list): in cluster mode the keys
	// span slots, so a loop costs one serialized round-trip each, while this
	// costs roughly one per node regardless of key count.
	MGet(ctx context.Context, keys []string) (map[string]string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	// MSet stores many entries under one TTL in a single round trip. It is on
	// the interface, not an optional capability, because a client that lacked it
	// would silently degrade a bulk fill to one serialized round trip per key on
	// the message hot path — where the key count is the size of a mention list,
	// which the sender chooses — and nothing would say so at compile time.
	MSet(ctx context.Context, entries []KV, ttl time.Duration) error
	// SetNX atomically sets key to value with ttl iff key is absent: (true,nil) acquired,
	// (false,nil) refused, (false,err) transport failure. ttl must be > 0 — a zero ttl stores without expiry.
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// IncrEx atomically increments key by 1, returning the post-increment count. ttl applies only
	// on the 0->1 transition (standard fixed-window rate-limit recipe), via INCR + conditional EXPIRE.
	IncrEx(ctx context.Context, key string, ttl time.Duration) (int64, error)
	Del(ctx context.Context, keys ...string) error
	// Expire re-arms an existing key's TTL without touching its value, reporting
	// whether the key existed. Prefer it over a re-Set when only the deadline
	// should move: a re-Set would resurrect a key deleted since it was read, and
	// would clobber a value another writer updated in the meantime.
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	Close() error
}

// ErrCacheMiss is returned by Get and GetJSON when the key does not exist.
var ErrCacheMiss = errors.New("valkey: cache miss")

type clusterClient struct {
	c *redis.ClusterClient
}

// ConnectCluster dials a Valkey cluster via the provided seed addresses, verifies connectivity with PING, and returns a Client.
func ConnectCluster(ctx context.Context, addrs []string, password string, opts ...Option) (Client, error) {
	c, err := dialCluster(ctx, addrs, password, opts...)
	if err != nil {
		return nil, err
	}
	return &clusterClient{c: c}, nil
}

// dialCluster is the shared dial: construct, instrument, PING, or clean up. Both
// the Client-returning and the raw-*redis.ClusterClient-returning entry points
// go through it, so a caller that needs the concrete type does not re-implement
// the setup — three services had, each with its own instrumentation.
func dialCluster(ctx context.Context, addrs []string, password string, opts ...Option) (*redis.ClusterClient, error) {
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
	return c, nil
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

// Cluster exposes the underlying *redis.ClusterClient behind a Client, or nil
// if the Client did not come from ConnectCluster/WrapClusterClient.
//
// It exists for the few callers needing commands outside the Client interface
// — a keyspace walk (SCAN + MEMORY USAGE) is the motivating case. Widening
// Client instead would force every implementation and test fake in the repo to
// grow methods it has no use for, so the escape hatch is narrower than the
// interface change it replaces. Prefer the Client interface everywhere else.
func Cluster(client Client) *redis.ClusterClient {
	cc, ok := client.(*clusterClient)
	if !ok {
		return nil
	}
	return cc.c
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

// MGet issues one GET per key through a pipeline. go-redis groups the pipelined
// commands by node and runs the groups concurrently, so cross-slot keys — which
// a plain MGET would reject — cost about one round-trip per node.
func (r *clusterClient) MGet(ctx context.Context, keys []string) (map[string]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	cmds := make([]*redis.StringCmd, len(keys))
	_, err := r.c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, k := range keys {
			cmds[i] = p.Get(ctx, k)
		}
		return nil
	})
	// Pipelined surfaces the first command error, and redis.Nil is the expected
	// answer for any absent key — only a genuine transport failure is an error.
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("valkey mget: %w", err)
	}
	out := make(map[string]string, len(keys))
	for i, cmd := range cmds {
		val, cmdErr := cmd.Result()
		if cmdErr != nil {
			continue // absent, or unreadable — both are a miss to the caller
		}
		out[keys[i]] = val
	}
	return out, nil
}

func (r *clusterClient) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := r.c.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("valkey set: %w", err)
	}
	return nil
}

// MSet stores every entry through a pipeline, the write-side counterpart of
// MGet: go-redis groups the commands by node and runs the groups concurrently,
// so entries spanning slots cost about one round trip per node rather than one
// per key. Satisfies the optional multiSetter capability behind SetMany.
func (r *clusterClient) MSet(ctx context.Context, entries []KV, ttl time.Duration) error {
	if len(entries) == 0 {
		return nil
	}
	if _, err := r.c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, e := range entries {
			p.Set(ctx, e.Key, e.Value, ttl)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("valkey mset: %w", err)
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

// Del issues one DEL per key through a pipeline, for the same reason MGet does:
// go-redis groups the pipelined commands by node and runs the groups
// concurrently, so keys spanning slots — which a single multi-key DEL rejects
// with CROSSSLOT, clearing none of them — cost about one round-trip per node.
// Callers may therefore hand over any key set without grouping it first.
func (r *clusterClient) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	// A missing key is not an error — DEL reports a count — so any error here is
	// a genuine transport or server failure.
	if _, err := r.c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, k := range keys {
			p.Del(ctx, k)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("valkey del: %w", err)
	}
	return nil
}

func (r *clusterClient) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	existed, err := r.c.Expire(ctx, key, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("valkey expire: %w", err)
	}
	return existed, nil
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

// CacheRecorder records the outcome of an L2 cache lookup. Every cache package's
// own Recorder satisfies it structurally, so callers pass theirs unchanged.
type CacheRecorder interface {
	Hit(ctx context.Context)
	Miss(ctx context.Context)
	Error(ctx context.Context)
}

// ReadCachedJSON GETs key, unmarshals the stored JSON into T, and records the
// outcome. It is the shared read half of every read-through cache tier in this
// repo, which otherwise reimplement the same five branches and drift apart.
//
// ok is true only for a usable hit. There are three ways to not have one, and
// they are deliberately not the same thing to the metrics:
//
//   - A clean miss records Miss.
//   - A decoded value that fails valid records Miss and warns. Any well-formed
//     JSON that is not a T unmarshals to the zero value, so without this an
//     entry of "null" — or a foreign value written under the same key — is
//     served as a real one for the rest of its TTL. What "usable" means is the
//     caller's to decide, hence the predicate; a nil valid accepts anything
//     that decodes.
//   - A transport or decode failure records Error and warns.
//
// All three return ok=false, so the caller falls through to its source of
// truth: this half is fail-open by construction and never returns an error.
//
// label names the cache in log messages; logAttrs are appended to them so each
// caller keeps its own structured fields.
func ReadCachedJSON[T any](ctx context.Context, client Client, key, label string,
	rec CacheRecorder, valid func(*T) bool, logAttrs ...any,
) (T, bool) {
	var cached T
	err := GetJSON(ctx, client, key, &cached)
	return decodeCached(ctx, cached, err, label, rec, valid, logAttrs...)
}

// DecodeCachedJSON applies ReadCachedJSON's outcome rules to a raw entry a
// caller already has in hand — the MGet path, where the fetch is one round trip
// for many keys and so cannot go through GetJSON. An empty raw means the key
// was absent.
//
// It exists so the bulk path and the single-key path cannot disagree about what
// counts as a hit, a miss or an error, or about the validity rule. They had
// already drifted in their log wording while both claiming to be identical.
func DecodeCachedJSON[T any](ctx context.Context, raw, label string,
	rec CacheRecorder, valid func(*T) bool, logAttrs ...any,
) (T, bool) {
	var cached T
	// An absent key is reported as a clean miss, exactly as GetJSON does, so the
	// two paths land on the same branch of the shared outcome table.
	err := ErrCacheMiss
	if raw != "" {
		err = json.Unmarshal([]byte(raw), &cached)
	}
	return decodeCached(ctx, cached, err, label, rec, valid, logAttrs...)
}

// decodeCached is the shared outcome table: hit, failed-validation, clean miss,
// or read/decode error.
func decodeCached[T any](ctx context.Context, cached T, err error, label string,
	rec CacheRecorder, valid func(*T) bool, logAttrs ...any,
) (T, bool) {
	var zero T
	switch {
	case err == nil && (valid == nil || valid(&cached)):
		rec.Hit(ctx)
		return cached, true
	case err == nil:
		rec.Miss(ctx)
		slog.WarnContext(ctx, label+" L2 entry failed validation, treating as miss", logAttrs...)
		return zero, false
	case errors.Is(err, ErrCacheMiss):
		rec.Miss(ctx)
		return zero, false
	default:
		rec.Error(ctx)
		attrs := make([]any, 0, len(logAttrs)+2)
		attrs = append(attrs, logAttrs...)
		attrs = append(attrs, "error", err)
		slog.WarnContext(ctx, label+" L2 read failed, falling back to the source of truth", attrs...)
		return zero, false
	}
}

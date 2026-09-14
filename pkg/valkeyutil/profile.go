package valkeyutil

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Profile is a bounded timeout budget for a Valkey client.
//
// The dialer previously passed only Addrs and Password, so go-redis cluster
// defaults applied: a 3s read/write ceiling, and — the load-bearing part —
// ContextTimeoutEnabled left false, meaning a caller's context deadline did not
// bound socket reads at all. Against a Valkey that blackholes packets (the
// common degraded mode, as opposed to one that refuses connections) every
// hot-path read stalled for seconds before falling through to Mongo or
// Elasticsearch. The consumers were fail-open; the network behaviour was
// fail-slow.
//
// These are code constants rather than environment variables by design. They
// are internal tuning, nobody will set them correctly under incident pressure,
// and a wrong value silently reintroduces the stall this exists to prevent.
//
// Note the cluster default for MaxRetries is -1 (no retries at all — see
// ClusterOptions.init), so a profile's retries are additive: worst case is
// (MaxRetries+1) x ReadTimeout plus backoff, not ReadTimeout alone.
type Profile struct {
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration
	MaxRetries   int
}

var (
	// CacheProfile serves consumers that have a backing store to fall through to
	// (room meta, room subscriptions, search restricted-rooms, the session L2).
	// Failing fast matters more than succeeding slowly — the fallback is always
	// there, so a slow success is strictly worse than a quick miss.
	CacheProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		PoolTimeout:  250 * time.Millisecond,
		MaxRetries:   1,
	}

	// StoreProfile serves user-presence-service, where Valkey is the store of
	// record and no fallback exists. A cache-tight ceiling on a Lua EVAL under
	// load would manufacture failures that nothing downstream can absorb.
	StoreProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolTimeout:  time.Second,
		MaxRetries:   2,
	}
)

// ClusterOptionsFor builds the go-redis cluster options for a profile.
func ClusterOptionsFor(addrs []string, password string, p Profile) *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		DialTimeout:  p.DialTimeout,
		ReadTimeout:  p.ReadTimeout,
		WriteTimeout: p.WriteTimeout,
		PoolTimeout:  p.PoolTimeout,
		MaxRetries:   p.MaxRetries,
		// Without this go-redis ignores the caller's context deadline for socket
		// reads and only ReadTimeout applies, which makes the rest decorative.
		ContextTimeoutEnabled: true,
	}
}

// WithProfile selects the timeout budget for the client. Defaults to
// CacheProfile, which is the right answer for every consumer that has a
// fallback; user-presence-service passes StoreProfile because Valkey is its
// store of record rather than a cache.
func WithProfile(p Profile) Option {
	return func(c *connectConfig) { c.profile = p }
}

// WithRequireReachable makes the startup PING fatal, so an unreachable cluster
// fails the dial instead of returning a usable client.
//
// It is off by default, and deliberately so. A shared datastore is the same for
// every replica, so gating startup on its reachability means a Valkey outage
// overlapping a rollout, autoscale or node drain crashloops every pod at once —
// including the message path — and the crashloop outlives the outage. go-redis
// dials lazily and self-heals per call, so a pod that starts during an outage
// recovers on its own once Valkey returns.
//
// The caller that wants this is the one-shot CLI: tools/seed-sample-data has no
// fallback and no next call to self-heal into, so aborting the run is right.
func WithRequireReachable() Option {
	return func(c *connectConfig) { c.requireReachable = true }
}

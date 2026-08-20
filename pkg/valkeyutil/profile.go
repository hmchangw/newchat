package valkeyutil

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// Profile is a bounded timeout budget for a Valkey client. go-redis cluster
// defaults (5s dial, 3s read/write, and MaxRetries -1 — no retries at all, see
// ClusterOptions.init) are far too loose for a cache on the message hot path:
// against a blackholing Valkey they turn a fail-open read into a multi-second
// stall on every call.
//
// Note these profiles ADD retries where the cluster default has none, so a
// profile's worst-case per-command latency is (MaxRetries+1) x ReadTimeout plus
// backoff, not ReadTimeout alone.
//
// These are code constants rather than environment variables by design (see
// the spec). They are internal tuning, and a wrong value silently reintroduces
// the stall this package exists to prevent.
type Profile struct {
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolTimeout  time.Duration
	MaxRetries   int
}

var (
	// CacheProfile serves consumers that have a backing store to fall back to
	// (room meta, room subscriptions, search restricted-rooms). Failing fast
	// matters more than succeeding slowly — the fallback is always available.
	CacheProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  150 * time.Millisecond,
		WriteTimeout: 150 * time.Millisecond,
		PoolTimeout:  250 * time.Millisecond,
		MaxRetries:   1,
	}

	// StoreProfile serves user-presence-service, where Valkey is the store of
	// record and no fallback exists. A cache-tight ceiling on a Lua EVAL under
	// load would manufacture failures nothing can absorb.
	StoreProfile = Profile{
		DialTimeout:  time.Second,
		ReadTimeout:  500 * time.Millisecond,
		WriteTimeout: 500 * time.Millisecond,
		PoolTimeout:  time.Second,
		MaxRetries:   2,
	}
)

// ClusterOptionsFor builds the go-redis cluster options for a profile.
// Exported so user-presence-service/presencestore, which constructs its own
// client for Lua scripting, applies the same budget without duplicating it.
func ClusterOptionsFor(addrs []string, password string, p Profile) *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:        addrs,
		Password:     password,
		DialTimeout:  p.DialTimeout,
		ReadTimeout:  p.ReadTimeout,
		WriteTimeout: p.WriteTimeout,
		PoolTimeout:  p.PoolTimeout,
		MaxRetries:   p.MaxRetries,
		// Without this, go-redis ignores the caller's context deadline for
		// socket reads and only ReadTimeout applies.
		ContextTimeoutEnabled: true,
	}
}

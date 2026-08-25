package mongoutil

import (
	"fmt"
	"time"
)

// PoolConfig is an env-tagged MongoDB connection-pool configuration. Add it as a
// named field to a service's config (e.g. `Pool mongoutil.PoolConfig`), call
// Validate() during config load, and pass WithPool(cfg.Pool) to Connect — so
// every service's pool ceiling is explicit and tunable rather than the driver
// default of 100. The env tags carry the full MONGO_ prefix so the field works
// whether it sits in a flat config or alongside a MONGO_-prefixed block.
//
// The 150 default sits just above a mongod node's ~128 WiredTiger read-ticket
// ceiling: enough headroom to keep tickets saturated without over-provisioning
// connections fleet-wide. A high-throughput service whose in-flight handler cap
// exceeds this (e.g. history-service, MAX_CONCURRENCY=256) should raise
// MONGO_MAX_POOL_SIZE in its own deployment. Connections are created lazily and,
// in a request/reply service, further gated by the handler cap, so this is a
// ceiling, not a count of connections opened. Size pods × MaxPoolSize under the
// cluster's connection limit — and note a service that opens separate read and
// write clients applies this cap to each, so its ceiling is 2 × MaxPoolSize.
type PoolConfig struct {
	MaxPoolSize uint64 `env:"MONGO_MAX_POOL_SIZE" envDefault:"150"`
	MinPoolSize uint64 `env:"MONGO_MIN_POOL_SIZE" envDefault:"0"`
	// MaxIdleTime reaps connections idle this long. Needed explicitly: the driver
	// reads its own default of 0 as NEVER reap, so a pool grown during a burst
	// holds those sockets for the life of the process, and a failover re-growing
	// them on another member adds to the total rather than replacing it. 0 keeps
	// the never-reap behaviour for a caller that wants it.
	MaxIdleTime time.Duration `env:"MONGO_MAX_IDLE_TIME" envDefault:"5m"`
	// ServerSelectionTimeout bounds how long an operation waits for a usable
	// server. A stopped MongoDB does not refuse connections, it goes quiet, and
	// the driver's 30s default turns that silence into a hang — long enough that
	// a caller's own deadline expires first and every fail-open path downstream
	// never runs. Two seconds makes an outage reportable while the request still
	// has budget to serve a cached fallback.
	//
	// It rides here rather than in each service's own config so adopting the
	// shared pool field carries the bound with it. 0 leaves the URI/driver value
	// alone; a negative value is refused, because the driver reads <= 0 as no
	// bound at all — the very hang this exists to prevent.
	ServerSelectionTimeout time.Duration `env:"MONGO_SERVER_SELECTION_TIMEOUT" envDefault:"2s"`
}

// WithPool is a Connect option that applies this pool configuration, so call
// sites read `Connect(ctx, uri, u, p, mongoutil.WithPool(cfg.Pool), ...)` rather
// than hand-splicing pool options into the variadic. MaxPoolSize is always
// applied (authoritative, validated >= 1); MinPoolSize is applied only when > 0,
// so an unset floor leaves a minPoolSize from the URI (or the driver default)
// intact rather than forcing it to 0.
func WithPool(p PoolConfig) Option {
	return func(c *connectConfig) {
		c.maxPoolSize = &p.MaxPoolSize
		if p.MinPoolSize > 0 {
			c.minPoolSize = &p.MinPoolSize
		}
		if p.MaxIdleTime > 0 {
			c.maxIdleTime = &p.MaxIdleTime
		}
		if p.ServerSelectionTimeout > 0 {
			c.serverSelectionTimeout = &p.ServerSelectionTimeout
		}
	}
}

// Validate rejects a zero max (the driver reads 0 as unbounded), a min above
// max, and a negative server-selection timeout (also read as unbounded).
func (p PoolConfig) Validate() error {
	if p.MaxPoolSize < 1 {
		return fmt.Errorf("MONGO_MAX_POOL_SIZE must be >= 1, got %d", p.MaxPoolSize)
	}
	if p.MinPoolSize > p.MaxPoolSize {
		return fmt.Errorf("MONGO_MIN_POOL_SIZE (%d) must be <= MONGO_MAX_POOL_SIZE (%d)", p.MinPoolSize, p.MaxPoolSize)
	}
	if p.MaxIdleTime < 0 {
		return fmt.Errorf("MONGO_MAX_IDLE_TIME must be >= 0, got %s", p.MaxIdleTime)
	}
	if p.ServerSelectionTimeout < 0 {
		return fmt.Errorf("MONGO_SERVER_SELECTION_TIMEOUT must be >= 0, got %s", p.ServerSelectionTimeout)
	}
	return nil
}

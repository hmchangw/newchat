package mongoutil

import "fmt"

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
}

// WithPool is a Connect option that applies this pool configuration, so call
// sites read `Connect(ctx, uri, u, p, mongoutil.WithPool(cfg.Pool), ...)` rather
// than hand-splicing pool options into the variadic.
//
// Both limits are authoritative, an explicit zero minimum included, so Validate()
// checks exactly the pair that reaches the driver. Applying only the maximum
// would leave a minPoolSize from the URI in force: a URI floor above the
// configured ceiling passes Validate() as {0, max} but reaches the driver as
// {uriMin, max}, which it rejects at construction — the pod then fails to start
// on a rollout that only lowered the maximum. It also leaves MONGO_MIN_POOL_SIZE=0
// unable to clear a URI floor.
func WithPool(p PoolConfig) Option {
	return func(c *connectConfig) {
		c.maxPoolSize = &p.MaxPoolSize
		c.minPoolSize = &p.MinPoolSize
	}
}

// Validate rejects a zero max (the driver reads 0 as unbounded) and a min above max.
func (p PoolConfig) Validate() error {
	if p.MaxPoolSize < 1 {
		return fmt.Errorf("MONGO_MAX_POOL_SIZE must be >= 1, got %d", p.MaxPoolSize)
	}
	if p.MinPoolSize > p.MaxPoolSize {
		return fmt.Errorf("MONGO_MIN_POOL_SIZE (%d) must be <= MONGO_MAX_POOL_SIZE (%d)", p.MinPoolSize, p.MaxPoolSize)
	}
	return nil
}

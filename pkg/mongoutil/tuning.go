package mongoutil

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// WithMaxPoolSize caps the connection pool per server. Unbounded handler
// concurrency against an uncapped pool (driver default 100) is a common route
// to pool exhaustion, so callers set this explicitly rather than relying on the
// driver default or a URI query parameter.
func WithMaxPoolSize(n uint64) Option {
	return func(c *connectConfig) { c.maxPoolSize = &n }
}

// WithMinPoolSize keeps a warm floor of connections so bursts don't pay
// establishment latency (throttled by maxConnecting) on every cold checkout.
func WithMinPoolSize(n uint64) Option {
	return func(c *connectConfig) { c.minPoolSize = &n }
}

// WithMaxIdleTime reaps connections idle this long. The driver's default of 0
// means never, so a pool that peaked during a burst holds those sockets for the
// life of the process — and MaxPoolSize is per server, so the total is the cap
// times the replica-set members the client has talked to.
func WithMaxIdleTime(d time.Duration) Option {
	return func(c *connectConfig) { c.maxIdleTime = &d }
}

// applyTuning writes the pool settings onto clientOpts. Nil fields are skipped
// so an unset option never clobbers a URI-provided or default value.
func (c connectConfig) applyTuning(clientOpts *options.ClientOptions) {
	if c.maxPoolSize != nil {
		clientOpts.SetMaxPoolSize(*c.maxPoolSize)
	}
	if c.minPoolSize != nil {
		clientOpts.SetMinPoolSize(*c.minPoolSize)
	}
	if c.maxIdleTime != nil {
		clientOpts.SetMaxConnIdleTime(*c.maxIdleTime)
	}
}

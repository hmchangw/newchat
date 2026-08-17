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

// WithServerSelectionTimeout bounds how long a read waits for a usable server
// before failing. The driver default is 30s, which is longer than any request
// budget in this system — so against a stopped MongoDB a read does not return
// an error, it consumes the caller's entire deadline and the request dies
// somewhere downstream.
//
// That distinction is what every fail-open path here depends on: falling back
// to a cached answer only works if the read FAILS, and fails with time left to
// serve the fallback. Set this well under the caller's request timeout.
//
// The trade-off is deliberate: a bound this short also trips during a replica
// set election rather than waiting it out. For a service fronted by a cache and
// a circuit breaker that is the better failure — it serves cached data and
// recovers on the breaker's next probe, instead of stalling every caller.
//
// Overrides any serverSelectionTimeoutMS in the connection URI.
func WithServerSelectionTimeout(d time.Duration) Option {
	return func(c *connectConfig) { c.serverSelectionTimeout = &d }
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
	if c.serverSelectionTimeout != nil {
		clientOpts.SetServerSelectionTimeout(*c.serverSelectionTimeout)
	}
}

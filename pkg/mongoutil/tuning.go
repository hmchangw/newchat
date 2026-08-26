package mongoutil

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
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

// WithWriteConcern binds a client-level write concern. Nil is a no-op, so a
// caller can pass one through from config without branching.
//
// Set it wherever an acknowledgement to something outside MongoDB is issued on
// the strength of a write having landed — a JetStream Ack after a BulkWrite,
// say. Without it the concern comes from the connection URI or the cluster's
// own default, neither of which the service can see, and a w=1 write that a
// primary failover rolls back is one the worker has already acked and will
// never redeliver. Left unset the driver sends no concern and the server
// default applies.
func WithWriteConcern(wc *writeconcern.WriteConcern) Option {
	return func(c *connectConfig) { c.writeConcern = wc }
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
	if c.serverSelectionTimeout != nil {
		clientOpts.SetServerSelectionTimeout(*c.serverSelectionTimeout)
	}
	if c.writeConcern != nil {
		clientOpts.SetWriteConcern(c.writeConcern)
	}
}

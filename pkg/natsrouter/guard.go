package natsrouter

import (
	"fmt"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
)

// GuardConfig is an env-tagged bundle of the two request/reply overload guards:
// an in-flight handler cap and a per-request timeout. Add it as a named field to
// a service's config (e.g. `Guard natsrouter.GuardConfig`), call Validate()
// during config load, pass Options() to New, and Use() the TimeoutMiddleware()
// after the standard middleware.
//
// Together they keep a burst or a slow dependency from spawning unbounded
// handlers and holding pooled connections: excess requests are shed with
// ErrUnavailable, and a stalled op is cancelled so its connection is returned.
type GuardConfig struct {
	// MaxConcurrency caps concurrent in-flight handlers; 0 disables the cap.
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"256"`
	// RequestTimeout bounds each handler so a slow dependency op is cancelled
	// and its connection released; 0 disables the deadline.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`
}

// Validate rejects negative values; zero is the documented disable value for both.
func (g GuardConfig) Validate() error {
	if g.MaxConcurrency < 0 {
		return fmt.Errorf("MAX_CONCURRENCY must be >= 0, got %d", g.MaxConcurrency)
	}
	if g.RequestTimeout < 0 {
		return fmt.Errorf("REQUEST_TIMEOUT must be >= 0, got %s", g.RequestTimeout)
	}
	return nil
}

// Options returns the router construction options for this guard (the admission
// cap when MaxConcurrency > 0, none otherwise).
func (g GuardConfig) Options() []Option {
	var opts []Option
	if g.MaxConcurrency > 0 {
		opts = append(opts, WithMaxConcurrency(g.MaxConcurrency))
	}
	return opts
}

// TimeoutMiddleware returns the per-request timeout middleware (empty when
// RequestTimeout is 0), suitable for spreading into Use().
func (g GuardConfig) TimeoutMiddleware() []HandlerFunc {
	if g.RequestTimeout > 0 {
		return []HandlerFunc{HandlerTimeout(g.RequestTimeout)}
	}
	return nil
}

// DefaultGuarded builds a Router with the standard middleware chain (Default:
// Recovery, RequestID, Logging) plus this guard's admission cap and per-request
// timeout, wired in the correct order. It exists so a service can't apply only
// half of the overload protection — the cap (a construction option) and the
// timeout (post-middleware) are otherwise two separate calls that are easy to
// forget one of. Services needing a non-standard middleware order keep New.
//
// opts are the caller's own construction options (WithSiteID, WithMetrics, …),
// applied alongside the guard's. They are variadic rather than absent so that
// adopting the helper never costs a service an option it already installed.
func DefaultGuarded(nc *o11ynats.Conn, queue string, g GuardConfig, opts ...Option) *Router {
	r := Default(nc, queue, append(g.Options(), opts...)...)
	r.Use(g.TimeoutMiddleware()...)
	return r
}

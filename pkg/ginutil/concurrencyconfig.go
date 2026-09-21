package ginutil

import (
	"fmt"

	"github.com/gin-gonic/gin"
)

// ConcurrencyConfig is an env-tagged in-flight request cap, the admission-control
// sibling of TimeoutConfig. Add it as a named field to an HTTP service's config,
// call Validate() during config load, and apply Middleware() to the routes that
// need shedding — so the field, its validation and the wiring live in one place
// instead of being re-declared per service.
//
// The default is deliberately low. The endpoints this exists for spend ~50-100ms
// of CPU per request in bcrypt before any credential is known to be valid, so the
// useful cap is a small multiple of the core count, not a connection-pool-sized
// number. Services that want a different ceiling set MAX_CONCURRENCY explicitly.
type ConcurrencyConfig struct {
	// MaxConcurrency caps in-flight requests; overflow is shed with 429 +
	// Retry-After rather than queued. 0 disables the cap.
	MaxConcurrency int `env:"MAX_CONCURRENCY" envDefault:"32"`
}

// Validate rejects a negative cap; zero is the documented disable value.
func (c ConcurrencyConfig) Validate() error {
	if c.MaxConcurrency < 0 {
		return fmt.Errorf("MAX_CONCURRENCY must be >= 0, got %d", c.MaxConcurrency)
	}
	return nil
}

// Middleware returns the admission-control middleware (a no-op when disabled).
// onShed is called once per rejection on the request goroutine and must not
// block; nil is allowed.
func (c ConcurrencyConfig) Middleware(onShed func()) gin.HandlerFunc {
	return MaxConcurrency(c.MaxConcurrency, onShed)
}

package ginutil

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
)

// TimeoutConfig is an env-tagged per-request HTTP timeout, the Gin-side parallel
// of natsrouter.GuardConfig's timeout half. Add it as a named field to an HTTP
// service's config, call Validate() during config load, and Use(Middleware()) on
// the engine — so the field, its validation, and the wiring live in one place
// instead of being re-declared per service.
type TimeoutConfig struct {
	// RequestTimeout bounds each request so a slow downstream op is cancelled and
	// its connection released; 0 disables the deadline.
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`
}

// Validate rejects a negative timeout; zero is the documented disable value.
func (c TimeoutConfig) Validate() error {
	if c.RequestTimeout < 0 {
		return fmt.Errorf("REQUEST_TIMEOUT must be >= 0, got %s", c.RequestTimeout)
	}
	return nil
}

// Middleware returns the per-request timeout middleware (a no-op when disabled).
func (c TimeoutConfig) Middleware() gin.HandlerFunc {
	return Timeout(c.RequestTimeout)
}

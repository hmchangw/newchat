package ginutil

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// defaultRetryAfter is long enough to let a burst drain, short enough that a
// client init still feels live.
const defaultRetryAfter = time.Second

type limiterConfig struct {
	retryAfter time.Duration
	onShed     func()
}

// LimiterOption configures MaxConcurrency.
type LimiterOption func(*limiterConfig)

// WithRetryAfter overrides the Retry-After advertised on a shed request.
func WithRetryAfter(d time.Duration) LimiterOption {
	return func(c *limiterConfig) { c.retryAfter = d }
}

// WithOnShed registers a per-shed callback for metrics. It runs on the request
// goroutine, so it must not block.
func WithOnShed(f func()) LimiterOption {
	return func(c *limiterConfig) { c.onShed = f }
}

// MaxConcurrency caps in-flight requests at n, shedding the overflow with 429 +
// Retry-After. n <= 0 disables the cap.
//
// Acquire is non-blocking by design: queueing a request whose client has already
// given up converts a burst into wasted work and retained memory. 429 rather
// than 503 because service-mesh outlier detection ejects hosts on consecutive
// 5xx, which would shrink the fleet exactly when it is busiest.
func MaxConcurrency(n int, opts ...LimiterOption) gin.HandlerFunc {
	if n <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	cfg := limiterConfig{retryAfter: defaultRetryAfter}
	for _, opt := range opts {
		opt(&cfg)
	}
	sem := make(chan struct{}, n)

	return func(c *gin.Context) {
		select {
		case sem <- struct{}{}:
			// Deferred so a panicking handler cannot leak the slot.
			defer func() { <-sem }()
			c.Next()
		default:
			if cfg.onShed != nil {
				cfg.onShed()
			}
			// errhttp.Write sets no headers, so Retry-After has to be set here.
			c.Header("Retry-After", strconv.Itoa(int(cfg.retryAfter.Seconds())))
			errhttp.Write(c.Request.Context(), c, errcode.TooManyRequests(
				"server is at capacity, retry shortly",
				errcode.WithReason(errcode.UserOverloaded)))
			c.Abort()
		}
	}
}

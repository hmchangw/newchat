package ginutil

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
)

// retryAfterSeconds is long enough to let a burst drain, short enough that a
// client init still feels live.
const retryAfterSeconds = "1"

// MaxConcurrency caps in-flight requests at n, shedding the overflow with 429 +
// Retry-After. n <= 0 disables the cap. onShed, if non-nil, is called once per
// rejection for metrics; it runs on the request goroutine and must not block.
//
// Acquire is non-blocking by design: queueing a request whose client has already
// given up converts a burst into wasted work and retained memory. 429 rather
// than 503 because service-mesh outlier detection ejects hosts on consecutive
// 5xx, which would shrink the fleet exactly when it is busiest.
func MaxConcurrency(n int, onShed func()) gin.HandlerFunc {
	if n <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	sem := make(chan struct{}, n)
	// Built once: the envelope is constant, and it is read-only from here on.
	shedErr := errcode.TooManyRequests("server is at capacity, retry shortly",
		errcode.WithReason(errcode.Overloaded))
	return func(c *gin.Context) {
		select {
		case sem <- struct{}{}:
			// Deferred so a panicking handler cannot leak the slot.
			defer func() { <-sem }()
			c.Next()
		default:
			if onShed != nil {
				onShed()
			}
			// Deliberately not errhttp.Write: it runs Classify, which logs once per
			// error. Under the burst this exists to survive that is one log line per
			// rejected request, spending I/O the pod has already run out of. The shed
			// counter is the signal instead — a metric to alert on, not a log flood.
			c.Header("Retry-After", retryAfterSeconds)
			c.AbortWithStatusJSON(shedErr.HTTPStatus(), shedErr)
		}
	}
}

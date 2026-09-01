package ginutil

import (
	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
)

// retryAfterSeconds is long enough to let a burst drain, short enough that a
// client init still feels live.
const retryAfterSeconds = "1"

// MaxConcurrency caps in-flight requests at n and sheds the overflow with 429 +
// Retry-After; n <= 0 disables it. onShed is called once per rejection, on the
// request goroutine, so it must not block.
//
// Acquire is non-blocking: queueing a request whose client has already given up
// is wasted work. 429 not 503, because mesh outlier detection ejects hosts on
// consecutive 5xx — shrinking the fleet exactly when it is busiest.
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
			// Not errhttp.Write: Classify would add a second log line per rejection on
			// top of AccessLog's. The shed counter is the signal to alert on.
			c.Header("Retry-After", retryAfterSeconds)
			c.AbortWithStatusJSON(shedErr.HTTPStatus(), shedErr)
		}
	}
}

package ginutil

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
)

// Timeout bounds a request's work with a deadlined context, so handlers that check
// ctx stop early instead of running to completion for a client that has gone.
//
// It writes no response of its own: the handler observes the cancellation and
// chooses its own error envelope. d <= 0 disables it.
func Timeout(d time.Duration) gin.HandlerFunc {
	if d <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

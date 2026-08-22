package ginutil

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
)

// Timeout bounds each request's work by replacing its context with a deadlined
// one. Handlers that check ctx between steps stop early when the client is gone
// instead of running the whole way to completion for nobody.
//
// It does not itself write a response on expiry: the handler observes the
// cancellation and returns its own error, so the error envelope stays the
// handler's to choose. d <= 0 disables it.
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

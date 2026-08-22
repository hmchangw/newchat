package ginutil

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
)

// Timeout bounds each request by deriving a context deadline on the request, so
// a slow downstream op (e.g. a MongoDB query) is cancelled and its pooled
// connection released rather than held until the pool starves. It complements
// the http.Server read/write timeouts, which bound the socket but do not cancel
// the handler's context. A non-positive duration is a no-op.
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

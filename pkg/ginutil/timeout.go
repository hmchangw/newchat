package ginutil

import (
	"context"
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/errcode/errhttp"
)

// Timeout bounds each request by deriving a context deadline on the request, so
// a slow downstream op (e.g. a MongoDB query) is cancelled and its pooled
// connection released rather than held until the pool starves. It complements
// the http.Server read/write timeouts, which bound the socket but do not cancel
// the handler's context. A non-positive duration is a no-op.
//
// Cancelling the context is not on its own enough: a handler that returns
// without translating the expiry falls through to Gin's implicit 200, reporting
// success over work that was aborted mid-flight. So an expired request that
// flushed nothing is answered with the unavailable envelope instead. A handler
// that already committed a response keeps it — its bytes are on the wire.
func Timeout(d time.Duration) gin.HandlerFunc {
	if d <= 0 {
		return func(c *gin.Context) { c.Next() }
	}
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), d)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && !c.Writer.Written() {
			errhttp.Write(ctx, c, errcode.Unavailable("request timed out"))
		}
	}
}

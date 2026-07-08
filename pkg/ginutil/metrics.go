package ginutil

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// errCodeKey mirrors errhttp.ErrCodeKey by value to avoid an import edge from
// ginutil to errhttp. errhttp.Write stores the classified Code here.
const errCodeKey = "errcode"

// Metrics returns middleware that records rpc_server_requests_total and
// rpc_server_request_duration_seconds for each matched HTTP route. The `route`
// label is the registered template (c.FullPath()), never the live URL. The
// `status` label is the errcode Code stamped by errhttp.Write when present,
// else an HTTP-status class (2xx->ok, 4xx->bad_request, 5xx->internal).
// Unmatched routes (empty FullPath) and /healthz are skipped.
func Metrics(service string) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := c.FullPath()
		if route == "" || route == "/healthz" {
			return
		}
		rpcmetrics.Observe(service, route, statusForHTTP(c), time.Since(start))
	}
}

// statusForHTTP prefers the errcode Code stamped by errhttp.Write; otherwise it
// derives a coarse class from the HTTP status code so plain responses still map
// onto the shared status taxonomy.
func statusForHTTP(c *gin.Context) string {
	if v, ok := c.Get(errCodeKey); ok {
		if code, ok := v.(string); ok && code != "" {
			return code
		}
	}
	switch code := c.Writer.Status(); {
	case code >= 500:
		return "internal"
	case code >= 400:
		return "bad_request"
	default:
		return "ok"
	}
}

package natsrouter

import (
	"time"

	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// Metrics returns middleware that records rpc_server_requests_total and
// rpc_server_request_duration_seconds for every request handled by this
// router. The `route` label is the matched pattern (c.Route()); the `status`
// label is the terminal status stamped by the Register* wrappers (c.Status(),
// defaulting to "ok"). Install once via r.Use — placement relative to other
// middleware only shifts what latency window is measured; place it outermost
// (first) to capture the full chain.
func Metrics(service string) HandlerFunc {
	return func(c *Context) {
		start := time.Now()
		c.Next()
		rpcmetrics.Observe(service, c.Route(), c.Status(), time.Since(start))
	}
}

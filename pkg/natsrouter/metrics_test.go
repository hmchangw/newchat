package natsrouter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/rpcmetrics"
)

// runChain drives a middleware+handler chain against a bare Context, the same
// way acquireContext wires `all` in addRoute.
func runChain(route string, handlers ...HandlerFunc) {
	c := NewContext(nil)
	c.route = route
	c.chain.handlers = handlers
	c.chain.index = -1
	c.Next()
}

func TestMetrics_RecordsOKStatus(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.ok", "ok")

	runChain("chat.route.ok",
		Metrics("user-service"),
		func(c *Context) { c.SetStatus("ok") },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.ok", "ok")
	assert.Equal(t, before+1, after)
}

func TestMetrics_RecordsErrorStatus(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.err", "not_found")

	runChain("chat.route.err",
		Metrics("user-service"),
		func(c *Context) { c.SetStatus(rpcmetrics.StatusLabel(errcode.NotFound("nope"))) },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.err", "not_found")
	assert.Equal(t, before+1, after)
}

func TestMetrics_UnsetStatusDefaultsOK(t *testing.T) {
	before := rpcmetrics.CounterValue("user-service", "chat.route.default", "ok")

	runChain("chat.route.default",
		Metrics("user-service"),
		func(c *Context) { /* never sets status */ },
	)

	after := rpcmetrics.CounterValue("user-service", "chat.route.default", "ok")
	assert.Equal(t, before+1, after)
}

package natsrouter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// The tests below drive REAL Register/RegisterNoBody/RegisterVoid handlers
// (not synthetic c.SetStatus calls) over an in-process NATS server, with
// Metrics installed as router middleware. This is the end-to-end coverage
// for the status-stamping wired into pkg/natsrouter/register.go: a
// regression that swaps or drops a SetStatus call there would compile and
// pass runChain-based tests above but must fail here.
//
// Note: Metrics.Observe runs after c.Next() returns, but the Register*
// wrappers send the NATS reply *inside* c.Next() (before control returns to
// Metrics). So nc.Request can complete slightly before the counter is
// incremented — assert via require.Eventually, never immediately after
// nc.Request returns.

func TestMetrics_EndToEnd_RegisterSuccess(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "svc-metrics-e2e-success")
	r.Use(Metrics("svc-metrics-e2e-success"))

	const pattern = "metrics.e2e.success.{id}"
	Register(r, pattern,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "hello " + req.Name}, nil
		})

	before := rpcmetrics.CounterValue("svc-metrics-e2e-success", pattern, "ok")

	data, _ := json.Marshal(testReq{Name: "world"})
	resp, err := nc.Request(context.Background(), "metrics.e2e.success.42", data, 2*time.Second)
	require.NoError(t, err)

	var result testResp
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "hello world", result.Greeting)

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("svc-metrics-e2e-success", pattern, "ok") == before+1
	}, time.Second, 5*time.Millisecond, "rpc_server_requests_total status=ok must increment after the real Register success path")
}

func TestMetrics_EndToEnd_RegisterErrcodeError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "svc-metrics-e2e-notfound")
	r.Use(Metrics("svc-metrics-e2e-notfound"))

	const pattern = "metrics.e2e.notfound.{id}"
	Register(r, pattern,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, errcode.NotFound("thing not found")
		})

	before := rpcmetrics.CounterValue("svc-metrics-e2e-notfound", pattern, "not_found")

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "metrics.e2e.notfound.42", data, 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "not_found", string(errResp.Code))

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("svc-metrics-e2e-notfound", pattern, "not_found") == before+1
	}, time.Second, 5*time.Millisecond, "rpc_server_requests_total status=not_found must increment after a real errcode.NotFound handler return")
}

func TestMetrics_EndToEnd_RegisterInvalidJSON(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "svc-metrics-e2e-badjson")
	r.Use(Metrics("svc-metrics-e2e-badjson"))

	const pattern = "metrics.e2e.badjson.{id}"
	Register(r, pattern,
		func(c *Context, req testReq) (*testResp, error) {
			t.Fatal("handler should not be called for invalid JSON")
			return nil, nil
		})

	before := rpcmetrics.CounterValue("svc-metrics-e2e-badjson", pattern, "bad_request")

	resp, err := nc.Request(context.Background(), "metrics.e2e.badjson.42", []byte("not json"), 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "invalid request payload", errResp.Message)

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("svc-metrics-e2e-badjson", pattern, "bad_request") == before+1
	}, time.Second, 5*time.Millisecond, "rpc_server_requests_total status=bad_request must increment on real unmarshal failure in Register")
}

func TestMetrics_EndToEnd_RegisterVoid(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "svc-metrics-e2e-void")
	r.Use(Metrics("svc-metrics-e2e-void"))

	const okPattern = "metrics.e2e.void.ok.{id}"
	const errPattern = "metrics.e2e.void.err.{id}"

	processed := make(chan struct{}, 1)
	RegisterVoid(r, okPattern,
		func(c *Context, req testReq) error {
			processed <- struct{}{}
			return nil
		})
	RegisterVoid(r, errPattern,
		func(c *Context, req testReq) error {
			return errcode.Forbidden("nope")
		})

	beforeOK := rpcmetrics.CounterValue("svc-metrics-e2e-void", okPattern, "ok")
	beforeErr := rpcmetrics.CounterValue("svc-metrics-e2e-void", errPattern, "forbidden")

	data, _ := json.Marshal(testReq{Name: "hello"})
	require.NoError(t, nc.Publish(context.Background(), "metrics.e2e.void.ok.1", data))
	require.NoError(t, nc.Publish(context.Background(), "metrics.e2e.void.err.1", data))

	select {
	case <-processed:
	case <-time.After(2 * time.Second):
		t.Fatal("void success handler not called within timeout")
	}

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("svc-metrics-e2e-void", okPattern, "ok") == beforeOK+1
	}, time.Second, 5*time.Millisecond, "rpc_server_requests_total status=ok must increment after a real RegisterVoid success")

	require.Eventually(t, func() bool {
		return rpcmetrics.CounterValue("svc-metrics-e2e-void", errPattern, "forbidden") == beforeErr+1
	}, time.Second, 5*time.Millisecond, "rpc_server_requests_total status=forbidden must increment after a real RegisterVoid error")
}

package natsrouter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/idgen"
	"github.com/hmchangw/chat/pkg/logctx"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsutil"
)

type testReq struct {
	Name string `json:"name"`
}

type testResp struct {
	Greeting string `json:"greeting"`
}

func startTestNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	opts := &natsserver.Options{Port: -1}
	ns, err := natsserver.NewServer(opts)
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

func TestRegister_Success(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "chat.user.{account}.request.room.{roomID}.site-1.msg.test", natsmetrics.MethodMoveChat,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "hello " + req.Name + " from " + c.Param("account")}, nil
		})

	data, _ := json.Marshal(testReq{Name: "world"})
	resp, err := nc.Request(context.Background(), "chat.user.alice.request.room.r1.site-1.msg.test", data, 2*time.Second)
	require.NoError(t, err)

	var result testResp
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "hello world from alice", result.Greeting)
}

func TestRouter_WithMetricsRecordsBoundedRequestResultsAndReplies(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	metrics := natsmetrics.NewFromProvider(mp).Publisher("site-a")
	r := New(nc, "room-service", WithMetrics(metrics))

	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.member.list", natsmetrics.MethodListMembers,
		func(_ *Context, req testReq) (*testResp, error) {
			if req.Name == "deny" {
				return nil, errcode.Forbidden("not allowed")
			}
			return &testResp{Greeting: "ok"}, nil
		})

	for _, body := range [][]byte{[]byte(`{"name":"ok"}`), []byte(`{"name":"deny"}`), []byte(`not json`)} {
		_, err := nc.Request(context.Background(), "chat.user.alice.request.room.room-a.site-a.member.list", body, 2*time.Second)
		require.NoError(t, err)
	}

	// A handled request is one rpc.server.call.duration observation. Per
	// semconv the successful call carries no error.type at all, so it is keyed
	// here by its absence rather than by a "success" label value.
	wantResults := map[string]uint64{"": 1, "forbidden": 1, "bad_request": 1}
	var (
		results       map[string]uint64
		replyAttempts int64
	)
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		results = map[string]uint64{}
		replyAttempts = 0
		for _, scope := range rm.ScopeMetrics {
			for _, metric := range scope.Metrics {
				switch data := metric.Data.(type) {
				case metricdata.Histogram[float64]:
					if metric.Name != "rpc.server.call.duration" {
						continue
					}
					for _, point := range data.DataPoints {
						attrs := attrsOfPoint(point.Attributes)
						if attrs["rpc.method"] != "list_members" || attrs["rpc.system.name"] != "nats" {
							return false
						}
						results[attrs["error.type"]] += point.Count
					}
				case metricdata.Sum[int64]:
					if metric.Name != "chat.nats.publish.failures" {
						continue
					}
					for _, point := range data.DataPoints {
						attrs := attrsOfPoint(point.Attributes)
						if attrs["destination_kind"] == "client_response" && attrs["operation"] == "client_response" {
							replyAttempts += point.Value
						}
					}
				}
			}
		}
		return assert.ObjectsAreEqual(wantResults, results)
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, wantResults, results)
	// Every reply in this test is delivered, and a successful client response
	// is no longer counted — the family exists only to attribute failures.
	assert.Zero(t, replyAttempts, "successful replies must not be recorded")
}

func TestRegister_ParamsExtraction(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	var captured Params
	Register(r, "chat.user.{account}.request.room.{roomID}.{siteID}.msg.test", natsmetrics.MethodOpenRoom,
		func(c *Context, req testReq) (*testResp, error) {
			captured = c.Params
			return &testResp{}, nil
		})

	data, _ := json.Marshal(testReq{})
	_, err := nc.Request(context.Background(), "chat.user.alice.request.room.room-42.site-prod.msg.test", data, 2*time.Second)
	require.NoError(t, err)

	assert.Equal(t, "alice", captured.Get("account"))
	assert.Equal(t, "room-42", captured.Get("roomID"))
	assert.Equal(t, "site-prod", captured.Get("siteID"))
}

func TestRegister_InvalidJSON(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodGetRoomKey,
		func(c *Context, req testReq) (*testResp, error) {
			t.Fatal("handler should not be called for invalid JSON")
			return nil, nil
		})

	resp, err := nc.Request(context.Background(), "test.123", []byte("not json"), 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "invalid request payload", errResp.Message)
}

func TestRegister_HandlerError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodGetRoomAppTabs,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, fmt.Errorf("something broke")
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "internal error", errResp.Message)
}

func TestRegisterNoBody_Success(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterNoBody(r, "chat.user.{account}.request.rooms.get.{roomID}", natsmetrics.MethodBatchGetRoomPreviews,
		func(c *Context) (*testResp, error) {
			return &testResp{Greeting: "room " + c.Param("roomID")}, nil
		})

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.rooms.get.room-42", nil, 2*time.Second)
	require.NoError(t, err)

	var result testResp
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "room room-42", result.Greeting)
}

func TestMiddleware_ExecutionOrder(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	doneCh := make(chan []string, 1)

	var order []string
	r.Use(func(c *Context) {
		c.Next()
		doneCh <- order
	})

	makeMiddleware := func(name string) HandlerFunc {
		return func(c *Context) {
			order = append(order, name+":before")
			c.Next()
			order = append(order, name+":after")
		}
	}

	r.Use(makeMiddleware("A"))
	r.Use(makeMiddleware("B"))
	r.Use(makeMiddleware("C"))

	Register(r, "test.{id}", natsmetrics.MethodListOrgMembers,
		func(c *Context, req testReq) (*testResp, error) {
			order = append(order, "handler")
			return &testResp{}, nil
		})

	data, _ := json.Marshal(testReq{})
	_, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	result := <-doneCh
	assert.Equal(t, []string{
		"A:before", "B:before", "C:before",
		"handler",
		"C:after", "B:after", "A:after",
	}, result)
}

func TestMiddleware_ShortCircuit(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	r.Use(func(c *Context) {
		c.Abort()
		c.Msg.Respond([]byte(`{"rejected":true}`))
	})

	handlerCalled := false
	Register(r, "test.{id}", natsmetrics.MethodListMemberStatuses,
		func(c *Context, req testReq) (*testResp, error) {
			handlerCalled = true
			return &testResp{}, nil
		})

	data, _ := json.Marshal(testReq{})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	assert.False(t, handlerCalled)
	assert.Contains(t, string(resp.Data), "rejected")
}

func TestRecovery_CatchesPanic(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")
	r.Use(Recovery())

	Register(r, "test.{id}", natsmetrics.MethodListMentionableSubscriptions,
		func(c *Context, req testReq) (*testResp, error) {
			panic("boom!")
		})

	data, _ := json.Marshal(testReq{})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "internal error", errResp.Message)
}

func TestRegister_NoParams(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "static.subject", natsmetrics.MethodMarkRoomRead,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "hello " + req.Name}, nil
		})

	data, _ := json.Marshal(testReq{Name: "world"})
	resp, err := nc.Request(context.Background(), "static.subject", data, 2*time.Second)
	require.NoError(t, err)

	var result testResp
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "hello world", result.Greeting)
}

func TestRegister_RouteError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodListMessageReaders,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, errcode.NotFound("thing not found")
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var result errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "thing not found", result.Message)
	assert.Equal(t, "not_found", string(result.Code))
}

func TestRegister_RouteErrorSimple(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodMarkThreadRead,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, errcode.BadRequest(fmt.Sprintf("user %s not allowed", "alice"))
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var result errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "user alice not allowed", result.Message)
	assert.Equal(t, "bad_request", string(result.Code))
}

func TestRegister_InternalErrorNotExposed(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodUpdateMemberRole,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, fmt.Errorf("database connection refused")
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "internal error", errResp.Message)
	assert.NotContains(t, string(resp.Data), "database")
}

func TestRegisterVoid_Success(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	processed := make(chan string, 1)
	RegisterVoid(r, "events.{type}",
		func(c *Context, req testReq) error {
			processed <- c.Param("type") + ":" + req.Name
			return nil
		})

	data, _ := json.Marshal(testReq{Name: "hello"})
	err := nc.Publish(context.Background(), "events.typing", data)
	require.NoError(t, err)

	select {
	case result := <-processed:
		assert.Equal(t, "typing:hello", result)
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called within timeout")
	}
}

func TestRegisterVoid_NoReply(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterVoid(r, "events.{type}",
		func(c *Context, req testReq) error {
			return nil
		})

	data, _ := json.Marshal(testReq{Name: "hello"})
	_, err := nc.Request(context.Background(), "events.typing", data, 200*time.Millisecond)
	require.Error(t, err)
}

func TestErrcodeError_Error(t *testing.T) {
	e := errcode.NotFound("room not found")
	assert.Equal(t, "room not found", e.Error())

	e2 := errcode.BadRequest("simple error")
	assert.Equal(t, "simple error", e2.Error())
}

func TestErrcodeError_WrappedInFmtErrorf(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodRemoveMember,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, fmt.Errorf("context: %w", errcode.Forbidden("not allowed"))
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var result errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "not allowed", result.Message)
	assert.Equal(t, "forbidden", string(result.Code))
}

func TestContext_SetGet(t *testing.T) {
	c := NewContext(map[string]string{"id": "123"})
	c.Set("user", "alice")

	val, ok := c.Get("user")
	assert.True(t, ok)
	assert.Equal(t, "alice", val)

	assert.Equal(t, "alice", c.MustGet("user"))
	assert.Equal(t, "123", c.Param("id"))

	_, ok = c.Get("nonexistent")
	assert.False(t, ok)
}

func TestContext_MustGet_Panics(t *testing.T) {
	c := NewContext(nil)
	require.Panics(t, func() { c.MustGet("nope") })
}

func TestContext_Abort(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	handlerCalled := false
	r.Use(func(c *Context) {
		c.Abort()
		// Don't call Next
	})

	Register(r, "test.abort", natsmetrics.MethodAddMembers,
		func(c *Context, req testReq) (*testResp, error) {
			handlerCalled = true
			return &testResp{}, nil
		})

	data, _ := json.Marshal(testReq{})
	_, err := nc.Request(context.Background(), "test.abort", data, 200*time.Millisecond)
	require.Error(t, err)
	assert.False(t, handlerCalled)
}

func TestRequestID_Generated(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")
	r.Use(RequestID())

	var capturedID string
	Register(r, "test.{id}", natsmetrics.MethodRenameRoom,
		func(c *Context, req testReq) (*testResp, error) {
			val, ok := c.Get("requestID")
			require.True(t, ok)
			capturedID = val.(string)
			return &testResp{}, nil
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	_, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, capturedID)
}

func TestRequestID_FromHeader(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")
	r.Use(RequestID())

	var capturedID string
	Register(r, "test.{id}", natsmetrics.MethodSetRoomRestricted,
		func(c *Context, req testReq) (*testResp, error) {
			capturedID = c.MustGet("requestID").(string)
			return &testResp{}, nil
		})

	msg := nats.NewMsg("test.123")
	msg.Data, _ = json.Marshal(testReq{Name: "test"})
	msg.Header = nats.Header{}
	testID := "01893f8b-1c4a-7000-abcd-ef0123456789"
	msg.Header.Set(natsutil.RequestIDHeader, testID)

	resp, err := nc.NatsConn().RequestMsg(msg, 2*time.Second)
	require.NoError(t, err)
	assert.NotEmpty(t, string(resp.Data))
	assert.Equal(t, testID, capturedID)
}

func TestRegisterNoBody_HandlerError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterNoBody(r, "test.{id}", natsmetrics.MethodBatchGetRoomsInfo,
		func(c *Context) (*testResp, error) {
			return nil, fmt.Errorf("something failed")
		})

	resp, err := nc.Request(context.Background(), "test.123", nil, 2*time.Second)
	require.NoError(t, err)

	var errResp errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &errResp))
	assert.Equal(t, "internal error", errResp.Message)
}

func TestRegisterNoBody_RouteError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterNoBody(r, "test.{id}", natsmetrics.MethodBatchGetThreadRoomsInfo,
		func(c *Context) (*testResp, error) {
			return nil, errcode.NotFound("item not found")
		})

	resp, err := nc.Request(context.Background(), "test.123", nil, 2*time.Second)
	require.NoError(t, err)

	var result errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "item not found", result.Message)
	assert.Equal(t, "not_found", string(result.Code))
}

func TestLogging_LogsRequest(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")
	r.Use(Logging())

	Register(r, "test.{id}", natsmetrics.MethodMarkAllThreadsRead,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "ok"}, nil
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var result testResp
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "ok", result.Greeting)
}

func TestRegister_TypedInternalError(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	Register(r, "test.{id}", natsmetrics.MethodEnsureRoomKey,
		func(c *Context, req testReq) (*testResp, error) {
			return nil, errcode.Internal("failed to load data")
		})

	data, _ := json.Marshal(testReq{Name: "test"})
	resp, err := nc.Request(context.Background(), "test.123", data, 2*time.Second)
	require.NoError(t, err)

	var result errcode.Error
	require.NoError(t, json.Unmarshal(resp.Data, &result))
	assert.Equal(t, "failed to load data", result.Message)
	assert.Equal(t, "internal", string(result.Code))
}

func TestContext_SetContext_Propagates(t *testing.T) {
	c := NewContext(nil)
	type k int
	const myKey k = 0
	newCtx := context.WithValue(c, myKey, "value-from-set")
	c.SetContext(newCtx)
	assert.Equal(t, "value-from-set", c.Value(myKey))
}

func TestRequestIDMiddleware_StoresIDOnUnderlyingContext(t *testing.T) {
	c := NewContext(nil)
	c.Msg = &nats.Msg{Header: nats.Header{}}
	testID := "01893f8b-1c4a-7000-abcd-ef0123456789"
	c.Msg.Header.Set(natsutil.RequestIDHeader, testID)

	called := false
	chain := []HandlerFunc{
		RequestID(),
		func(c *Context) {
			called = true
			fromKeys, _ := c.Get("requestID")
			fromCtx := natsutil.RequestIDFromContext(c)
			assert.Equal(t, testID, fromKeys)
			assert.Equal(t, testID, fromCtx)
		},
	}
	runChain(c, chain)
	assert.True(t, called, "downstream handler must run")
}

func TestRequestIDMiddleware_GeneratesAndStoresOnContext_WhenHeaderMissing(t *testing.T) {
	c := NewContext(nil)
	c.Msg = &nats.Msg{Header: nats.Header{}}

	var fromCtx string
	var fromKeys string
	chain := []HandlerFunc{
		RequestID(),
		func(c *Context) {
			fromCtx = natsutil.RequestIDFromContext(c)
			fromKeysAny, _ := c.Get("requestID")
			fromKeys = fromKeysAny.(string)
		},
	}
	runChain(c, chain)
	assert.NotEmpty(t, fromCtx, "RequestID middleware must mint and propagate to ctx when header is absent")
	assert.Equal(t, fromCtx, fromKeys, "minted ID must be identical in ctx and keys map")
}

func TestRequestIDMiddleware_RegeneratesOnMalformedHeader(t *testing.T) {
	c := NewContext(nil)
	c.Msg = &nats.Msg{Header: nats.Header{}}
	c.Msg.Header.Set(natsutil.RequestIDHeader, "not-a-uuidv7")

	var fromCtx string
	chain := []HandlerFunc{
		RequestID(),
		func(c *Context) {
			fromCtx = natsutil.RequestIDFromContext(c)
		},
	}
	runChain(c, chain)

	assert.NotEqual(t, "not-a-uuidv7", fromCtx, "malformed inbound ID must be replaced")
	assert.True(t, idgen.IsValidUUID(fromCtx), "regenerated ID must be a valid hyphenated UUID")
}

func TestRequestIDMiddleware_OtherCtxKeysStillReadable(t *testing.T) {
	// Regression: an earlier version passed c as parent to WithRequestID, creating a circular ctx.parent loop.
	// Fixed by passing c.ctx instead — other-key lookups must complete in finite time.
	type otherKey int
	const k otherKey = 0

	parentCtx := context.WithValue(context.Background(), k, "parent-value")
	c := NewContext(nil)
	c.SetContext(parentCtx)
	c.Msg = &nats.Msg{Header: nats.Header{}}

	called := false
	chain := []HandlerFunc{
		RequestID(),
		func(c *Context) {
			called = true
			// This lookup must complete in finite time — would infinite-loop with the bug.
			got := c.Value(k)
			assert.Equal(t, "parent-value", got, "non-requestIDKey lookup must still find values from the original parent ctx")
		},
	}
	runChain(c, chain)
	assert.True(t, called, "downstream handler must run")
}

func TestRouter_DefaultIsUnbounded(t *testing.T) {
	r := New(nil, "test")
	assert.Nil(t, r.sem, "default router has no admission semaphore (unbounded spawn)")
}

func TestRouter_WithMaxConcurrency_Overrides(t *testing.T) {
	r := New(nil, "test", WithMaxConcurrency(7))
	assert.Equal(t, 7, cap(r.sem))
}

func TestRouter_WithMaxConcurrency_IgnoresNonPositive(t *testing.T) {
	r := New(nil, "test", WithMaxConcurrency(0))
	assert.Nil(t, r.sem, "WithMaxConcurrency(0) leaves the router unbounded")
	r2 := New(nil, "test", WithMaxConcurrency(-1))
	assert.Nil(t, r2.sem, "WithMaxConcurrency(-1) leaves the router unbounded")
}

// TestRouter_replyBusy_NoReplySubject verifies fire-and-forget messages (empty Reply) trigger silent-drop without panic.
func TestRouter_replyBusy_NoReplySubject(t *testing.T) {
	r := New(nil, "test")
	msg := &nats.Msg{Subject: "void.subject", Reply: ""}
	r.replyBusy(context.Background(), msg)
}

func TestDefault_PreInstallsMiddleware(t *testing.T) {
	r := Default(nil, "test")
	assert.Len(t, r.middleware, 3, "Default should pre-install Recovery, RequestID, Logging")
}

func TestDefault_ForwardsOptions(t *testing.T) {
	r := Default(nil, "test", WithMaxConcurrency(7))
	require.NotNil(t, r.sem, "WithMaxConcurrency through Default must set the semaphore")
	assert.Equal(t, 7, cap(r.sem))
}

// TestRouter_admit_Unbounded verifies unbounded router always admits and returns a safe no-op release.
func TestRouter_admit_Unbounded(t *testing.T) {
	r := New(nil, "test")
	require.Nil(t, r.sem)

	admitted, release := r.admit()
	require.True(t, admitted, "unbounded router must always admit")
	require.NotNil(t, release, "release must be non-nil on admitted path")
	require.NotPanics(t, release, "no-op release must not panic")
	require.NotPanics(t, release, "release must be safe to call repeatedly when no-op")
}

// TestRouter_admit_BoundedAvailable verifies bounded acquire: slot taken on success, returned on release.
func TestRouter_admit_BoundedAvailable(t *testing.T) {
	r := New(nil, "test", WithMaxConcurrency(1))
	require.NotNil(t, r.sem)
	require.Equal(t, 0, len(r.sem), "fresh sem must be empty")

	admitted, release := r.admit()
	require.True(t, admitted)
	require.NotNil(t, release)
	require.Equal(t, 1, len(r.sem), "successful admit must occupy one slot")

	release()
	require.Equal(t, 0, len(r.sem), "release must free the slot")

	// Subsequent admit must succeed since the slot was freed.
	admitted2, release2 := r.admit()
	require.True(t, admitted2)
	release2()
}

// TestRouter_admit_BoundedSaturated verifies full semaphore returns (false, nil); nil release is intentional footgun.
func TestRouter_admit_BoundedSaturated(t *testing.T) {
	r := New(nil, "test", WithMaxConcurrency(1))

	admitted1, release1 := r.admit()
	require.True(t, admitted1)
	defer release1()

	admitted2, release2 := r.admit()
	require.False(t, admitted2)
	require.Nil(t, release2, "rejected admit must return nil release (footgun protection)")
}

// Payload capture: when DEBUG_LOG_PAYLOADS is on and the request carries
// X-Debug-Payload, Register logs the full request and reply payloads; when the
// flag is off, nothing is logged even with the header (prod invariant).
func TestRegister_PayloadCapture(t *testing.T) {
	const validUUID = "01970a4f-8c2d-7c9a-abcd-e0123456789f"
	run := func(t *testing.T, payloadsEnabled bool) *flowRecorder {
		rec := &flowRecorder{}
		prev := slog.Default()
		slog.SetDefault(slog.New(rec))
		logctx.Configure(logctx.Config{Rate: 1e6, Burst: 1 << 20, Payloads: payloadsEnabled})
		t.Cleanup(func() { slog.SetDefault(prev); logctx.Configure(logctx.Config{Rate: 0, Burst: 0}) })

		nc := startTestNATS(t)
		r := New(nc, "test-service")
		r.Use(RequestID())
		Register(r, "test.{id}", natsmetrics.MethodCreateRoom, func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "hi " + req.Name}, nil
		})

		reqData, _ := json.Marshal(testReq{Name: "alice"})
		msg := &nats.Msg{Subject: "test.123", Data: reqData, Header: nats.Header{
			natsutil.RequestIDHeader:    []string{validUUID},
			natsutil.DebugPayloadHeader: []string{"1"},
		}}
		reply, err := nc.NatsConn().RequestMsg(msg, 2*time.Second)
		require.NoError(t, err)
		_ = reply
		return rec
	}

	t.Run("enabled: request + reply captured", func(t *testing.T) {
		rec := run(t, true)
		got := rec.payloads()
		assert.Contains(t, got, `{"name":"alice"}`, "request payload captured")
		assert.Contains(t, got, `{"greeting":"hi alice"}`, "reply payload captured")
	})

	t.Run("disabled: nothing captured even with the header", func(t *testing.T) {
		rec := run(t, false)
		assert.Empty(t, rec.payloads(), "prod invariant: no body when DEBUG_LOG_PAYLOADS is off")
	})
}

func TestRegisterOptionalBody_EmptyPayloadYieldsZeroValue(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt.test", natsmetrics.MethodStartTeamsRoomCall,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "name=" + req.Name}, nil
		})

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt.test", nil, 2*time.Second)
	require.NoError(t, err)
	var out testResp
	require.NoError(t, json.Unmarshal(resp.Data, &out))
	assert.Equal(t, "name=", out.Greeting)
}

func TestRegisterOptionalBody_NonEmptyPayloadUnmarshals(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt2.test", natsmetrics.MethodStartTeamsUserCall,
		func(c *Context, req testReq) (*testResp, error) {
			return &testResp{Greeting: "name=" + req.Name}, nil
		})

	data, _ := json.Marshal(testReq{Name: "bob"})
	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt2.test", data, 2*time.Second)
	require.NoError(t, err)
	var out testResp
	require.NoError(t, json.Unmarshal(resp.Data, &out))
	assert.Equal(t, "name=bob", out.Greeting)
}

func TestRegisterOptionalBody_MalformedPayloadIsBadRequest(t *testing.T) {
	nc := startTestNATS(t)
	r := New(nc, "test-service")

	RegisterOptionalBody(r, "chat.user.{account}.request.user.s1.opt3.test", natsmetrics.MethodCreateTeamsMeeting,
		func(c *Context, req testReq) (*testResp, error) {
			t.Fatal("handler must not run on malformed payload")
			return nil, nil
		})

	resp, err := nc.Request(context.Background(), "chat.user.alice.request.user.s1.opt3.test", []byte("{not json"), 2*time.Second)
	require.NoError(t, err)
	var envelope struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(resp.Data, &envelope))
	assert.Equal(t, "bad_request", envelope.Code)
}

func attrsOfPoint(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.String()
	}
	return out
}

// serverCallMethods returns the rpc.method of every rpc.server.call.duration
// point recorded so far. Collect is a snapshot, so callers that expect zero
// samples must first wait on something the handler itself sets.
func serverCallMethods(t *testing.T, reader *sdkmetric.ManualReader) []string {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var methods []string
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || m.Name != "rpc.server.call.duration" {
				continue
			}
			for _, point := range hist.DataPoints {
				methods = append(methods, attrsOfPoint(point.Attributes)["rpc.method"])
			}
		}
	}
	return methods
}

// A reply-registering route records the method its registration named — not one
// derived from the subject, which is what let user-service's
// chatlist.section.create record as room-service's room_mutation.
func TestRouter_RecordsRegisteredMethod(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "room-service", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.room.rename",
		natsmetrics.MethodRenameRoom,
		func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil })

	_, err := nc.Request(context.Background(),
		"chat.user.alice.request.room.room-a.site-a.room.rename", []byte(`{"name":"ok"}`), 2*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"rename_room"}, serverCallMethods(t, reader))
	}, time.Second, 10*time.Millisecond)
}

// A RegisterVoid route has no reply subject, so it is not an RPC call. It must
// produce no rpc.server.call.duration sample at all — its local handler time is
// not a round trip, and the presence heartbeat lane that uses this is the
// fleet's highest-volume traffic, so recording it inflates the histogram's count
// and pulls every percentile down.
//
// The handler signals through done, because there is no reply to wait on and
// asserting "still empty" against an unsynchronised handler would pass before
// the handler had run at all. done alone is not enough, though: recordHandled
// runs from a defer in the dispatch goroutine, so it is ordered *after* the
// handler returns and racing it would let this test pass against an
// implementation that had lost the recordRPC guard. Shutdown waits on r.wg, and
// `defer r.wg.Done()` is registered first in that goroutine so it unwinds last
// — strictly after the record site. That is the edge the assertion needs.
func TestRouter_RegisterVoidRecordsNoRPCSample(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "user-presence-service", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))
	done := make(chan struct{})
	RegisterVoid(r, "chat.user.{account}.event.presence.site-a.ping",
		func(_ *Context, req testReq) error { close(done); return nil })

	require.NoError(t, nc.PublishMsg(context.Background(), &nats.Msg{
		Subject: "chat.user.alice.event.presence.site-a.ping",
		Data:    []byte(`{"name":"ok"}`),
	}))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("void handler never ran")
	}
	require.NoError(t, r.Shutdown(context.Background()))
	assert.Empty(t, serverCallMethods(t, reader))
}

// An undeclared method must not kill the process. Registration is gated
// earlier — by .semgrep/rpcmethod.yml and by each service's registration test
// — so a value arriving here means both were bypassed. Degrade to _OTHER,
// which is alertable, and keep serving: a chat service is worth more than a
// clean dashboard.
func TestRegisterDegradesUndeclaredMethodToOther(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "test", WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")))

	require.NotPanics(t, func() {
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
			natsmetrics.RPCMethod("not_registered"),
			func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil })
	})

	_, err := nc.Request(context.Background(),
		"chat.user.alice.request.room.room-a.site-a.open", []byte(`{"name":"ok"}`), 2*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return assert.ObjectsAreEqual([]string{"_OTHER"}, serverCallMethods(t, reader))
	}, time.Second, 10*time.Millisecond)
}

// A duplicate method merges two routes into one series — a blunt signal, but
// refusing the registration would drop the API entirely, which is an outage.
// Register both and let the per-service golden test be the gate.
func TestRegisterKeepsBothRoutesOnDuplicateMethod(t *testing.T) {
	r := New(startTestNATS(t), "room-service")
	handler := func(_ *Context, req testReq) (*testResp, error) { return &testResp{Greeting: "ok"}, nil }

	require.NotPanics(t, func() {
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
			natsmetrics.MethodOpenRoom, handler)
		Register(r, "chat.user.{account}.request.room.{roomID}.site-a.app.tabs",
			natsmetrics.MethodOpenRoom, handler)
	})

	// Both subscriptions live, and both are visible. Routes is keyed by
	// pattern, so the duplicate does not overwrite its incumbent — the golden
	// file grows a second line carrying an already-used method, which is the
	// diff a reviewer sees. Keyed by method the later registration replaced
	// the earlier one and, when the duplicate registered first, the golden
	// came out byte-identical to the committed one.
	assert.Equal(t, []natsmetrics.RPCRoute{
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.open"},
		{Method: natsmetrics.MethodOpenRoom, Pattern: "chat.user.{account}.request.room.{roomID}.site-a.app.tabs"},
	}, r.Routes())
}

// Routes is what each service's registration test compares to its golden file,
// so it must be a copy — a caller holding the router's own map could mutate
// the dispatch table.
func TestRoutesReturnsACopy(t *testing.T) {
	r := New(startTestNATS(t), "test")
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(_ *Context, req testReq) (*testResp, error) { return &testResp{}, nil })

	got := r.Routes()
	require.Len(t, got, 1)
	got[0].Method = "mutated"
	assert.Equal(t, natsmetrics.MethodOpenRoom, r.Routes()[0].Method,
		"Routes must not hand out the router's own slice")
}

// Admission rejection replies before the handler runs, so it has no result to
// classify — but it is still that route's traffic and must carry its method, or
// a saturation incident lands in a different series from the route that
// saturated.
func TestRouter_AdmissionRejectionKeepsRouteMethod(t *testing.T) {
	nc := startTestNATS(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	r := New(nc, "room-service",
		WithMetrics(natsmetrics.NewFromProvider(mp).Publisher("site-a")), WithMaxConcurrency(1))
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	Register(r, "chat.user.{account}.request.room.{roomID}.site-a.open",
		natsmetrics.MethodOpenRoom,
		func(_ *Context, req testReq) (*testResp, error) { <-block; return &testResp{}, nil })

	// The first request occupies the only slot; the second is shed.
	go func() {
		_, _ = nc.Request(context.Background(),
			"chat.user.alice.request.room.room-a.site-a.open", []byte(`{"name":"ok"}`), 5*time.Second)
	}()
	require.Eventually(t, func() bool {
		_, err := nc.Request(context.Background(),
			"chat.user.bob.request.room.room-b.site-a.open", []byte(`{"name":"ok"}`), time.Second)
		return err == nil && len(serverCallMethods(t, reader)) > 0
	}, 3*time.Second, 20*time.Millisecond)

	assert.Contains(t, serverCallMethods(t, reader), "open_room")
}

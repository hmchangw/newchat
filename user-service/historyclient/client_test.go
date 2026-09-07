package historyclient

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// startTestNATS spins up an embedded, in-process NATS server (no Docker) for
// request/reply unit tests.
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

func TestRoomsGet_Hints(t *testing.T) {
	t.Run("non-empty hints are marshaled into the request body", func(t *testing.T) {
		nc := startTestNATS(t)

		lastMsgAt := int64(1234)
		var gotReq model.RoomsGetRequest
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			require.NoError(t, json.Unmarshal(m.Data, &gotReq))
			out, _ := json.Marshal(model.RoomsGetResponse{})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		hints := map[string]model.RoomTimeHint{"r1": {LastMsgAt: &lastMsgAt}}
		_, err = New(nc, natsmetrics.Publisher{}).RoomsGet(context.Background(), "site-a", []string{"r1"}, hints)
		require.NoError(t, err)

		require.Contains(t, gotReq.Hints, "r1")
		require.NotNil(t, gotReq.Hints["r1"].LastMsgAt)
		assert.Equal(t, lastMsgAt, *gotReq.Hints["r1"].LastMsgAt)
	})

	t.Run("nil hints are omitted from the request body", func(t *testing.T) {
		nc := startTestNATS(t)

		var gotRaw map[string]any
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			require.NoError(t, json.Unmarshal(m.Data, &gotRaw))
			out, _ := json.Marshal(model.RoomsGetResponse{})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, natsmetrics.Publisher{}).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.NoError(t, err)

		_, present := gotRaw["hints"]
		assert.False(t, present, "nil hints must be omitted from the wire request")
	})

	t.Run("happy path — returns preview messages from responder", func(t *testing.T) {
		nc := startTestNATS(t)
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			out, _ := json.Marshal(model.RoomsGetResponse{
				Rooms: map[string]model.PreviewMessage{"r1": {MessageID: "m1"}},
			})
			_ = m.Respond(out)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		out, err := New(nc, natsmetrics.Publisher{}).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.NoError(t, err)
		require.Contains(t, out, "r1")
		assert.Equal(t, "m1", out["r1"].MessageID)
	})

	t.Run("errcode reply — returns typed errcode error", func(t *testing.T) {
		nc := startTestNATS(t)
		sub, err := nc.Subscribe(context.Background(), subject.RoomsGet("site-a"), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.NotFound("room not found"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = sub.Unsubscribe() })

		_, err = New(nc, natsmetrics.Publisher{}).RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
		require.Error(t, err)
		var e *errcode.Error
		require.True(t, errors.As(err, &e))
		assert.Equal(t, errcode.CodeNotFound, e.Code)
	})

	t.Run("no responder — returns error wrapping rooms-get rpc", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := New(nc, natsmetrics.Publisher{}).RoomsGet(ctx, "site-a", []string{"r1"}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rooms-get rpc")
	})
}

// clientMetricFor returns a Publisher writing into a manual reader, plus a
// lookup over rpc.client.call.duration keyed by rpc.method and error.type.
//
// The pair matters together: asserting only on the requested error class would
// still pass if one call were recorded twice under different labels, and
// asserting only on the method would not catch a remote failure recorded as a
// success — the defect that made the client histogram disagree with the
// callee's server histogram about whether a call failed.
func clientMetricFor(t *testing.T) (natsmetrics.Publisher, func(method natsmetrics.RPCMethod, errorType string) (count, methodTotal uint64)) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	pub := natsmetrics.NewFromProvider(mp).Publisher("site-a")

	return pub, func(method natsmetrics.RPCMethod, errorType string) (uint64, uint64) {
		t.Helper()
		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))

		var count, methodTotal uint64
		for _, scope := range rm.ScopeMetrics {
			for _, m := range scope.Metrics {
				if m.Name != "rpc.client.call.duration" {
					continue
				}
				histogram, ok := m.Data.(metricdata.Histogram[float64])
				require.True(t, ok, "rpc.client.call.duration must be a histogram")
				for _, dp := range histogram.DataPoints {
					attrs := map[string]string{}
					for _, kv := range dp.Attributes.ToSlice() {
						attrs[string(kv.Key)] = kv.Value.AsString()
					}
					if attrs["rpc.method"] != string(method) {
						continue
					}
					methodTotal += dp.Count
					// A successful call carries no error.type at all, per the
					// convention's conditional attribute.
					if attrs["error.type"] == errorType {
						count += dp.Count
					}
				}
			}
		}
		return count, methodTotal
	}
}

// Every outbound call records rpc.client.call.duration from the call's final
// outcome, not from what nc.Request returned. A remote errcode envelope and a
// decode failure both arrive as a successful transport read, so recording at
// the call site labelled two of the three failure modes success — and the
// client histogram then disagreed with the callee's server histogram about
// whether the call failed.
//
// Each case asserts the method as well as the class, because asserting only the
// class would pass if one call were recorded twice under different labels.
func TestHistoryClient_RecordsClientCall(t *testing.T) {
	tests := []struct {
		name      string
		subject   string
		method    natsmetrics.RPCMethod
		call      func(c *Client) error
		okReply   string
		subscribe bool
		reply     func(m *nats.Msg)
		wantErr   bool
		wantClass string // "" means a successful call, which carries no error.type
	}{
		{
			name:    "RoomsGet success",
			subject: subject.RoomsGet("site-a"),
			method:  natsmetrics.MethodBatchGetRoomPreviews,
			call: func(c *Client) error {
				_, err := c.RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
				return err
			},
			subscribe: true,
			reply:     func(m *nats.Msg) { _ = m.Respond([]byte(`{"previews":{}}`)) },
		},
		{
			name:    "RoomsGet remote errcode envelope",
			subject: subject.RoomsGet("site-a"),
			method:  natsmetrics.MethodBatchGetRoomPreviews,
			call: func(c *Client) error {
				_, err := c.RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
				return err
			},
			subscribe: true,
			reply: func(m *nats.Msg) {
				body, _ := json.Marshal(errcode.NotFound("nope"))
				_ = m.Respond(body)
			},
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:    "RoomsGet undecodable reply",
			subject: subject.RoomsGet("site-a"),
			method:  natsmetrics.MethodBatchGetRoomPreviews,
			call: func(c *Client) error {
				_, err := c.RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
				return err
			},
			subscribe: true,
			reply:     func(m *nats.Msg) { _ = m.Respond([]byte("{not json")) },
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:    "RoomsGet no responder",
			subject: subject.RoomsGet("site-a"),
			method:  natsmetrics.MethodBatchGetRoomPreviews,
			call: func(c *Client) error {
				_, err := c.RoomsGet(context.Background(), "site-a", []string{"r1"}, nil)
				return err
			},
			wantErr:   true,
			wantClass: "no_responders",
		},
		{
			name:    "GetThreadList success",
			subject: subject.ThreadSubscriptionList("site-a"),
			method:  natsmetrics.MethodListThreadSubscriptions,
			call: func(c *Client) error {
				_, err := c.GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{})
				return err
			},
			subscribe: true,
			reply:     func(m *nats.Msg) { _ = m.Respond([]byte(`{}`)) },
		},
		{
			name:    "GetThreadList remote errcode envelope",
			subject: subject.ThreadSubscriptionList("site-a"),
			method:  natsmetrics.MethodListThreadSubscriptions,
			call: func(c *Client) error {
				_, err := c.GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{})
				return err
			},
			subscribe: true,
			reply: func(m *nats.Msg) {
				body, _ := json.Marshal(errcode.Forbidden("nope"))
				_ = m.Respond(body)
			},
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:    "GetThreadList no responder",
			subject: subject.ThreadSubscriptionList("site-a"),
			method:  natsmetrics.MethodListThreadSubscriptions,
			call: func(c *Client) error {
				_, err := c.GetThreadList(context.Background(), "site-a", model.ThreadSubscriptionListRequest{})
				return err
			},
			wantErr:   true,
			wantClass: "no_responders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := testutil.EmbeddedNATS(t)
			if tt.subscribe {
				sub, err := nc.Subscribe(context.Background(), tt.subject, func(_ context.Context, m *nats.Msg) {
					tt.reply(m)
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}

			pub, calls := clientMetricFor(t)
			err := tt.call(New(nc, pub))
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			count, methodTotal := calls(tt.method, tt.wantClass)
			assert.Equal(t, uint64(1), count, "one call recorded as %q", tt.wantClass)
			assert.Equal(t, uint64(1), methodTotal, "recorded exactly once, under one label set")
		})
	}
}

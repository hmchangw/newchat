package roomclient

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
	"github.com/hmchangw/chat/pkg/testutil"
)

// Every outbound call records rpc.client.call.duration from the call's final
// outcome, not from what nc.Request returned. A remote errcode envelope and a
// decode failure both arrive as a successful transport read, so recording at
// the call site labelled two of the three failure modes success — and the
// client histogram then disagreed with the callee's server histogram about
// whether the call failed.
//
// Every instrumented method is in the table, not just one. Each case asserts
// the method as well as the class: asserting only the class would pass if a
// method were bound to a valid-but-wrong constant, and asserting only once per
// package would leave the other three methods unpinned — the same gap the
// golden files close on the server side. methodTotal pins exactly-once, which
// is what catches a stray second recording beside a deferred one.
func TestRoomClient_RecordsClientCall(t *testing.T) {
	const site = "site-a"

	type call struct {
		subject string
		method  natsmetrics.RPCMethod
		okReply string
		invoke  func(c *Client) error
	}
	calls := []call{
		{
			subject: subject.RoomsInfoBatch(site),
			method:  natsmetrics.MethodBatchGetRoomsInfo,
			okReply: `{"rooms":[]}`,
			invoke: func(c *Client) error {
				_, err := c.GetRoomsInfo(context.Background(), site, []string{"r1"})
				return err
			},
		},
		{
			subject: subject.ThreadRoomInfoBatch(site),
			method:  natsmetrics.MethodBatchGetThreadRoomsInfo,
			okReply: `{"threads":[]}`,
			invoke: func(c *Client) error {
				_, err := c.GetThreadRoomInfoBatch(context.Background(), site, []string{"tr1"})
				return err
			},
		},
		{
			// The reply carries no payload, so there is no decode step and no
			// undecodable case below — success is a nil error.
			subject: subject.RoomThreadReadAll(site),
			method:  natsmetrics.MethodMarkAllThreadsRead,
			okReply: `{}`,
			invoke: func(c *Client) error {
				return c.ClearAllThreadUnread(context.Background(), site, "u1")
			},
		},
		{
			subject: subject.RoomCreateDMSync(site),
			method:  natsmetrics.MethodCreateDMRoom,
			okReply: `{"success":true}`,
			invoke: func(c *Client) error {
				_, err := c.CreateDMRoom(context.Background(), "u1", "u2", model.RoomTypeDM)
				return err
			},
		},
	}

	outcomes := []struct {
		name      string
		subscribe bool
		reply     func(okReply string) []byte
		wantErr   bool
		wantClass string // "" means a successful call, which carries no error.type
	}{
		{
			name:      "success",
			subscribe: true,
			reply:     func(ok string) []byte { return []byte(ok) },
		},
		{
			name:      "remote errcode envelope",
			subscribe: true,
			reply: func(string) []byte {
				body, _ := json.Marshal(errcode.NotFound("nope"))
				return body
			},
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "undecodable reply",
			subscribe: true,
			reply:     func(string) []byte { return []byte("{not json") },
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "no responder",
			wantErr:   true,
			wantClass: "no_responders",
		},
	}

	for _, c := range calls {
		for _, o := range outcomes {
			if o.name == "undecodable reply" && c.method == natsmetrics.MethodMarkAllThreadsRead {
				continue // no payload to decode
			}
			t.Run(string(c.method)+"/"+o.name, func(t *testing.T) {
				nc := testutil.EmbeddedNATS(t)
				if o.subscribe {
					sub, err := nc.Subscribe(context.Background(), c.subject, func(_ context.Context, m *nats.Msg) {
						_ = m.Respond(o.reply(c.okReply))
					})
					require.NoError(t, err)
					t.Cleanup(func() { _ = sub.Unsubscribe() })
				}

				pub, recorded := clientMetricFor(t)
				err := c.invoke(New(nc, site, pub))
				if o.wantErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				count, methodTotal := recorded(c.method, o.wantClass)
				assert.Equal(t, uint64(1), count, "one call recorded as %q", o.wantClass)
				assert.Equal(t, uint64(1), methodTotal, "recorded exactly once, under one label set")
			})
		}
	}
}

// clientMetricFor returns a Publisher writing into a manual reader, plus a
// lookup over rpc.client.call.duration keyed by rpc.method and error.type.
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

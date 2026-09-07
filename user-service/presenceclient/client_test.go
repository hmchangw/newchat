package presenceclient

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
// Each case asserts the method as well as the class, because asserting only the
// class would pass if one call were recorded twice under different labels.
func TestQueryPresence_RecordsClientCall(t *testing.T) {
	tests := []struct {
		name      string
		reply     func(m *nats.Msg)
		subscribe bool
		wantErr   bool
		wantClass string // "" means a successful call, which carries no error.type
	}{
		{
			name:      "success",
			subscribe: true,
			reply:     func(m *nats.Msg) { _ = m.Respond([]byte(`{"states":[]}`)) },
			wantClass: "",
		},
		{
			name:      "remote errcode envelope",
			subscribe: true,
			reply: func(m *nats.Msg) {
				body, _ := json.Marshal(errcode.NotFound("nope"))
				_ = m.Respond(body)
			},
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "undecodable reply",
			subscribe: true,
			reply:     func(m *nats.Msg) { _ = m.Respond([]byte("{{not json")) },
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "no responder",
			subscribe: false,
			wantErr:   true,
			wantClass: "no_responders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := testutil.EmbeddedNATS(t)
			if tt.subscribe {
				sub, err := nc.Subscribe(context.Background(), subject.PresenceQueryBatchPeer("site-a"), func(_ context.Context, m *nats.Msg) {
					tt.reply(m)
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}

			pub, calls := clientMetricFor(t)
			_, err := New(nc, pub).QueryPresence(context.Background(), "site-a", []string{"u1"})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			count, methodTotal := calls(natsmetrics.MethodBatchGetPeerPresence, tt.wantClass)
			assert.Equal(t, uint64(1), count, "one call recorded as %q", tt.wantClass)
			assert.Equal(t, uint64(1), methodTotal, "recorded exactly once, under one label set")
		})
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

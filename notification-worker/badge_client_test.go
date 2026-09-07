package main

import (
	"context"
	"encoding/json"
	"testing"

	nats "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/errcode"
	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestNatsBadgeClient_Counts(t *testing.T) {
	const (
		siteID = "site-a"
		roomID = "room-1"
	)
	accounts := []string{"alice", "bob"}

	t.Run("happy path — returns the response's counts map", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			var req model.BadgeCountBatchRequest
			require.NoError(t, json.Unmarshal(m.Data, &req))
			assert.Equal(t, roomID, req.RoomID)
			assert.Equal(t, accounts, req.Accounts)

			data, _ := json.Marshal(model.BadgeCountBatchResponse{Counts: map[string]int{"alice": 3, "bob": 7}})
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc, natsmetrics.Publisher{})
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"alice": 3, "bob": 7}, got)
	})

	t.Run("remote errcode error envelope — returns typed error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			data, _ := json.Marshal(errcode.Internal("badge service down"))
			_ = m.Respond(data)
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc, natsmetrics.Publisher{})
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
		var ee *errcode.Error
		require.ErrorAs(t, err, &ee)
		assert.Equal(t, errcode.CodeInternal, ee.Code)
	})

	t.Run("no responder — returns error", func(t *testing.T) {
		nc := startTestNATS(t)
		// Intentionally no subscriber: nc.Request must fail with "no responders".

		client := newNatsBadgeClient(nc, natsmetrics.Publisher{})
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
	})

	t.Run("malformed response body — returns unmarshal error", func(t *testing.T) {
		nc := startTestNATS(t)

		_, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
			_ = m.Respond([]byte("not json"))
		})
		require.NoError(t, err)

		client := newNatsBadgeClient(nc, natsmetrics.Publisher{})
		got, err := client.Counts(context.Background(), siteID, roomID, accounts)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "unmarshal badge count batch response")
	})
}

// The badge RPC blocks the notification handler, so its client series is the
// only thing that separates a slow user-service from consumer lag. That makes
// exactly-once recording load-bearing, not cosmetic: a stray second call
// alongside the deferred one doubles the count, halves the apparent error rate
// on a failing call (one success sample and one error sample for one RPC), and
// skews the latency distribution — which is exactly what happened when the
// deferred recorder was added and the immediate one was not removed.
//
// The outcome is the function's, not nc.Request's: a remote errcode envelope
// and a decode failure both arrive as a successful transport read.
func TestNatsBadgeClient_RecordsClientCallExactlyOnce(t *testing.T) {
	const (
		siteID = "site-a"
		roomID = "room-1"
	)

	tests := []struct {
		name      string
		subscribe bool
		reply     []byte
		wantErr   bool
		wantClass string // "" means a successful call, which carries no error.type
	}{
		{
			name:      "success",
			subscribe: true,
			reply:     []byte(`{"counts":{"alice":3}}`),
		},
		{
			name:      "remote errcode envelope",
			subscribe: true,
			reply:     mustJSON(errcode.NotFound("nope")),
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "undecodable reply",
			subscribe: true,
			reply:     []byte("{not json"),
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			// Parse recognises this envelope; Code.Valid() does not. Gating the
			// whole branch on Valid() let it fall through to the decoder, which
			// ignores unknown fields, so the call returned a zero-value map, a
			// nil error, and a success sample.
			name:      "unknown remote error code",
			subscribe: true,
			reply:     []byte(`{"code":"upstream_only_code","error":"upstream boom"}`),
			wantErr:   true,
			wantClass: "other_error",
		},
		{
			name:      "no responder",
			wantErr:   true,
			wantClass: "no_responders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc := startTestNATS(t)
			if tt.subscribe {
				sub, err := nc.Subscribe(context.Background(), subject.BadgeCountBatch(siteID), func(_ context.Context, m *nats.Msg) {
					_ = m.Respond(tt.reply)
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = sub.Unsubscribe() })
			}

			pub, recorded := badgeMetricFor(t)
			got, err := newNatsBadgeClient(nc, pub).Counts(context.Background(), siteID, roomID, []string{"alice"})
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, got, "a failed call must not hand back a zero-value success")
			} else {
				require.NoError(t, err)
			}

			count, methodTotal := recorded(natsmetrics.MethodBatchGetBadgeCounts, tt.wantClass)
			assert.Equal(t, uint64(1), count, "one call recorded as %q", tt.wantClass)
			assert.Equal(t, uint64(1), methodTotal, "recorded exactly once, under one label set")
		})
	}
}

func mustJSON(v any) []byte {
	out, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return out
}

// badgeMetricFor returns a Publisher writing into a manual reader, plus a
// lookup over rpc.client.call.duration keyed by rpc.method and error.type.
func badgeMetricFor(t *testing.T) (natsmetrics.Publisher, func(method natsmetrics.RPCMethod, errorType string) (count, methodTotal uint64)) {
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

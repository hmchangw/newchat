package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/subject"
)

func TestBroadcastMetrics_BoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newBroadcastMetrics(mp.Meter("test"))
	m.Fanout(context.Background(), "dm", "created", 3)
	m.Delivery(context.Background(), "dynamic-room", "raw-event", errors.New("secret"))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var fanoutCount uint64
	var deliveries int64
	var deliveryLabels map[string]string
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			switch metric.Name {
			case "broadcast_worker_fanout_recipients":
				fanoutCount = metric.Data.(metricdata.Histogram[int64]).DataPoints[0].Count
			case "broadcast_worker_recipient_deliveries_total":
				point := metric.Data.(metricdata.Sum[int64]).DataPoints[0]
				deliveries = point.Value
				deliveryLabels = map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					deliveryLabels[string(attr.Key)] = attr.Value.AsString()
				}
			}
		}
	}
	assert.Equal(t, uint64(1), fanoutCount)
	assert.Equal(t, int64(1), deliveries)
	assert.Equal(t, map[string]string{"room_kind": "unknown", "event_type": "unknown", "result": "failed"}, deliveryLabels)
}

func TestHandler_PublishToThreadAccounts_RecordsFanoutScenarios(t *testing.T) {
	tests := []struct {
		name           string
		accounts       []string
		failedAccounts []string
		wantError      bool
		wantFanout     int64
		wantSuccess    int64
		wantFailed     int64
		wantTerminal   int64
	}{
		{name: "zero recipients", wantFanout: 0},
		{name: "full success", accounts: []string{"alice", "bob"}, wantFanout: 2, wantSuccess: 2},
		{name: "partial success", accounts: []string{"alice", "bob"}, failedAccounts: []string{"bob"}, wantFanout: 2, wantSuccess: 1, wantFailed: 1, wantTerminal: 1},
		{name: "total failure", accounts: []string{"alice", "bob"}, failedAccounts: []string{"alice", "bob"}, wantError: true, wantFanout: 2, wantFailed: 2},
		{name: "duplicate recipients remain separate attempts", accounts: []string{"alice", "alice"}, wantFanout: 2, wantSuccess: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newBroadcastMetrics(mp.Meter("test"))
			pub := &mockPublisher{failOn: map[string]error{}}
			for _, account := range tt.failedAccounts {
				pub.failOn[subject.UserRoomEvent(account)] = errors.New("publish failed")
			}
			h := NewHandler(nil, nil, pub, nil, nil, false, fixedRoutes(subject.RouteGlobal), withBroadcastMetrics(metrics))
			ctx := context.Background()
			consumer := natsmetrics.New(mp.Meter("shared")).Consumer(natsmetrics.ConsumerConfig{
				Site: "site-a", Stream: "MESSAGES_CANONICAL_site-a", Consumer: "broadcast-worker",
			})
			consumer.LoopStarted(ctx)
			tracked := consumer.Track(ctx, stubJSMsg{subject: "chat.msg.canonical.site-a.created"}, natsmetrics.EventCreated, 5)
			ctx = tracked.Context(ctx)
			ctx = withBroadcastMetricLabels(ctx, roomThread, natsmetrics.EventCreated)

			err := h.publishToThreadAccounts(ctx, tt.accounts, []byte(`{"type":"test"}`), "parent-id")
			tracked.Finish(ctx)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			var fanoutCount uint64
			var fanoutSum int64
			deliveries := map[string]int64{}
			var terminal int64
			for _, scope := range rm.ScopeMetrics {
				for _, metric := range scope.Metrics {
					switch metric.Name {
					case "broadcast_worker_fanout_recipients":
						point := metric.Data.(metricdata.Histogram[int64]).DataPoints[0]
						fanoutCount = point.Count
						fanoutSum = point.Sum
					case "broadcast_worker_recipient_deliveries_total":
						for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
							for _, attr := range point.Attributes.ToSlice() {
								if string(attr.Key) == "result" {
									deliveries[attr.Value.AsString()] = point.Value
								}
							}
						}
					case "chat.nats.terminal.failures":
						for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
							for _, attr := range point.Attributes.ToSlice() {
								if string(attr.Key) == "reason" && attr.Value.AsString() == "publish_exhausted" {
									terminal += point.Value
								}
							}
						}
					}
				}
			}
			assert.Equal(t, uint64(1), fanoutCount)
			assert.Equal(t, tt.wantFanout, fanoutSum)
			assert.Equal(t, tt.wantSuccess, deliveries["success"])
			assert.Equal(t, tt.wantFailed, deliveries["failed"])
			assert.Equal(t, tt.wantTerminal, terminal)
		})
	}
}

func TestBroadcastMetrics_Record_NilReceiverIsSafe(t *testing.T) {
	var metrics *broadcastMetrics
	metrics.Fanout(context.Background(), roomChannel, natsmetrics.EventCreated, 1)
	metrics.Delivery(context.Background(), roomChannel, natsmetrics.EventCreated, nil)
}

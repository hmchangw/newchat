package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/hmchangw/chat/pkg/model"
	"github.com/hmchangw/chat/pkg/roomsubcache"
)

func TestNotificationMetrics_BoundedLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newNotificationMetrics(mp.Meter("test"))
	m.Record(context.Background(), "push", "sent")
	m.Record(context.Background(), "dynamic", "secret")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "notification_worker_outcomes_total" {
				continue
			}
			for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
				values := map[string]string{}
				for _, attr := range point.Attributes.ToSlice() {
					values[string(attr.Key)] = attr.Value.AsString()
				}
				got[values["kind"]+"/"+values["result"]] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"push/sent": 1, "unknown/failed": 1}, got)
}

func TestHandler_HandleMessage_RecordsNotificationOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		messageType string
		emitter     Emitter
		wantResult  string
		wantError   bool
	}{
		{name: "sent", messageType: model.MessageTypeImportant, emitter: &recordingEmitter{}, wantResult: "sent"},
		{name: "suppressed", messageType: model.MessageTypeRoomRenamed, emitter: &recordingEmitter{}, wantResult: "suppressed"},
		{name: "publish failed", messageType: model.MessageTypeImportant, emitter: failingEmitter{err: errors.New("publish failed")}, wantResult: "publish_failed", wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			metrics := newNotificationMetrics(mp.Meter("test"))
			h := NewHandler(HandlerDeps{
				Members: &stubMembers{out: map[string][]roomsubcache.Member{
					"r1": {{ID: "alice", Account: "alice"}, {ID: "bob", Account: "bob"}},
				}},
				Followers:          &stubFollowers{},
				Parent:             stubParent{},
				Presence:           noopPresenceSnapshotter{},
				Settings:           noopUserSettings{},
				Hook:               noopVetoer{},
				Emitter:            tt.emitter,
				LargeRoomThreshold: 500,
				Metrics:            metrics,
			})

			err := h.HandleMessage(context.Background(), msgEvent(&model.Message{
				ID: "m1", RoomID: "r1", UserID: "alice", UserAccount: "alice",
				Type: tt.messageType, CreatedAt: time.Now(),
			}))
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			var rm metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(context.Background(), &rm))
			var got int64
			for _, scope := range rm.ScopeMetrics {
				for _, metric := range scope.Metrics {
					if metric.Name != "notification_worker_outcomes_total" {
						continue
					}
					for _, point := range metric.Data.(metricdata.Sum[int64]).DataPoints {
						attrs := map[string]string{}
						for _, attr := range point.Attributes.ToSlice() {
							attrs[string(attr.Key)] = attr.Value.AsString()
						}
						if attrs["kind"] == "push" && attrs["result"] == tt.wantResult {
							got += point.Value
						}
					}
				}
			}
			assert.Equal(t, int64(1), got)
		})
	}
}

func TestNotificationMetrics_Record_NilReceiverIsSafe(t *testing.T) {
	var metrics *notificationMetrics
	metrics.Record(context.Background(), notifyKindPush, notifySent)
}

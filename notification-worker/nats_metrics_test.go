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

// collectMentionFailures reads the mention-failure counter back out of a manual
// reader. Returns 0 when the instrument has recorded nothing.
func collectMentionFailures(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "notification_worker_mention_lookup_failures_total" {
				continue
			}
			var total int64
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

// TestHandle_MentionLookupFailureIsCounted pins the one mention metric that
// survives: only a failed lookup is counted. Unresolved tokens are a normal
// outcome (someone mentioned an unknown account) and must stay silent, so the
// counter means "the users lookup is broken", not "a name was missing".
func TestHandle_MentionLookupFailureIsCounted(t *testing.T) {
	tests := []struct {
		name  string
		names *stubMentionNames
		want  int64
	}{
		{
			name:  "successful lookup records nothing",
			names: &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}},
			want:  0,
		},
		{
			name:  "unresolved account records nothing",
			names: &stubMentionNames{out: map[string]string{}},
			want:  0,
		},
		{
			name:  "failed lookup records one",
			names: &stubMentionNames{err: errors.New("mongo down")},
			want:  1,
		},
		{
			name: "partial failure still records one",
			names: &stubMentionNames{
				out: map[string]string{"bob": "Bob Chen"},
				err: errors.New("mongo down"),
			},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			h := NewHandler(HandlerDeps{
				Members:            mentionMembers(),
				Followers:          &stubFollowers{},
				Parent:             stubParent{},
				Presence:           noopPresenceSnapshotter{},
				Hook:               noopVetoer{},
				Emitter:            &recordingEmitter{},
				MentionNames:       tt.names,
				Metrics:            newNotificationMetrics(mp.Meter("test")),
				LargeRoomThreshold: 500,
			})

			require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@bob and @ghost")))

			assert.Equal(t, tt.want, collectMentionFailures(t, reader))
		})
	}
}

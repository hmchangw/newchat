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

// collectMentionResults reads the mention counter back out of a manual reader,
// keyed by its `result` attribute.
func collectMentionResults(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "notification_worker_mention_resolution_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				for _, attr := range point.Attributes.ToSlice() {
					if attr.Key == "result" {
						got[attr.Value.AsString()] = point.Value
					}
				}
			}
		}
	}
	return got
}

// TestRecordMentionResults_PartitionsTokens pins the metric contract that makes
// the fail-open path observable: resolved+unresolved+failed always equals the
// tokens looked up, and a failed lookup's remainder is attributed to `failed`
// rather than `unresolved`, so an outage is distinguishable from users simply
// mentioning unknown accounts.
func TestRecordMentionResults_PartitionsTokens(t *testing.T) {
	tests := []struct {
		name     string
		lookup   []string
		resolved map[string]string
		err      error
		want     map[string]int64
	}{
		{
			name: "all resolved", lookup: []string{"a", "b"},
			resolved: map[string]string{"a": "A", "b": "B"},
			want:     map[string]int64{"resolved": 2},
		},
		{
			name: "none resolved, no error", lookup: []string{"a", "b"},
			resolved: map[string]string{},
			want:     map[string]int64{"unresolved": 2},
		},
		{
			name: "mixed, no error", lookup: []string{"a", "ghost"},
			resolved: map[string]string{"a": "A"},
			want:     map[string]int64{"resolved": 1, "unresolved": 1},
		},
		{
			name: "error attributes the remainder to failed", lookup: []string{"a", "ghost"},
			resolved: map[string]string{"a": "A"}, err: errors.New("boom"),
			want: map[string]int64{"resolved": 1, "failed": 1},
		},
		{
			name: "total failure", lookup: []string{"a", "b"},
			resolved: nil, err: errors.New("boom"),
			want: map[string]int64{"failed": 2},
		},
		{
			name: "empty display name counts as unresolved", lookup: []string{"a"},
			resolved: map[string]string{"a": ""},
			want:     map[string]int64{"unresolved": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			h := &Handler{metrics: newNotificationMetrics(mp.Meter("test"))}

			h.recordMentionResults(context.Background(), tt.lookup, tt.resolved, tt.err)

			got := collectMentionResults(t, reader)
			assert.Equal(t, tt.want, got)
			var total int64
			for _, v := range got {
				total += v
			}
			assert.Equal(t, int64(len(tt.lookup)), total, "every looked-up token lands in exactly one bucket")
		})
	}
}

func TestHandle_MentionResolutionIsCounted(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	names := &stubMentionNames{out: map[string]string{"bob": "Bob Chen"}}
	h := NewHandler(HandlerDeps{
		Members:            mentionMembers(),
		Followers:          &stubFollowers{},
		Parent:             stubParent{},
		Presence:           noopPresenceSnapshotter{},
		Hook:               noopVetoer{},
		Emitter:            &recordingEmitter{},
		MentionNames:       names,
		Metrics:            newNotificationMetrics(mp.Meter("test")),
		LargeRoomThreshold: 500,
	})

	require.NoError(t, h.HandleMessage(context.Background(), mentionMsg("@bob and @ghost and @all")))

	assert.Equal(t, map[string]int64{"resolved": 1, "unresolved": 1}, collectMentionResults(t, reader),
		"@all resolves from a constant and is not counted as a lookup")
}

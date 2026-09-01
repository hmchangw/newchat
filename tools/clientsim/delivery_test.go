package main

import (
	"encoding/json"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/model"
)

func roomEventJSON(t *testing.T, ts, eventTS int64) []byte {
	t.Helper()
	data, err := json.Marshal(model.RoomEvent{
		Type: model.RoomEventNewMessage, RoomID: "r1",
		Timestamp: ts, EventTimestamp: eventTS,
	})
	require.NoError(t, err)
	return data
}

func TestHandleDelivery_ObservesBothLatencies(t *testing.T) {
	m := newMetrics()
	now := time.Now()
	data := roomEventJSON(t,
		now.Add(-50*time.Millisecond).UnixMilli(),
		now.Add(-120*time.Millisecond).UnixMilli())

	handleDelivery(m, "channel", data, now)

	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
	assert.Equal(t, uint64(1), histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
	assert.Equal(t, uint64(1), histogramCount(t, m.Registry, "clientsim_canonical_to_client_latency_seconds"))
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
}

func TestHandleDelivery_InvalidTimestamps(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name        string
		ts, eventTS int64
		wantInvalid float64
		wantBcast   uint64
	}{
		{"zero broadcast ts on new_message is a contract violation", 0, now.Add(-1 * time.Millisecond).UnixMilli(), 1, 0},
		{"future broadcast ts (negative age)", now.Add(time.Minute).UnixMilli(), now.Add(-1 * time.Millisecond).UnixMilli(), 1, 0},
		{"zero canonical ts still observes broadcast", now.Add(-5 * time.Millisecond).UnixMilli(), 0, 0, 1},
		{"small future ts within skew tolerance observes zero, not invalid",
			now.Add(500 * time.Millisecond).UnixMilli(), now.Add(-1 * time.Millisecond).UnixMilli(), 0, 1},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			m := newMetrics()
			handleDelivery(m, "user", roomEventJSON(t, tt.ts, tt.eventTS), now)
			assert.InDelta(t, tt.wantInvalid, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
			assert.Equal(t, tt.wantBcast, histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
			// Delivery is counted regardless of timestamp quality.
			assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")), 0.001)
		})
	}
}

func TestHandleDelivery_DecodeFailure(t *testing.T) {
	m := newMetrics()
	handleDelivery(m, "user", []byte("{not json"), time.Now())
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.DecodeFailures), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("user")), 0.001)
}

func TestHandleDelivery_NonRoomEventJSONStillCounts(t *testing.T) {
	// Other event types on the same subjects (edits, read receipts) decode
	// but legitimately omit these stamps -> counted, no latency, not invalid.
	m := newMetrics()
	handleDelivery(m, "channel", []byte(`{"type":"message_read","roomId":"r1"}`), time.Now())
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
	assert.Equal(t, uint64(0), histogramCount(t, m.Registry, "clientsim_broadcast_to_client_latency_seconds"))
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
}

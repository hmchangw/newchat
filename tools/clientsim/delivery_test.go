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
		// Both stamps are mandatory on a new_message: the broadcast sample is
		// still observed, and the missing canonical stamp is still evidence.
		{"zero canonical ts is evidence and does not block the broadcast sample",
			now.Add(-5 * time.Millisecond).UnixMilli(), 0, 1, 1},
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

// broadcast-worker builds every new_message RoomEvent from the canonical
// event's own Timestamp, which CLAUDE.md requires every NATS event to carry.
// A zero eventTimestamp on a new_message is therefore corruption, not an
// optional field — and skipping it silently computes the canonical-latency
// p99 over an unknown subset of the traffic.
func TestHandleDelivery_NewMessageWithoutAnEventTimestampIsEvidence(t *testing.T) {
	m := newMetrics()
	handleDelivery(m, "channel", []byte(`{"type":"new_message","timestamp":1}`), time.UnixMilli(2))
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001,
		"a new_message missing eventTimestamp must count as evidence")
}

// Other event types on the same subjects legitimately omit both stamps.
func TestHandleDelivery_OtherEventTypesStillSkipSilently(t *testing.T) {
	m := newMetrics()
	handleDelivery(m, "channel", []byte(`{"type":"thread_metadata_updated"}`), time.UnixMilli(2))
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.InvalidTimestamp), 0.001)
}

// json.Unmarshal accepts `null` and `{}` into a zero envelope: the delivery is
// counted, the empty type makes both timestamps non-strict, and every loss
// counter stays at zero — so a responder emitting garbage reads as a clean,
// non-degraded window. The event type is mandatory.
func TestHandleDelivery_SchemaInvalidPayloadIsNotACleanDelivery(t *testing.T) {
	for _, payload := range []string{`null`, `{}`, `{"roomId":"r1"}`} {
		t.Run(payload, func(t *testing.T) {
			m := newMetrics()
			handleDelivery(m, "channel", []byte(payload), time.Now())
			assert.InDelta(t, 1, promtestutil.ToFloat64(m.DecodeFailures), 0.001,
				"an envelope with no event type is evidence, not a clean delivery")
		})
	}
}

// A typed event that simply is not new_message stays clean.
func TestHandleDelivery_ATypedNonMessageEventStaysClean(t *testing.T) {
	m := newMetrics()
	handleDelivery(m, "channel", []byte(`{"type":"thread_metadata_updated"}`), time.Now())
	assert.InDelta(t, 0, promtestutil.ToFloat64(m.DecodeFailures), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Delivered.WithLabelValues("channel")), 0.001)
}

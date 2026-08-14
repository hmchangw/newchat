package main

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestLoadgenNATSHealth_TracksDisconnectAndRecovery(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	health := newLoadgenNATSHealth("soak", metrics, func() time.Time { return now })

	health.connected()
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("soak"),
	))

	health.disconnected(errors.New("connection reset"))
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("soak"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("soak", "disconnected"),
	))
	now = now.Add(7 * time.Second)
	health.updateCurrentOutage()
	assert.Equal(t, float64(7), testutil.ToFloat64(metrics.NATSCurrentOutage.WithLabelValues("soak")))
	health.reconnected("nats://nats-2:4222")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("soak"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("soak", "reconnected"),
	))
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.NATSOutageDuration))
}

func TestLoadgenNATSHealth_ClosedInvalidatesConnectionState(t *testing.T) {
	metrics := NewMetrics()
	health := newLoadgenNATSHealth("members", metrics, time.Now)
	health.connected()
	health.closed()

	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("members"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("members", "closed"),
	))
}

func TestLoadgenNATSHealth_AggregatesEveryConnectionInPool(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	first := newLoadgenNATSHealth("recipient_observer", metrics, func() time.Time { return now })
	second := newLoadgenNATSHealth("recipient_observer", metrics, func() time.Time { return now })
	first.connected()
	second.connected()
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	))

	first.disconnected(errors.New("connection reset"))
	second.reconnected("nats://still-connected")
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	), "one healthy connection must not hide another connection outage")
	now = now.Add(3 * time.Second)
	first.updateCurrentOutage()
	assert.Equal(t, float64(3), testutil.ToFloat64(
		metrics.NATSCurrentOutage.WithLabelValues("recipient_observer"),
	))

	first.reconnected("nats://recovered")
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	))
	second.closed()
	first.reconnected("nats://duplicate")
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	), "a permanently closed connection keeps the logical pool down")
}

func TestLoadgenNATSHealth_InitialConnectedDoesNotOverwriteCallbackState(t *testing.T) {
	tests := []struct {
		name       string
		transition func(*loadgenNATSHealth)
	}{
		{
			name: "disconnected",
			transition: func(health *loadgenNATSHealth) {
				health.disconnected(errors.New("connection reset"))
			},
		},
		{
			name:       "closed",
			transition: func(health *loadgenNATSHealth) { health.closed() },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics := NewMetrics()
			health := newLoadgenNATSHealth("soak", metrics, time.Now)

			tt.transition(health)
			health.connected()

			assert.Equal(t, float64(0), testutil.ToFloat64(
				metrics.NATSConnected.WithLabelValues("soak"),
			))
			assert.Equal(t, float64(0), testutil.ToFloat64(
				metrics.NATSConnectionEvents.WithLabelValues("soak", "connected"),
			))
		})
	}
}

func TestLoadgenNATSHealth_RejectsUnboundedPool(t *testing.T) {
	assert.Nil(t, newLoadgenNATSHealth("message-123-user-input", NewMetrics(), time.Now))
	connection, err := dialNATSPoolWithMetrics("nats://unused", "", "message-123-user-input", NewMetrics(), nil)
	assert.Nil(t, connection)
	assert.Error(t, err)
	var nilHealth *loadgenNATSHealth
	nilHealth.connected()
	nilHealth.disconnected(nil)
	nilHealth.reconnected("")
	nilHealth.closed()
	nilHealth.asyncError(nil)
	nilHealth.bufferFull(nil)
	nilHealth.updateCurrentOutage()
}

func TestLoadgenNATSHealth_ObserverDisconnectOverflowAndReconnect(t *testing.T) {
	now := time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC)
	metrics := NewMetrics()
	observer := newFailureObserverHealth(failureObserverRecipient, now)
	health := newLoadgenNATSHealth("recipient_observer", metrics, func() time.Time { return now })
	health.observer = observer
	health.connected()
	health.asyncError(errors.New("permission violation"))
	health.bufferFull(errors.New("slow consumer"))
	assert.False(t, observer.Snapshot(now).Up)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("recipient_observer", "async_error"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("recipient_observer", "buffer_full"),
	))
	health.disconnected(errors.New("connection reset"))
	now = now.Add(time.Second)
	health.reconnected("nats://redacted")
	assert.True(t, observer.Snapshot(now).Up)
	health.closed()
	assert.False(t, observer.Snapshot(now).Up)
	// Duplicate callbacks after close must not mutate state or panic.
	health.disconnected(errors.New("late"))
	health.reconnected("nats://late")
}

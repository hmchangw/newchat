package main

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

type lockedFailureClock struct {
	mu  sync.Mutex
	now time.Time
}

func newLockedFailureClock(now time.Time) *lockedFailureClock {
	return &lockedFailureClock{now: now}
}

func (c *lockedFailureClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *lockedFailureClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

func TestLoadgenNATSHealth_TracksDisconnectAndRecovery(t *testing.T) {
	clock := newLockedFailureClock(time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC))
	metrics := NewMetrics()
	t.Cleanup(metrics.StopNATSHealth)
	health := newLoadgenNATSHealth("soak", metrics, clock.Now)

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
	clock.Advance(7 * time.Second)
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
	t.Cleanup(metrics.StopNATSHealth)
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
	clock := newLockedFailureClock(time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC))
	metrics := NewMetrics()
	t.Cleanup(metrics.StopNATSHealth)
	first := newLoadgenNATSHealth("recipient_observer", metrics, clock.Now)
	second := newLoadgenNATSHealth("recipient_observer", metrics, clock.Now)
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
	clock.Advance(3 * time.Second)
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
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	), "a retired connection must not keep the remaining active pool down")
	first.closed()
	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("recipient_observer"),
	), "closing the final active connection makes the pool unavailable")
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
			t.Cleanup(metrics.StopNATSHealth)
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
	clock := newLockedFailureClock(time.Date(2026, 8, 12, 1, 2, 3, 0, time.UTC))
	metrics := NewMetrics()
	t.Cleanup(metrics.StopNATSHealth)
	observer := newFailureObserverHealth(failureObserverRecipient, clock.Now())
	health := newLoadgenNATSHealth("recipient_observer", metrics, clock.Now)
	health.observer = observer
	health.connected()
	health.asyncError(errors.New("permission violation"))
	health.bufferFull(errors.New("slow consumer"))
	assert.False(t, observer.Snapshot(clock.Now()).Up)
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("recipient_observer", "async_error"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("recipient_observer", "buffer_full"),
	))
	health.disconnected(errors.New("connection reset"))
	clock.Advance(time.Second)
	health.reconnected("nats://redacted")
	assert.True(t, observer.Snapshot(clock.Now()).Up)
	health.closed()
	assert.False(t, observer.Snapshot(clock.Now()).Up)
	// Duplicate callbacks after close must not mutate state or panic.
	health.disconnected(errors.New("late"))
	health.reconnected("nats://late")
}

func TestLoadgenNATSHealth_StopTerminatesOutageTickers(t *testing.T) {
	metrics := NewMetrics()
	health := newLoadgenNATSHealth("soak", metrics, time.Now)
	health.connected()
	health.disconnected(errors.New("connection reset"))

	metrics.StopNATSHealth()
	health.reconnected("nats://late-reconnect")
	health.disconnected(errors.New("late disconnect"))

	health.poolState.mu.Lock()
	assert.Nil(t, health.poolState.outageStop)
	health.poolState.mu.Unlock()
}

func TestFailureNATSConnect_WrapsConnectionError(t *testing.T) {
	metrics := NewMetrics()
	t.Cleanup(metrics.StopNATSHealth)

	connection, err := connectWithCredsHealth("://invalid", "test", "", "daily", metrics)

	assert.Nil(t, connection)
	assert.ErrorContains(t, err, "connect NATS pool daily")
}

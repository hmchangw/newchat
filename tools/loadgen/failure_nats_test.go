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
	health := newLoadgenNATSHealth(metrics, func() time.Time { return now })

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
	health := newLoadgenNATSHealth(metrics, time.Now)
	health.connected()
	health.closed()

	assert.Equal(t, float64(0), testutil.ToFloat64(
		metrics.NATSConnected.WithLabelValues("soak"),
	))
	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.NATSConnectionEvents.WithLabelValues("soak", "closed"),
	))
}

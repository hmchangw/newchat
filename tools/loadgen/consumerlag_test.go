package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumerSampler_SnapshotInitialState(t *testing.T) {
	m := NewMetrics()
	s, err := NewConsumerSampler(nil, "MESSAGES-CANONICAL-site-local", "message-worker", m, time.Second)
	require.NoError(t, err)
	snap := s.Snapshot()
	assert.Equal(t, "MESSAGES-CANONICAL-site-local", snap.Stream)
	assert.Equal(t, "message-worker", snap.Durable)
	assert.Equal(t, uint64(0), snap.MinPending)
	assert.Equal(t, uint64(0), snap.PeakPending)
	assert.Equal(t, uint64(0), snap.FinalPending)
	assert.Equal(t, uint64(0), snap.PeakAckPending)
	assert.Equal(t, uint64(0), snap.Redelivered)
}

func TestConsumerSampler_SampleErrorIsMetricEvidence(t *testing.T) {
	m := NewMetrics()
	var invalidReason string
	s, err := NewConsumerSampler(
		nil,
		"MESSAGES-CANONICAL-site-local",
		"message-worker",
		m,
		time.Second,
		withConsumerSamplerInvalidation(func(reason string) { invalidReason = reason }),
	)
	require.NoError(t, err)
	m.ConsumerAckFloorStall.WithLabelValues(
		"MESSAGES-CANONICAL-site-local", "message-worker",
	).Set(17)
	s.sample = func(context.Context) (*jetstream.ConsumerInfo, error) { return nil, errors.New("unavailable") }
	s.sampleOnce(context.Background())
	assert.Equal(t, float64(1), testutil.ToFloat64(m.ConsumerSampleErrors.WithLabelValues("MESSAGES-CANONICAL-site-local", "message-worker", "lookup")))
	assert.Equal(t, "lookup", invalidReason)
	assert.Equal(t, uint64(1), s.Snapshot().SampleErrors)
	assert.False(t, gatheredMetricFamilyExists(t, m, "loadgen_consumer_ack_floor_stall_seconds"))
}

func TestNewConsumerSampler_SnapshotDifferentParams(t *testing.T) {
	m := NewMetrics()
	s, err := NewConsumerSampler(nil, "ROOMS-site-remote", "room-worker", m, 500*time.Millisecond)
	require.NoError(t, err)
	snap := s.Snapshot()
	assert.Equal(t, "ROOMS-site-remote", snap.Stream)
	assert.Equal(t, "room-worker", snap.Durable)
	// All counters start at zero before any samples are taken.
	assert.Equal(t, uint64(0), snap.MinPending)
	assert.Equal(t, uint64(0), snap.PeakPending)
	assert.Equal(t, uint64(0), snap.FinalPending)
	assert.Equal(t, uint64(0), snap.PeakAckPending)
	assert.Equal(t, uint64(0), snap.Redelivered)
}

func TestNewConsumerSampler_AcceptsBoundedMessageSoakTargets(t *testing.T) {
	tests := []struct {
		stream  string
		durable string
	}{
		{stream: "MESSAGES-site-local", durable: "message-gatekeeper"},
		{stream: "MESSAGES-CANONICAL-site-local", durable: "message-worker"},
		{stream: "MESSAGES-CANONICAL-site-local", durable: "broadcast-worker"},
		{stream: "MESSAGES-CANONICAL-site-local", durable: "notification-worker"},
	}
	for _, tt := range tests {
		t.Run(tt.stream+"/"+tt.durable, func(t *testing.T) {
			_, err := NewConsumerSampler(nil, tt.stream, tt.durable, NewMetrics(), time.Second)
			require.NoError(t, err)
		})
	}
}

func TestConsumerSampler_AckFloorStallRequiresPendingWorkAndTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 15, 14, 0, 0, 0, time.UTC)
	lastAck := now.Add(-17 * time.Second)
	metrics := NewMetrics()
	sampler, err := NewConsumerSampler(
		nil,
		"MESSAGES-CANONICAL-site-local",
		"message-worker",
		metrics,
		time.Second,
		withConsumerSamplerNow(func() time.Time { return now }),
	)
	require.NoError(t, err)

	sampler.recordInfo(&jetstream.ConsumerInfo{
		NumPending: 1,
		AckFloor:   jetstream.SequenceInfo{Last: &lastAck},
	})
	assert.Equal(t, float64(17), testutil.ToFloat64(
		metrics.ConsumerAckFloorStall.WithLabelValues(
			"MESSAGES-CANONICAL-site-local", "message-worker",
		),
	))

	sampler.recordInfo(&jetstream.ConsumerInfo{
		NumPending: 0,
		AckFloor:   jetstream.SequenceInfo{Last: &lastAck},
	})
	assert.False(t, gatheredMetricFamilyExists(t, metrics, "loadgen_consumer_ack_floor_stall_seconds"))

	sampler.recordInfo(&jetstream.ConsumerInfo{NumPending: 1})
	assert.False(t, gatheredMetricFamilyExists(t, metrics, "loadgen_consumer_ack_floor_stall_seconds"))
}

func gatheredMetricFamilyExists(t *testing.T, metrics *Metrics, name string) bool {
	t.Helper()
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}

func TestNewConsumerSampler_RejectsUnboundedLabelsAndInterval(t *testing.T) {
	tests := []struct {
		name     string
		stream   string
		durable  string
		interval time.Duration
	}{
		{name: "unknown stream", stream: "tenant-secret", durable: "message-worker", interval: time.Second},
		{name: "unknown durable", stream: "MESSAGES-CANONICAL-site-local", durable: "dynamic-user", interval: time.Second},
		{name: "zero interval", stream: "MESSAGES-CANONICAL-site-local", durable: "message-worker"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConsumerSampler(nil, tc.stream, tc.durable, NewMetrics(), tc.interval)
			assert.Error(t, err)
		})
	}
}

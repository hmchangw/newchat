package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestNewConsumerSampler_SnapshotInitialState(t *testing.T) {
	m := NewMetrics()
	s := NewConsumerSampler(nil, "MESSAGES-CANONICAL-site-local", "message-worker", m, 1*time.Second)
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
	s := NewConsumerSampler(nil, "MESSAGES_CANONICAL_site-local", "message-worker", m, time.Second)
	s.sample = func(context.Context) (*jetstream.ConsumerInfo, error) { return nil, errors.New("unavailable") }
	s.sampleOnce(context.Background())
	assert.Equal(t, float64(1), testutil.ToFloat64(m.ConsumerSampleErrors.WithLabelValues("MESSAGES_CANONICAL_site-local", "message-worker", "lookup")))
}

func TestNewConsumerSampler_SnapshotDifferentParams(t *testing.T) {
	m := NewMetrics()
	s := NewConsumerSampler(nil, "MESSAGES-CANONICAL-site-remote", "broadcast-worker", m, 500*time.Millisecond)
	snap := s.Snapshot()
	assert.Equal(t, "MESSAGES-CANONICAL-site-remote", snap.Stream)
	assert.Equal(t, "broadcast-worker", snap.Durable)
	// All counters start at zero before any samples are taken.
	assert.Equal(t, uint64(0), snap.MinPending)
	assert.Equal(t, uint64(0), snap.PeakPending)
	assert.Equal(t, uint64(0), snap.FinalPending)
	assert.Equal(t, uint64(0), snap.PeakAckPending)
	assert.Equal(t, uint64(0), snap.Redelivered)
}

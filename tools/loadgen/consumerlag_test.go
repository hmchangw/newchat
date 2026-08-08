package main

import (
	"testing"
	"time"

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

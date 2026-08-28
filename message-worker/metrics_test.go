package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/cassutil"
)

func TestNewMetrics_RecordsWithoutPanicking(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)

	// Recording must never panic against a real instance backed by a no-op meter provider.
	m.onHistoryWriteFailure(cassutil.CQLInfra.String())
	m.onDropped("invalid")
	m.setDegraded(true)
	m.setLag(42, 12.5)

	assert.Equal(t, uint64(42), m.numPending.Load())
	assert.Equal(t, int64(1), m.degraded.Load())
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics // the handler's mtr field is nil in tests that don't care about metrics
	require.NotPanics(t, func() {
		m.onHistoryWriteFailure(cassutil.CQLRequest.String())
		m.onDropped("syntax")
		m.setDegraded(true)
		m.setLag(42, 12.5)
	})
}

func TestStartLagPoller_RecordsConsumerInfo(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)

	last := time.Now().UTC().Add(-30 * time.Second)
	var calls atomic.Int64
	polled := make(chan struct{}, 1)

	info := func(ctx context.Context) (*jetstream.ConsumerInfo, error) {
		calls.Add(1)
		select {
		case polled <- struct{}{}:
		default:
		}
		return &jetstream.ConsumerInfo{
			NumPending: 7,
			AckFloor:   jetstream.SequenceInfo{Last: &last},
		}, nil
	}

	stop := startLagPoller(context.Background(), m, info, 5*time.Millisecond)
	t.Cleanup(stop)

	select {
	case <-polled:
	case <-time.After(2 * time.Second):
		t.Fatal("poller never called consumer info")
	}

	assert.Eventually(t, func() bool { return m.numPending.Load() == 7 }, 2*time.Second, 5*time.Millisecond)
	assert.GreaterOrEqual(t, m.ackFloorAgeSeconds.Load(), int64(29))
}

func TestStartLagPoller_StopTerminatesGoroutine(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)

	var calls atomic.Int64
	info := func(ctx context.Context) (*jetstream.ConsumerInfo, error) {
		calls.Add(1)
		return nil, errors.New("consumer info unavailable")
	}

	stop := startLagPoller(context.Background(), m, info, 5*time.Millisecond)
	assert.Eventually(t, func() bool { return calls.Load() > 0 }, 2*time.Second, 5*time.Millisecond)

	stop()
	settled := calls.Load()
	time.Sleep(50 * time.Millisecond) // not synchronization: asserting the absence of further calls
	assert.Equal(t, settled, calls.Load(), "poller kept running after stop")
}

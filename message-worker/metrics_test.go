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
	m.setLag(42, 3, 12.5)
	m.onInfoPollFailure()

	assert.Equal(t, uint64(42), m.numPending.Load())
	assert.Equal(t, uint64(3), m.numAckPending.Load())
	assert.Equal(t, int64(1), m.infoPollFailures.Load())
	assert.Equal(t, int64(1), m.degraded.Load())
}

func TestMetrics_NilSafe(t *testing.T) {
	var m *metrics // the handler's mtr field is nil in tests that don't care about metrics
	require.NotPanics(t, func() {
		m.onHistoryWriteFailure(cassutil.CQLRequest.String())
		m.onDropped("syntax")
		m.setDegraded(true)
		m.setLag(42, 3, 12.5)
		m.onInfoPollFailure()
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
			NumPending:    7,
			NumAckPending: 3,
			AckFloor:      jetstream.SequenceInfo{Last: &last},
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

// TestMetrics_AckFloorAgeIsZeroWhenNothingIsPending guards the gauge the PR's ops
// section tells operators to alert on.
//
// AckFloor.Last is the timestamp of the last acknowledged message and keeps that
// value once the consumer goes quiet, so an age computed from it alone climbs
// forever on an idle site. The alert is meant to catch a retry loop that is stuck;
// unqualified it pages every low-traffic site overnight instead, and an alert that
// cries wolf is the one nobody reads during the incident it was built for.
func TestMetrics_AckFloorAgeIsZeroWhenNothingIsPending(t *testing.T) {
	last := time.Now().Add(-9 * time.Hour)

	tests := []struct {
		name           string
		numAckPending  int
		wantAgeAtMost  int64
		wantAgeAtLeast int64
	}{
		{name: "idle consumer reports no age", numAckPending: 0, wantAgeAtMost: 0},
		{name: "a stuck redelivery set reports the real age", numAckPending: 3, wantAgeAtLeast: 32000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newMetrics()
			require.NoError(t, err)
			info := func(context.Context) (*jetstream.ConsumerInfo, error) {
				return &jetstream.ConsumerInfo{
					NumPending:    1,
					NumAckPending: tt.numAckPending,
					AckFloor:      jetstream.SequenceInfo{Last: &last},
				}, nil
			}
			stop := startLagPoller(context.Background(), m, info, time.Hour)
			t.Cleanup(stop)

			assert.Eventually(t, func() bool { return m.numPending.Load() == 1 },
				2*time.Second, 5*time.Millisecond)
			if tt.numAckPending == 0 {
				assert.Equal(t, tt.wantAgeAtMost, m.ackFloorAgeSeconds.Load(),
					"nothing is waiting to be acked, so no ack is overdue")
			} else {
				assert.GreaterOrEqual(t, m.ackFloorAgeSeconds.Load(), tt.wantAgeAtLeast)
			}
		})
	}
}

// TestMetrics_PollFailuresAreCounted covers the silent half of the same problem.
//
// On a failed consumer-info poll the sampler returns without touching the gauges, so
// they hold their last value — zero if the very first poll failed. A site whose NATS
// connection is broken therefore reports a perfectly flat, healthy-looking lag. The
// counter is what lets an alert distinguish "no lag" from "no data".
func TestMetrics_PollFailuresAreCounted(t *testing.T) {
	m, err := newMetrics()
	require.NoError(t, err)

	info := func(context.Context) (*jetstream.ConsumerInfo, error) {
		return nil, errors.New("consumer info unavailable")
	}
	stop := startLagPoller(context.Background(), m, info, 5*time.Millisecond)
	t.Cleanup(stop)

	assert.Eventually(t, func() bool { return m.infoPollFailures.Load() > 0 },
		2*time.Second, 5*time.Millisecond,
		"a lag gauge frozen at zero is indistinguishable from a healthy one without this")
	assert.Equal(t, uint64(0), m.numPending.Load(), "a failed poll must not invent a reading")
}

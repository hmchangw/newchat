package jsretry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingHeartbeatMsg struct {
	mu   sync.Mutex
	n    int
	fail error
}

func (m *countingHeartbeatMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	return m.fail
}

func (m *countingHeartbeatMsg) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

func TestHeartbeat_ExtendsDeadlineUntilStopped(t *testing.T) {
	msg := &countingHeartbeatMsg{}
	stop := Heartbeat(context.Background(), msg, 5*time.Millisecond)
	t.Cleanup(stop)

	require.Eventually(t, func() bool { return msg.calls() >= 2 }, time.Second, time.Millisecond,
		"a handler outliving the interval must have its ack deadline extended")

	stop()
	settled := msg.calls()
	assert.Never(t, func() bool { return msg.calls() > settled }, 50*time.Millisecond, 5*time.Millisecond,
		"no heartbeat may fire after stop — the message is already settled")
}

// stop is the only termination path, so it must be safe on the panic path too.
func TestHeartbeat_StopIsIdempotent(t *testing.T) {
	msg := &countingHeartbeatMsg{}
	stop := Heartbeat(context.Background(), msg, time.Millisecond)
	stop()
	assert.NotPanics(t, stop)
}

func TestHeartbeat_CancelledContextStops(t *testing.T) {
	msg := &countingHeartbeatMsg{}
	ctx, cancel := context.WithCancel(context.Background())
	stop := Heartbeat(ctx, msg, 5*time.Millisecond)
	t.Cleanup(stop)

	require.Eventually(t, func() bool { return msg.calls() >= 1 }, time.Second, time.Millisecond)
	cancel()

	// A cancelled context must not leave a ticker goroutine running.
	require.Eventually(t, func() bool {
		n := msg.calls()
		time.Sleep(20 * time.Millisecond)
		return msg.calls() == n
	}, time.Second, 25*time.Millisecond)
}

// A failing InProgress means the message is gone; retrying would spam the log.
func TestHeartbeat_StopsAfterFailure(t *testing.T) {
	msg := &countingHeartbeatMsg{fail: errors.New("consumer deleted")}
	stop := Heartbeat(context.Background(), msg, 2*time.Millisecond)
	t.Cleanup(stop)

	require.Eventually(t, func() bool { return msg.calls() >= 1 }, time.Second, time.Millisecond)
	assert.Never(t, func() bool { return msg.calls() > 1 }, 50*time.Millisecond, 5*time.Millisecond)
}

func TestHeartbeat_NonPositiveIntervalIsDisabled(t *testing.T) {
	for _, every := range []time.Duration{0, -time.Second} {
		msg := &countingHeartbeatMsg{}
		stop := Heartbeat(context.Background(), msg, every)
		t.Cleanup(stop)
		assert.Never(t, func() bool { return msg.calls() > 0 }, 30*time.Millisecond, 5*time.Millisecond,
			"interval %s must disable the heartbeat entirely", every)
	}
}

func TestHeartbeatInterval(t *testing.T) {
	tests := []struct {
		name    string
		ackWait time.Duration
		want    time.Duration
	}{
		{"thirty second ack wait heartbeats at a third", 30 * time.Second, 10 * time.Second},
		{"one minute ack wait heartbeats at a third", 60 * time.Second, 20 * time.Second},
		{"short ack wait is floored, never zero", 100 * time.Millisecond, minHeartbeatInterval},
		{"zero ack wait disables the heartbeat", 0, 0},
		{"negative ack wait disables the heartbeat", -time.Second, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HeartbeatInterval(tt.ackWait))
		})
	}
}

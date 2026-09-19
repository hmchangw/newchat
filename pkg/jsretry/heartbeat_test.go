package jsretry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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
	stop := Heartbeat(context.Background(), msg, HeartbeatBudget{Every: 5 * time.Millisecond})
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
	stop := Heartbeat(context.Background(), msg, HeartbeatBudget{Every: time.Millisecond})
	stop()
	assert.NotPanics(t, stop)
}

func TestHeartbeat_CancelledContextStops(t *testing.T) {
	msg := &countingHeartbeatMsg{}
	ctx, cancel := context.WithCancel(context.Background())
	stop := Heartbeat(ctx, msg, HeartbeatBudget{Every: 5 * time.Millisecond})
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

func TestHeartbeat_NonPositiveIntervalIsDisabled(t *testing.T) {
	for _, every := range []time.Duration{0, -time.Second} {
		msg := &countingHeartbeatMsg{}
		stop := Heartbeat(context.Background(), msg, HeartbeatBudget{Every: every})
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

type funcHeartbeatMsg struct{ fn func() error }

func (m *funcHeartbeatMsg) InProgress() error { return m.fn() }

// stop is what main defers before settling the message, so it must not return
// while a heartbeat is still in flight — otherwise InProgress races the Ack.
func TestHeartbeat_StopWaitsForInFlightHeartbeat(t *testing.T) {
	var once sync.Once
	entered := make(chan struct{})
	release := make(chan struct{})
	var returned atomic.Bool

	msg := &funcHeartbeatMsg{fn: func() error {
		once.Do(func() { close(entered) })
		<-release
		returned.Store(true)
		return nil
	}}
	stop := Heartbeat(context.Background(), msg, HeartbeatBudget{Every: time.Millisecond})
	<-entered

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()

	select {
	case <-stopped:
		t.Fatal("stop returned while a heartbeat was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped
	assert.True(t, returned.Load(), "stop must wait for the in-flight heartbeat to finish")
}

// InProgress cannot say whether a failure is terminal or a transport blip, so
// the safe reading is transient: keep extending rather than silently stop.
func TestHeartbeat_RetriesAfterTransientFailure(t *testing.T) {
	msg := &countingHeartbeatMsg{fail: errors.New("connection reset")}
	stop := Heartbeat(context.Background(), msg, HeartbeatBudget{Every: 2 * time.Millisecond})
	t.Cleanup(stop)

	require.Eventually(t, func() bool { return msg.calls() >= 3 }, time.Second, time.Millisecond,
		"a failing InProgress must not end the heartbeat")
}

// An unbounded heartbeat parks a wedged handler's message forever: it holds its
// MaxAckPending slot with no redelivery and no MaxDeliver escape, so the server
// lever CLAUDE.md relies on for a hang never fires. The budget gives that up.
func TestHeartbeat_StopsExtendingOnceBudgetIsSpent(t *testing.T) {
	msg := &countingHeartbeatMsg{}
	stop := Heartbeat(context.Background(), msg, HeartbeatBudget{
		Every: 5 * time.Millisecond,
		Max:   50 * time.Millisecond,
	})
	t.Cleanup(stop)

	require.Eventually(t, func() bool { return msg.calls() >= 2 }, time.Second, time.Millisecond,
		"the heartbeat must extend the deadline while the budget lasts")

	// A 50ms budget at 5ms allows ~11 extensions. An unbounded ticker reaches
	// ~100 inside this window, so the ceiling separates bounded from unbounded
	// without asserting an exact count.
	assert.Never(t, func() bool { return msg.calls() > 25 }, 500*time.Millisecond, 10*time.Millisecond,
		"the heartbeat must stop once its budget is spent, letting the message redeliver")
}

// Unbounded extension must not be reachable by omission — the zero value is the
// one a caller gets wrong, so it resolves to the default rather than to "forever".
func TestHeartbeatBudget_ResolvedMaxIsNeverUnbounded(t *testing.T) {
	tests := []struct {
		name string
		max  time.Duration
		want time.Duration
	}{
		{"unset falls back to the default bound", 0, DefaultHeartbeatMax},
		{"negative falls back to the default bound", -time.Second, DefaultHeartbeatMax},
		{"an explicit bound is honored", 90 * time.Second, 90 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HeartbeatBudget{Every: time.Second, Max: tt.max}.resolvedMax())
		})
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimClient_ConnectAfterCloseDoesNotLeak(t *testing.T) {
	fc := newFakeConn(emptySubListPage())
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-lc", jwtModeExpiry, mint)
	dialed := make(chan struct{})
	proceed := make(chan struct{})
	s.dial = func(context.Context) (simConn, error) {
		close(dialed)
		<-proceed
		return fc, nil
	}
	require.NoError(t, s.primeJWT(context.Background()))

	errCh := make(chan error, 1)
	go func() { errCh <- s.connect(context.Background()) }()
	<-dialed
	s.close() // churn picks the client mid-dial
	close(proceed)

	err := <-errCh
	require.Error(t, err, "connect must refuse to install a conn after close")
	assert.Equal(t, int64(1), fc.closes.Load(), "the fresh conn must be closed, not leaked")
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.ConnsActive), 0.001)
}

func TestDisconnectReason_Table(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"auth expired", nats.ErrAuthExpired, "auth_expired"},
		{"authorization", nats.ErrAuthorization, "authorization"},
		{"closed", nats.ErrConnectionClosed, "closed"},
		{"anything else", errors.New("tcp reset"), "other"},
		{"wrapped auth expired", fmt.Errorf("outer: %w", nats.ErrAuthExpired), "auth_expired"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, disconnectReason(tt.err))
		})
	}
}

func TestSimClient_AsyncSubscriptionErrorDemotesReady(t *testing.T) {
	m := newMetrics()
	s := &simClient{account: "user-a", m: m, planVerified: true}
	s.markConnUp()
	s.markReady()

	// nats.go names the offending subscription; without it an operator facing
	// a permanently not-ready client has no way to find the denied subject.
	s.handleAsyncError(context.Background(), &nats.Subscription{Subject: "chat.room.r1.event.member"},
		nats.ErrPermissionViolation)

	assert.InDelta(t, 0, promtestutil.ToFloat64(m.ConnsReady), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Errors.WithLabelValues("async")), 0.001)
	assert.True(t, s.asyncFault, "the fault must outlive the demote itself")
}

func TestSimClient_SlowConsumerIsNotAReadinessFault(t *testing.T) {
	// A slow consumer is a throughput symptom that resolves itself; treating
	// it as a permission-class fault would park the client until its next
	// reconnect for what is a transient backlog.
	m := newMetrics()
	s := &simClient{account: "user-a", m: m, planVerified: true}
	s.markConnUp()
	s.markReady()

	s.handleAsyncError(context.Background(), nil, nats.ErrSlowConsumer)

	assert.InDelta(t, 1, promtestutil.ToFloat64(m.ConnsReady), 0.001, "still ready")
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.SlowConsumer), 0.001)
	assert.False(t, s.asyncFault)
}

// --- connection name: the ops-facing discriminator ---

func TestConnName_MirrorsTheDesktopShapeWithAnUnmistakablePrefix(t *testing.T) {
	got := connName("u000042", "soak20260902a", 3)
	assert.Equal(t, "clientsim-u000042-soak20260902a-s3", got)
	assert.True(t, strings.HasPrefix(got, connNamePrefix),
		"ops filters on the prefix; it is the whole contract")
}

func TestConnName_StaysDistinctFromARealClientName(t *testing.T) {
	// The desktop client is desktop-${account}[-${hostname}]. Sharing the
	// shape keeps existing "split on the first dash" tooling working while
	// the first token still tells the two apart.
	assert.NotEqual(t, "desktop", strings.SplitN(connName("a", "r", 0), "-", 2)[0])
}

// --- reconnect backoff: nats.ws's curve, not nats.go's flat ReconnectWait ---

func TestReconnectBackoff_MatchesTheClientCurve(t *testing.T) {
	tests := []struct {
		name    string
		attempt int
		base    time.Duration
	}{
		{"first attempt", 1, 2 * time.Second},
		{"end of the 2s band", 5, 2 * time.Second},
		{"start of the 5s band", 6, 5 * time.Second},
		{"end of the 5s band", 10, 5 * time.Second},
		{"exponential starts", 11, 10 * time.Second},
		{"exponential doubles", 12, 20 * time.Second},
		{"exponential doubles again", 13, 40 * time.Second},
		{"exponential reaches the cap", 14, 60 * time.Second},
		{"capped inside the band", 17, 60 * time.Second},
		{"long polling", 18, 60 * time.Second},
		{"long polling stays flat", 500, 60 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.base, reconnectBackoffBase(tt.attempt))
			// Jitter adds up to 50%, never subtracts: the floor is the base.
			got := reconnectDelay(tt.attempt)
			assert.GreaterOrEqual(t, got, tt.base)
			assert.Less(t, got, tt.base+tt.base/2+time.Millisecond)
		})
	}
}

func TestReconnectBackoff_NonPositiveAttemptIsTheFirstBand(t *testing.T) {
	assert.Equal(t, 2*time.Second, reconnectBackoffBase(0))
	assert.Equal(t, 2*time.Second, reconnectBackoffBase(-1))
}

// --- stability timer: the attempt counter survives a short-lived reconnect ---

func TestReconnectAttempts_AccumulateUntilFiveMinutesOfStability(t *testing.T) {
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)

	assert.Equal(t, 1, s.nextReconnectAttempt())
	assert.Equal(t, 2, s.nextReconnectAttempt())

	// A reconnect that does not survive the stability window must NOT reset
	// the counter — that is what makes a flapping link climb the curve.
	s.armStability()
	s.cancelStability()
	assert.Equal(t, 3, s.nextReconnectAttempt())

	// Surviving the window resets it.
	s.stabilityWindow = time.Millisecond
	s.armStability()
	require.Eventually(t, func() bool { return s.reconnectAttemptsForTest() == 0 },
		2*time.Second, 5*time.Millisecond, "stability window must reset the counter")
	assert.Equal(t, 1, s.nextReconnectAttempt())
}

func TestReconnectAttempts_LateStabilityTimerCannotResetANewerEpisode(t *testing.T) {
	fc := newFakeConn(emptySubListPage())
	s, _ := newLifecycleClient(t, fc, jwtModeExpiry)
	s.stabilityWindow = 20 * time.Millisecond

	s.armStability()
	s.cancelStability() // episode 1 is over; its timer must be inert
	s.nextReconnectAttempt()
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, 1, s.reconnectAttemptsForTest(),
		"a disarmed timer must not reset a counter that has moved on")
}

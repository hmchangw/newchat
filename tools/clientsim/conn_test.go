package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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

// ForceReconnect passes a nil error to DisconnectedErrCB (nats.go v1.50.0
// doReconnect(nil, true)), so a deliberate JWT-refresh reconnect was recorded
// under the same "closed" label as a real teardown. An operator reading the
// disconnect series during a soak could not tell the fleet's own refresh
// churn from connections actually going away.
func TestDisconnectReason_SeparatesADeliberateRefreshFromAClose(t *testing.T) {
	m := newMetrics()
	s := newTestSimClient(t, "user-1", jwtModeProactive, &countingMinter{})
	s.m = m

	s.recordDisconnect(nil) // an ordinary close
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Disconnects.WithLabelValues("closed")), 0.001)

	s.markIntentionalReconnect()
	s.recordDisconnect(nil) // the forced reconnect that follows a refresh
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Disconnects.WithLabelValues("jwt_refresh")), 0.001)
	assert.InDelta(t, 1, promtestutil.ToFloat64(m.Disconnects.WithLabelValues("closed")), 0.001,
		"the latch is consumed once, not sticky")

	s.recordDisconnect(nil)
	assert.InDelta(t, 2, promtestutil.ToFloat64(m.Disconnects.WithLabelValues("closed")), 0.001)
}

// A real error still classifies as itself even if a refresh latch is pending:
// the latch describes an intentional close, not an error that happened to
// arrive during one.
func TestDisconnectReason_ARealErrorOutranksThePendingLatch(t *testing.T) {
	s := newTestSimClient(t, "user-1", jwtModeProactive, &countingMinter{})
	s.markIntentionalReconnect()
	s.recordDisconnect(io.EOF)
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Disconnects.WithLabelValues("eof")), 0.001)

	// ...and the latch is spent, not left armed: a real disconnect means the
	// forced reconnect this latch described never happened, so carrying it
	// forward would mislabel the NEXT ordinary close as a JWT refresh.
	s.recordDisconnect(nil)
	assert.InDelta(t, 1, promtestutil.ToFloat64(s.m.Disconnects.WithLabelValues("closed")), 0.001)
	assert.InDelta(t, 0, promtestutil.ToFloat64(s.m.Disconnects.WithLabelValues("jwt_refresh")), 0.001)
}

// In expiry mode userCB only re-mints once the LOCAL clock says the JWT is
// dead. A broker that rejects a still-locally-valid token would be handed the
// same dead credential on every reconnect, forever. The disconnect path
// latches the broker's verdict so the next callback mints once.
func TestUserCB_ABrokerAuthExpiryForcesOneRefresh(t *testing.T) {
	mint := &countingMinter{jwt: func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }}
	s := newTestSimClient(t, "user-1", jwtModeExpiry, mint)
	require.NoError(t, s.primeJWT(context.Background()))
	require.Equal(t, int64(1), mint.calls.Load())

	// Locally valid: no mint.
	_, err := s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(1), mint.calls.Load())

	s.recordDisconnect(nats.ErrAuthExpired)
	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(2), mint.calls.Load(), "the broker's verdict must force one mint")

	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(2), mint.calls.Load(), "and exactly one — the latch is consumed")
}

// The broker's expiry verdict is consumed by CompareAndSwap before the mint is
// attempted, so a mint that FAILS threw the verdict away: the next reconnect
// presented the same dead credential with nothing left to force a re-mint, and
// the client could never recover.
func TestUserCB_AFailedForcedRefreshKeepsTheBrokerVerdict(t *testing.T) {
	mint := &countingMinter{err: assert.AnError}
	s := newTestSimClient(t, "user-1", jwtModeExpiry, mint)
	// Prime by hand: the minter is set to fail from the start.
	require.NoError(t, s.cache.set(mintTestJWT(t, time.Now().Add(2*time.Hour))))

	s.recordDisconnect(nats.ErrAuthExpired)
	_, err := s.userCB()
	require.Error(t, err, "the forced mint fails")
	assert.Equal(t, int64(1), mint.calls.Load())

	// The verdict must survive, or the next reconnect hands back the same
	// credential the broker already rejected.
	_, err = s.userCB()
	require.Error(t, err)
	assert.Equal(t, int64(2), mint.calls.Load(), "the broker verdict must outlive a failed mint")

	// Once the mint succeeds the latch is finally spent.
	mint.err = nil
	mint.jwt = func() string { return mintTestJWT(t, time.Now().Add(2*time.Hour)) }
	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(3), mint.calls.Load())
	_, err = s.userCB()
	require.NoError(t, err)
	assert.Equal(t, int64(3), mint.calls.Load(), "and not re-armed afterwards")
}

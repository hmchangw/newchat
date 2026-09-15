package natsutil_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// reservePort returns a URL nothing listens on yet, and a starter that brings a
// server up on it later — the shape of a pod booting while its home cluster is
// down and the cluster returning afterwards.
func reservePort(t *testing.T) (url string, start func() *restartableNATS) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return fmt.Sprintf("nats://127.0.0.1:%d", port), func() *restartableNATS {
		n := &restartableNATS{port: port}
		n.start(t)
		t.Cleanup(func() { n.srv.Shutdown() })
		return n
	}
}

func buddyEnabled() *natsutil.BuddyDialer {
	return dialer(natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"})
}

// A service with no buddy has nothing to do without its home cluster, so the
// fail-fast dial stays: crash-looping is the right signal in a single-site
// deployment.
func TestBuddyDialer_ConnectHome_NoBuddyFailsFast(t *testing.T) {
	url, _ := reservePort(t)

	_, err := dialer(natsutil.BuddyConfig{}).ConnectHome(context.Background(), url, nil)

	require.Error(t, err)
}

// With a buddy configured, a pod that boots while its home cluster is down must
// still come up: it is precisely then that the buddy lane needs it. The home
// connection is returned in the reconnecting state and dials in the background.
func TestBuddyDialer_ConnectHome_WithBuddyDialsInTheBackground(t *testing.T) {
	url, start := reservePort(t)

	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })
	assert.Equal(t, nats.RECONNECTING, conn.NatsConn().Status(), "not connected yet, not failed either")

	start()
	require.Eventually(t, func() bool { return conn.NatsConn().Status() == nats.CONNECTED },
		20*time.Second, 50*time.Millisecond, "the background dial must land once the cluster returns")
}

// The ordinary case must not change: a reachable home connects synchronously.
func TestBuddyDialer_ConnectHome_WithBuddyAndReachableHomeConnectsNow(t *testing.T) {
	url := startTestNATSURL(t)

	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	assert.Equal(t, nats.CONNECTED, conn.NatsConn().Status())
}

// A missing creds file is a configuration error, not an outage; retrying it in
// the background would hide it forever.
func TestBuddyDialer_ConnectHome_BadCredsFileFailsFastEvenWithBuddy(t *testing.T) {
	d := buddyEnabled()
	d.CredsFile = "/nonexistent/creds"

	_, err := d.ConnectHome(context.Background(), startTestNATSURL(t), nil)

	require.Error(t, err)
}

// A bind on an already-connected home runs right away, and its error is the
// caller's to treat as fatal: home is up, so a failed bind is a real fault.
func TestBindWhenConnected_ConnectedRunsNowAndReturnsTheError(t *testing.T) {
	conn := natsutil.ConnectBuddy(context.Background(), startTestNATSURL(t), "",
		buddyEnabled().TracerProvider, buddyEnabled().Propagator, false)
	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })

	ran := 0
	d, err := natsutil.BindWhenConnected(context.Background(), conn, "lane", func(context.Context) (func(), error) {
		ran++
		return func() {}, nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, ran)
	assert.True(t, d.Ready())

	_, err = natsutil.BindWhenConnected(context.Background(), conn, "lane", func(context.Context) (func(), error) {
		return nil, errors.New("consumer create failed")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "consumer create failed")
}

// The case this exists for: home is down at boot. The bind is deferred, the
// service reports not-ready on that lane, and the moment the cluster returns the
// bind runs and the lane comes up — no restart, no operator.
func TestBindWhenConnected_DeferredUntilTheClusterReturns(t *testing.T) {
	url, start := reservePort(t)
	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	var ran, stopped atomic.Int32
	d, err := natsutil.BindWhenConnected(context.Background(), conn, "lane", func(context.Context) (func(), error) {
		ran.Add(1)
		return func() { stopped.Add(1) }, nil
	})
	require.NoError(t, err)
	assert.False(t, d.Ready())
	assert.Equal(t, int32(0), ran.Load(), "nothing to bind on before the cluster is reachable")

	start()
	require.Eventually(t, d.Ready, 20*time.Second, 50*time.Millisecond, "the lane must bind once home returns")
	assert.Equal(t, int32(1), ran.Load())

	d.Stop()
	assert.Equal(t, int32(1), stopped.Load(), "Stop must stop what the deferred bind started")
	d.Stop()
	assert.Equal(t, int32(1), stopped.Load(), "Stop is idempotent")
}

// A bind that fails after the cluster returns — streams not yet restored, say —
// is retried rather than abandoned, because a lane that never comes up is the
// outage all over again.
func TestBindWhenConnected_RetriesAFailedDeferredBind(t *testing.T) {
	url, start := reservePort(t)
	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	var attempts atomic.Int32
	d, err := natsutil.BindWhenConnected(context.Background(), conn, "lane", func(context.Context) (func(), error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("stream not restored yet")
		}
		return func() {}, nil
	})
	require.NoError(t, err)

	start()
	require.Eventually(t, d.Ready, 20*time.Second, 50*time.Millisecond)
	assert.Equal(t, int32(2), attempts.Load())
}

// Shutdown can begin before a deferred bind lands. A bind that completes after
// Stop must be stopped on the spot, or a lane outlives the process's teardown.
func TestBindWhenConnected_StopBeforeReadyStopsALateBind(t *testing.T) {
	url, start := reservePort(t)
	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	var stopped atomic.Int32
	d, err := natsutil.BindWhenConnected(context.Background(), conn, "lane", func(context.Context) (func(), error) {
		return func() { stopped.Add(1) }, nil
	})
	require.NoError(t, err)

	d.Stop()
	start()
	require.Eventually(t, func() bool { return stopped.Load() == 1 },
		20*time.Second, 50*time.Millisecond, "a bind landing after Stop must be stopped immediately")
	assert.False(t, d.Ready(), "a stopped lane is not a serving lane")
}

// Cancelling the context releases the watcher without binding anything.
func TestBindWhenConnected_ContextCancelReleasesTheWatcher(t *testing.T) {
	url, _ := reservePort(t)
	conn, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.NatsConn().Close() })

	ctx, cancel := context.WithCancel(context.Background())
	d, err := natsutil.BindWhenConnected(ctx, conn, "lane", func(context.Context) (func(), error) {
		t.Fatal("must not bind after cancel")
		return nil, nil
	})
	require.NoError(t, err)
	cancel()
	require.Eventually(t, d.Done, 5*time.Second, 20*time.Millisecond)
	assert.False(t, d.Ready())
}

// A page budget or payload cap derived before the home connection is up must
// not collapse to zero: zero disables trimming, and a reply that then exceeds
// the broker's limit is refused outright. The server default is the safe floor.
func TestMaxPayload(t *testing.T) {
	url, _ := reservePort(t)
	lazy, err := buddyEnabled().ConnectHome(context.Background(), url, nil)
	require.NoError(t, err)
	t.Cleanup(func() { lazy.NatsConn().Close() })

	live := natsutil.ConnectBuddy(context.Background(), startTestNATSURL(t), "",
		buddyEnabled().TracerProvider, buddyEnabled().Propagator, false)
	require.NotNil(t, live)
	t.Cleanup(func() { live.NatsConn().Close() })

	assert.Equal(t, int64(natsutil.DefaultMaxPayload), natsutil.MaxPayload(lazy), "unknown before connect: assume the server default")
	assert.Equal(t, int64(natsutil.DefaultMaxPayload), natsutil.MaxPayload(nil))
	assert.Equal(t, live.NatsConn().MaxPayload(), natsutil.MaxPayload(live), "connected: the broker's advertised value")
	assert.Positive(t, natsutil.MaxPayload(live))
}

// Readiness under a lazy home dial means "at least one lane is serving": the
// NATS check alone reads RECONNECTING as healthy, which is exactly the state a
// pod is in while it has nothing bound at all.
func TestLanesCheck(t *testing.T) {
	yes := func() bool { return true }
	no := func() bool { return false }
	ctx := context.Background()

	assert.Error(t, natsutil.LanesCheck(no).Probe(ctx), "no lane bound")
	assert.Error(t, natsutil.LanesCheck(no, no).Probe(ctx))
	assert.NoError(t, natsutil.LanesCheck(yes, no).Probe(ctx), "home serving")
	assert.NoError(t, natsutil.LanesCheck(no, yes).Probe(ctx), "buddy serving while home is down")
	assert.Equal(t, "lanes", natsutil.LanesCheck(yes).Name)
}

// A nil Lane is a lane that never came up; Bound lets a readiness check ask
// without a nil guard of its own.
func TestLane_Bound(t *testing.T) {
	var none *natsutil.Lane
	assert.False(t, none.Bound())
	assert.True(t, natsutil.NewLane(nil, nil).Bound())
}

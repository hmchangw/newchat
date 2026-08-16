package natsutil_test

import (
	"context"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// startTestServer starts an embedded NATS server and returns its client URL.
// Separate from startTestNATS (which hands back a raw *nats.Conn) because these
// tests need to build a real *o11ynats.Conn through natsutil.Connect, so the
// production option wiring — including DrainTimeout — is what gets exercised.
func startTestServer(t *testing.T) string {
	t.Helper()
	_, url := startTestServerWithHandle(t)
	return url
}

// startTestServerWithHandle is startTestServer but also returns the
// *natsserver.Server handle, for tests that need to shut the server down
// mid-test (e.g. to drive a connection into RECONNECTING) rather than only
// at t.Cleanup.
func startTestServerWithHandle(t *testing.T) (*natsserver.Server, string) {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	url := ns.ClientURL()
	return ns, url
}

func testConn(t *testing.T, url string) *o11ynats.Conn {
	t.Helper()
	conn, err := natsutil.Connect(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	require.NoError(t, err)
	return conn
}

func TestDrain_ClosesConnection(t *testing.T) {
	conn := testConn(t, startTestServer(t))

	_, err := conn.NatsConn().Subscribe("drain.test", func(*nats.Msg) {})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, natsutil.Drain(ctx, conn))
	require.True(t, conn.NatsConn().IsClosed(),
		"Drain must not return until the connection has reached CLOSED")
}

// The regression this whole change exists for: Drain must not return while a
// handler is still running. Before the fix nats.Conn.Drain returned instantly
// and this assertion fails with handlerDone still false.
func TestDrain_WaitsForInFlightHandler(t *testing.T) {
	url := startTestServer(t)
	conn := testConn(t, url)

	entered := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	_, err := conn.NatsConn().Subscribe("drain.slow", func(*nats.Msg) {
		close(entered)
		<-release
		close(handlerDone)
	})
	require.NoError(t, err)
	require.NoError(t, conn.NatsConn().Flush())

	pub := testConn(t, url)
	require.NoError(t, pub.NatsConn().Publish("drain.slow", []byte("x")))
	require.NoError(t, pub.NatsConn().Flush())

	// pub.Flush only guarantees the server processed the PUB, not that this
	// subscriber's readLoop has dispatched it yet. Wait for the handler to
	// actually start before racing it against Drain, so a slow dispatch can't
	// let Drain's sub.Drain() see an empty queue and skip the handler.
	<-entered

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		drained <- natsutil.Drain(ctx, conn)
	}()

	select {
	case <-drained:
		t.Fatal("Drain returned while the handler was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	<-handlerDone
	require.NoError(t, <-drained)
}

func TestDrain_CtxExpiredReturnsError(t *testing.T) {
	url := startTestServer(t)
	conn := testConn(t, url)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	_, err := conn.NatsConn().Subscribe("drain.wedged", func(*nats.Msg) { <-release })
	require.NoError(t, err)
	require.NoError(t, conn.NatsConn().Flush())

	pub := testConn(t, url)
	require.NoError(t, pub.NatsConn().Publish("drain.wedged", []byte("x")))
	require.NoError(t, pub.NatsConn().Flush())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = natsutil.Drain(ctx, conn)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// The production path this covers: Connect sets MaxReconnects(-1), so a pod
// that receives SIGTERM while NATS is unreachable sits in RECONNECTING
// indefinitely and Drain is called against that state, not a healthy one.
// nc.Drain() hard-closes the connection in that case (discarding the write
// buffer) and returns ErrConnectionReconnecting rather than nil — Drain must
// still report a clean nil, since the connection genuinely did reach CLOSED,
// not surface the library's internal sentinel as a failure.
func TestDrain_ReconnectingHardClosesAndReturnsNil(t *testing.T) {
	ns, url := startTestServerWithHandle(t)
	conn := testConn(t, url)

	_, err := conn.NatsConn().Subscribe("drain.reconnecting", func(*nats.Msg) {})
	require.NoError(t, err)

	ns.Shutdown()
	require.Eventually(t, conn.NatsConn().IsReconnecting, 5*time.Second, 10*time.Millisecond,
		"connection must enter RECONNECTING once the server goes away")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, natsutil.Drain(ctx, conn))
	require.True(t, conn.NatsConn().IsClosed())
}

func TestDrain_AlreadyClosedIsNotAnError(t *testing.T) {
	conn := testConn(t, startTestServer(t))
	conn.NatsConn().Close()

	require.NoError(t, natsutil.Drain(context.Background(), conn))
}

func TestDrain_Idempotent(t *testing.T) {
	conn := testConn(t, startTestServer(t))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, natsutil.Drain(ctx, conn))
	require.NoError(t, natsutil.Drain(ctx, conn))
}

func TestDrain_NilConnIsNotAnError(t *testing.T) {
	require.NoError(t, natsutil.Drain(context.Background(), nil))
}

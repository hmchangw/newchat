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
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
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

	release := make(chan struct{})
	handlerDone := make(chan struct{})
	_, err := conn.NatsConn().Subscribe("drain.slow", func(*nats.Msg) {
		<-release
		close(handlerDone)
	})
	require.NoError(t, err)
	require.NoError(t, conn.NatsConn().Flush())

	pub := testConn(t, url)
	require.NoError(t, pub.NatsConn().Publish("drain.slow", []byte("x")))
	require.NoError(t, pub.NatsConn().Flush())

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

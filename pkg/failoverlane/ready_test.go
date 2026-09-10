package failoverlane

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	o11ynats "github.com/flywindy/o11y/nats"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/natsutil"
	"github.com/hmchangw/chat/pkg/subject"
)

// embeddedNATS starts an in-process server so these tests need no Docker.
func embeddedNATS(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

// unlistenedURL is a URL nothing answers on: the home cluster during an outage.
func unlistenedURL(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return fmt.Sprintf("nats://127.0.0.1:%d", port)
}

func buildNothing(_ context.Context, _ *o11ynats.Conn, _ o11ynats.JetStream, _ subject.Lane) (*natsrouter.Router, error) {
	return nil, nil
}

// Readiness for a request/reply service with a lazily dialed home is "some
// connection is serving": the home one, or the buddy while home is down. The
// plain NATS check cannot say this — it reads a dialing home as healthy.
func TestRouters_Check(t *testing.T) {
	ctx := context.Background()
	tp, prop := noop.NewTracerProvider(), propagation.TraceContext{}

	t.Run("home down, no buddy: not ready", func(t *testing.T) {
		d := &natsutil.BuddyDialer{Config: natsutil.BuddyConfig{SiteID: "site-b", NatsURL: "nats://127.0.0.1:1"},
			TracerProvider: tp, Propagator: prop}
		home, err := d.ConnectHome(ctx, unlistenedURL(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { home.NatsConn().Close() })

		routers, err := BindRouters(ctx, home, nil, d, buildNothing)
		require.NoError(t, err)

		assert.Error(t, routers.Check().Probe(ctx))
		assert.Equal(t, "lanes", routers.Check().Name)
	})

	t.Run("home down, buddy up: ready on the buddy", func(t *testing.T) {
		d := &natsutil.BuddyDialer{Config: natsutil.BuddyConfig{SiteID: "site-b", NatsURL: embeddedNATS(t)},
			TracerProvider: tp, Propagator: prop}
		home, err := d.ConnectHome(ctx, unlistenedURL(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { home.NatsConn().Close() })

		routers, err := BindRouters(ctx, home, nil, d, buildNothing)
		require.NoError(t, err)
		t.Cleanup(func() { _ = natsutil.DrainBuddy(routers.buddyConn)(ctx) })

		assert.NoError(t, routers.Check().Probe(ctx))
	})

	t.Run("home up, no buddy: ready", func(t *testing.T) {
		d := &natsutil.BuddyDialer{TracerProvider: tp, Propagator: prop}
		home, err := d.ConnectHome(ctx, embeddedNATS(t), nil)
		require.NoError(t, err)
		t.Cleanup(func() { home.NatsConn().Close() })

		routers, err := BindRouters(ctx, home, nil, d, buildNothing)
		require.NoError(t, err)

		assert.NoError(t, routers.Check().Probe(ctx))
	})
}

// reserveJetStreamPort hands out a URL nothing listens on and a starter that
// brings a JetStream server up on it later.
func reserveJetStreamPort(t *testing.T) (url string, start func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return fmt.Sprintf("nats://127.0.0.1:%d", port), func() { embeddedJetStream(t, port) }
}

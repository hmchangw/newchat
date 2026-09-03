package natsutil_test

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/hmchangw/chat/pkg/natsutil"
)

// startTestNATSURL runs an embedded server and returns its client URL. The
// sibling helper in reply_test.go hands back a *nats.Conn; ConnectBuddy needs
// the URL so it can dial for itself.
func startTestNATSURL(t *testing.T) string {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second))
	t.Cleanup(ns.Shutdown)
	return ns.ClientURL()
}

func TestConnectBuddy_UnreachableReturnsNil(t *testing.T) {
	conn := natsutil.ConnectBuddy(context.Background(), "nats://127.0.0.1:1", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unreachable buddy must degrade to nil, never block startup")
}

func TestConnectBuddy_EmptyURLReturnsNil(t *testing.T) {
	conn := natsutil.ConnectBuddy(context.Background(), "", "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)
	assert.Nil(t, conn, "an unconfigured buddy is not an error")
}

func TestConnectBuddy_ReachableReturnsConn(t *testing.T) {
	url := startTestNATSURL(t)

	conn := natsutil.ConnectBuddy(context.Background(), url, "",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)

	require.NotNil(t, conn)
	t.Cleanup(func() { conn.NatsConn().Close() })
	assert.True(t, conn.NatsConn().IsConnected())
}

// A missing creds file is a config error, not a reachable-buddy question — it
// must still degrade rather than fail startup, since the buddy lane is optional.
func TestConnectBuddy_BadCredsFileReturnsNil(t *testing.T) {
	url := startTestNATSURL(t)

	conn := natsutil.ConnectBuddy(context.Background(), url, "/nonexistent/creds.json",
		noop.NewTracerProvider(), propagation.TraceContext{}, false)

	assert.Nil(t, conn)
}

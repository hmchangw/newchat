package main

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsrouter"
	"github.com/hmchangw/chat/pkg/testutil"
)

// TestRegisteredRoutes runs user-presence-service's real registration table and pins every
// route's rpc.method to the subject pattern that claimed it. See
// testutil.AssertRoutesGolden for what the golden file guards and how to
// regenerate it.
//
// The golden holds three entries, not seven: Hello/Ping/Activity/Bye are
// RegisterVoid routes, which name no rpc.method and so never enter Routes().
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(startRoutesNATS(t), "user-presence-service")
	registerRoutes(r, &Handler{}, "site-a")

	testutil.AssertRoutesGolden(t, r.Routes())
}

// startRoutesNATS runs an embedded NATS server in-process, so registering the
// real route table needs no Docker and this stays a unit test.
func startRoutesNATS(t *testing.T) *o11ynats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{Port: -1})
	require.NoError(t, err)
	ns.Start()
	require.True(t, ns.ReadyForConnections(5*time.Second), "nats server did not become ready")
	t.Cleanup(ns.Shutdown)

	nc, err := o11ynats.Connect(context.Background(), ns.ClientURL(), noop.NewTracerProvider(), propagation.TraceContext{})
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	return nc
}

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace/noop"

	o11ynats "github.com/flywindy/o11y/nats"

	"github.com/hmchangw/chat/pkg/natsmetrics"
	"github.com/hmchangw/chat/pkg/natsrouter"
)

// TestRegisteredRoutes pins every route's rpc.method to the subject pattern
// that claimed it. The router's own duplicate check catches a collision, not a
// valid-but-wrong constant on a route, and registration no longer panics on a
// bad method — so this golden file is the gate: a copy-pasted method shows up
// in review as a one-line diff.
//
// Regenerate after an intentional route change by deleting
// testdata/routes.golden and re-running `make test SERVICE=media-service`. The test
// rewrites the file and fails once, so the new table lands in the diff instead
// of being absorbed silently.
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(startRoutesNATS(t), "media-service")
	registerEmojiNATS(r, &handler{}, "site-a")

	requireRoutesMatchGolden(t, r.Routes())
}

// requireRoutesMatchGolden renders the router's method-to-pattern table as one
// sorted "method pattern" line per route and compares it to
// testdata/routes.golden.
func requireRoutesMatchGolden(t *testing.T, routes map[natsmetrics.RPCMethod]string) {
	t.Helper()

	lines := make([]string, 0, len(routes))
	for method, pattern := range routes {
		lines = append(lines, string(method)+" "+pattern)
	}
	slices.Sort(lines)
	got := strings.Join(lines, "\n") + "\n"

	const path = "testdata/routes.golden"
	want, err := os.ReadFile(filepath.Clean(path))
	if os.IsNotExist(err) {
		require.NoError(t, os.MkdirAll("testdata", 0o750))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
		t.Fatalf("%s did not exist and was written; check every line against the naming table before committing", path)
	}
	require.NoError(t, err)
	require.Equal(t, string(want), got,
		"route-to-method table changed; if the change is intended, delete %s and re-run", path)
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

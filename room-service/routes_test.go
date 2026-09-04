package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
// testdata/routes.golden and re-running `make test SERVICE=room-service`. The
// test rewrites the file and fails once, so the new table lands in the diff
// instead of being absorbed silently.
func TestRegisteredRoutes(t *testing.T) {
	r := natsrouter.New(startOtelNATS(t), "room-service")
	(&Handler{siteID: "site-a"}).Register(r)

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

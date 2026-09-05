// This file is deliberately untagged. Every //go:build integration file in this
// package pulls in testcontainers, gocql and minio; the routes golden helper is
// used by ordinary unit tests, so it must stay outside that build.
package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hmchangw/chat/pkg/natsmetrics"
)

// routesGoldenPath is relative to the package under test, which is the working
// directory `go test` gives it. The Go toolchain ignores testdata/ when
// building, so a golden file never reaches a binary.
const routesGoldenPath = "testdata/routes.golden"

// AssertRoutesGolden compares a router's registered method→pattern table to
// <pkg>/testdata/routes.golden, one sorted "method pattern" line per route.
// It is what pins a route to its correct method: natsrouter's duplicate check
// catches collisions, not a valid-but-wrong constant, and registration now
// degrades a bad method to _OTHER rather than panicking — so a copy-paste
// mistake surfaces here, as a one-line diff in review.
//
// A route recorded as natsmetrics.MethodOther is rejected before the golden
// file is read or generated. _OTHER is the record-time fallback for a value
// that should never occur, so a route carrying it means registration degraded
// one — and letting that reach the comparison would either bake "_OTHER" into
// a regenerated golden or demand it be hand-written into an existing one,
// turning the fallback into an accepted spelling.
//
// Never hand-edit a golden file: an edited one asserts whatever the code
// already does, which is the gate quietly switching itself off. Regenerate it
// instead, by deleting it and re-running the service's tests:
//
//	rm <package dir>/testdata/routes.golden && make test SERVICE=<service>
//
// That writes the file from the live registration table and fails once, so the
// new table lands in the diff for review rather than being absorbed silently.
// The same happens the first time a service adds this test.
func AssertRoutesGolden(t *testing.T, routes map[natsmetrics.RPCMethod]string) {
	t.Helper()

	require.NoError(t, rejectFallbackMethod(routes))

	lines := make([]string, 0, len(routes))
	for method, pattern := range routes {
		lines = append(lines, string(method)+" "+pattern)
	}
	slices.Sort(lines)
	got := strings.Join(lines, "\n") + "\n"

	want, err := os.ReadFile(filepath.Clean(routesGoldenPath))
	if os.IsNotExist(err) {
		require.NoError(t, os.MkdirAll(filepath.Dir(routesGoldenPath), 0o750))
		require.NoError(t, os.WriteFile(routesGoldenPath, []byte(got), 0o600))
		t.Fatalf("%s did not exist and was written from the live route table; check every line against the naming table before committing", routesGoldenPath)
	}
	require.NoError(t, err)
	require.Equal(t, string(want), got,
		"route-to-method table changed; if the change is intended, regenerate: rm <package dir>/%s && make test SERVICE=<service>", routesGoldenPath)
}

// rejectFallbackMethod reports the pattern registered under MethodOther, if
// any. Kept separate from AssertRoutesGolden so it can be tested without a
// fake *testing.T.
func rejectFallbackMethod(routes map[natsmetrics.RPCMethod]string) error {
	pattern, degraded := routes[natsmetrics.MethodOther]
	if !degraded {
		return nil
	}
	return fmt.Errorf(
		"route %q registered as %s: registration degraded a method outside the vocabulary. "+
			"Add the method to pkg/natsmetrics/rpcmethod.go and pass its constant, "+
			"rather than regenerating %s with the fallback in it",
		pattern, natsmetrics.MethodOther, routesGoldenPath)
}

// This file is deliberately untagged. Every //go:build integration file in this
// package pulls in testcontainers, gocql and minio; the routes golden helper is
// used by ordinary unit tests, so it must stay outside that build.
package testutil

import (
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
